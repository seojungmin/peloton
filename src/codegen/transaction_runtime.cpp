//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.cpp
//
// Identification: src/codegen/transaction_runtime.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_runtime.h"

namespace peloton {
namespace codegen {

// Perform a read operation for all tuples in the tile group in the given range
// TODO: Right now, we split this check into two loops: a visibility check and
//       the actual reading. Can this be merged?
uint32_t TransactionRuntime::PerformVectorizedRead(
    concurrency::Transaction &txn, storage::TileGroup &tile_group,
    uint32_t tid_start, uint32_t tid_end, uint32_t *selection_vector) {
  // Get the transaction manager
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  // Get the tile group header
  auto tile_group_header = tile_group.GetHeader();

  // Check visibility of tuples in the range [tid_start, tid_end), storing all
  // visible tuple IDs in the provided selection vector
  uint32_t out_idx = 0;
  for (uint32_t i = tid_start; i < tid_end; i++) {
    // Perform the visibility check
    auto visibility = txn_manager.IsVisible(&txn, tile_group_header, i);

    // Update the output position
    selection_vector[out_idx] = i;
    out_idx += (visibility == VisibilityType::OK);
  }

  uint32_t tile_group_idx = tile_group.GetTileGroupId();

  // Perform a read operation for every visible tuple we found
  uint32_t end_idx = out_idx;
  out_idx = 0;
  for (uint32_t idx = 0; idx < end_idx; idx++) {
    // Construct the item location
    ItemPointer location{tile_group_idx, selection_vector[idx]};

    // Perform the read
    bool can_read = txn_manager.PerformRead(&txn, location);

    // Update the selection vector and output position
    selection_vector[out_idx] = selection_vector[idx];
    out_idx += static_cast<uint32_t>(can_read);
  }

  return out_idx;
}

/**
* @brief Delete executor.
*
* This function will be called from the JITed code to perform delete on the
* specified tuple.
* This logic is extracted from executor::delete_executor, and refactorized.
*
* @param tile_group_id the offset of the tile in the table where the tuple
*        resides
* @param tuple_id the tuple id of the tuple in current tile
* @param txn the transaction executing this delete operation
* @param table the table containing the tuple to be deleted
*
* @return true on success, false otherwise.
*/
bool TransactionRuntime::PerformDelete(uint32_t tuple_id,
                                       concurrency::Transaction *txn,
                                       storage::DataTable *table,
                                       storage::TileGroup *tile_group) {
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  auto tile_group_id_retrieved = tile_group->GetTileGroupId();
  ItemPointer old_location(tile_group_id_retrieved, tuple_id);

  auto *tile_group_header = tile_group->GetHeader();

  bool is_written = txn_manager.IsWritten(txn, tile_group_header, tuple_id);
  if (is_written) {
    LOG_TRACE("I am the owner of the tuple");
    txn_manager.PerformDelete(txn, old_location);
    return true;
  }

 bool is_owner = txn_manager.IsOwner(txn, tile_group_header, tuple_id);
 bool is_ownable = is_owner ||
     txn_manager.IsOwnable(txn, tile_group_header, tuple_id);
 if (!is_ownable) {
    // transaction should be aborted as we cannot update the latest version.
    LOG_TRACE("Fail to delete tuple.");
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }
  LOG_TRACE("I am NOT the owner, but it is visible");

  bool acquired_ownership = is_owner ||
      txn_manager.AcquireOwnership(txn, tile_group_header, tuple_id);
  if (!acquired_ownership) {
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }

  LOG_TRACE("Ownership is acquired");
  // if it is the latest version and not locked by other threads, then
  // insert an empty version.
  ItemPointer new_location = table->InsertEmptyVersion();

  // PerformDelete() will not be executed if the insertion failed.
  if (new_location.IsNull()) {
    LOG_TRACE("Fail to insert a new tuple version and so fail to delete");
    // this means we acquired ownership from AcquireOwnership
    if (!is_owner) {
      // A write lock is acquired when inserted, but it is not in the write set,
      // because we haven't yet put it into the write set.
      // The acquired lock is not released when the txn gets aborted, and
      // YieldOwnership() will help us release the acquired write lock.
      txn_manager.YieldOwnership(txn, tile_group_header, tuple_id);
    }
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformDelete(txn, old_location, new_location);
  return true;
}

void DoProjection(AbstractTuple *dest, const AbstractTuple *tuple,
                  type::Value *values, uint32_t *col_ids, uint32_t target_size,
                  DirectMapList direct_map_list) {
  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_map_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value);
  }
}

void DoProjection(storage::Tuple *dest, const AbstractTuple *tuple,
                  type::Value *values, uint32_t *col_ids, uint32_t target_size,
                  DirectMapList direct_map_list,
                  executor::ExecutorContext *context = nullptr) {
  type::AbstractPool *pool = nullptr;
  if (context != nullptr) pool = context->GetPool();

  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_map_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;
    type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value, pool);
  }
}

bool PerformUpdatePrimaryKey(concurrency::Transaction *txn, bool is_owner,
                             storage::TileGroupHeader *tile_group_header,
                             storage::DataTable &table,
                             oid_t tuple_offset, 
                             ItemPointer &old_location,
                             storage::TileGroup *tile_group,
                             storage::Tuple *tuple) {
                            
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  ItemPointer new_location = table.InsertEmptyVersion();
  if (new_location.IsNull() == true) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    if (is_owner == false) {
      txn_manager.YieldOwnership(txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }

  txn_manager.PerformDelete(txn, old_location, new_location);

  // Insert tuple rather than install version
  //storage::Tuple new_tuple(table.GetSchema(), true);
  //expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
  //                                                         tuple_offset);
  //DoProjection(&new_tuple, &old_tuple, values, col_ids, target_size,
  //             direct_map_list, executor_context);

  ItemPointer *index_entry_ptr = nullptr;
  peloton::ItemPointer location = table.InsertTuple(tuple, txn,
                                                    &index_entry_ptr);
  // Another concurrent transaction may have inserted the same tuple, just abort
  if (location.block == INVALID_OID) {
    txn_manager.SetTransactionResult(txn, peloton::ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformInsert(txn, location, index_entry_ptr);
  return true;
}

bool TransactionRuntime::PerformUpdate(concurrency::Transaction &txn,
                                       storage::DataTable &table,
                                       storage::TileGroup *tile_group,
                                       uint32_t tuple_offset,
                                       storage::AbstractTuple *tuple,
                                       bool is_primary_key) {

  uint32_t tile_group_id = tile_group->GetTileGroupId();
  storage::TileGroupHeader *tile_group_header = tile_group->GetHeader();

  ItemPointer old_location(tile_group_id, tuple_offset);
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();

  bool is_owner = txn_manager.IsOwner(&txn, tile_group_header, tuple_offset);
  bool is_written = txn_manager.IsWritten(&txn, tile_group_header,
                                          tuple_offset);
  PL_ASSERT((is_owner == false && is_written == true) == false);

  // We have acquired the ownership, and so ...
  if (is_primary_key == true) {

    // Delete the old one logically
    ItemPointer new_location = table.InsertEmptyVersion();
    if (new_location.IsNull() == true) {
      if (is_owner == false) {
        txn_manager.YieldOwnership(txn, tile_group_header, tuple_offset);
      }
      LOG_TRACE("Fail to insert new tuple. Set txn failure.");
      txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
      return false;
    }
    txn_manager.PerformDelete(txn, old_location, new_location);

    // Insert the new one
    ItemPointer *index_ptr = nullptr;
    peloton::ItemPointer location = table.InsertTuple(tuple, txn, &index_ptr);
    if (location.block == INVALID_OID) {
      LOG_TRACE("Fail to insert new tuple. Set txn failure.");
      txn_manager.SetTransactionResult(txn, peloton::ResultType::FAILURE);
      return false;
    }
    txn_manager.PerformInsert(txn, location, index_entry_ptr);
    return true;
  }

  // This is my transaction, and I am doing in-place update
  if (is_written == true) {
    txn_manager.PerformUpdate(&txn, old_location);
    return true;
  }

  // It is written somewhere else, but I can write, so install a new version
  ItemPointer new_location = table.AcquireVersion();
  auto &manager = catalog::Manager::GetInstance();
  auto new_tile_group = manager.GetTileGroup(new_location.block);
//expression::ContainerTuple<storage::TileGroup> new_tuple(new_tile_group.get(),
//                                                        new_location.offset);
  ItemPointer *indirection =
      tile_group_header->GetIndirection(old_location.offset);
  ret = table.InstallVersion(tuple, &target_list, &txn, indirection);
  if (ret == false) {
    LOG_TRACE("Fail to insert new tuple while updating. Set txn failure.");
    if (is_owner == false) {
      // I am not the owner, and the ownership acquired from select is yielded
      txn_manager.YieldOwnership(&txn, tile_group_header, tuple_offset);
    }
    txn_manager.SetTransactionResult(&txn, ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformUpdate(&txn, old_location, new_location);

  LOG_TRACE("Perform update old location: %u, %u", old_location.block,
            old_location.offset);
  LOG_TRACE("Perform update new location: %u, %u", new_location.block,
            new_location.offset);

  return true;
}

void TransactionRuntime::IncreaseNumProcessed(
    executor::ExecutorContext *executor_context) {
  executor_context->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
