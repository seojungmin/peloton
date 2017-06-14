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
  auto tile_group_hdr = tile_group.GetHeader();

  // Check visibility of tuples in the range [tid_start, tid_end), storing all
  // visible tuple IDs in the provided selection vector
  uint32_t out_idx = 0;
  for (uint32_t i = tid_start; i < tid_end; i++) {
    // Perform the visibility check
    auto visibility = txn_manager.IsVisible(&txn, tile_group_hdr, i);

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

  auto *tile_group_hdr = tile_group->GetHeader();

  bool is_written = txn_manager.IsWritten(txn, tile_group_hdr, tuple_id);
  if (is_written) {
    LOG_TRACE("I am the owner of the tuple");
    txn_manager.PerformDelete(txn, old_location);
    return true;
  }

 bool is_owner = txn_manager.IsOwner(txn, tile_group_hdr, tuple_id);
 bool is_ownable = is_owner ||
     txn_manager.IsOwnable(txn, tile_group_hdr, tuple_id);
 if (!is_ownable) {
    // transaction should be aborted as we cannot update the latest version.
    LOG_TRACE("Fail to delete tuple.");
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }
  LOG_TRACE("I am NOT the owner, but it is visible");

  bool acquired_ownership = is_owner ||
      txn_manager.AcquireOwnership(txn, tile_group_hdr, tuple_id);
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
      txn_manager.YieldOwnership(txn, tile_group_hdr, tuple_id);
    }
    txn_manager.SetTransactionResult(txn, ResultType::FAILURE);
    return false;
  }
  txn_manager.PerformDelete(txn, old_location, new_location);
  return true;
}


////////////////////////////////////////////////////////////////////////////////
void DoProjection(AbstractTuple *dest, const AbstractTuple *tuple,
                  type::Value *values, uint32_t *col_ids, uint32_t target_size,
                  DirectMapList direct_list, 
                  UNUSED_ATTRIBUTE executor::ExecutorContext *context) {
  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_list) {
    auto dest_col_id = dm.first;
    // whether left tuple or right tuple ?
    auto src_col_id = dm.second.second;

    type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value);
  }
}

void DoProjection(storage::Tuple *dest, const AbstractTuple *tuple,
                  type::Value *values, uint32_t *col_ids, uint32_t target_size,
                  DirectMapList direct_list,
                  executor::ExecutorContext *context = nullptr) {
  // Get varlen pool
  type::AbstractPool *pool = nullptr;
  if (context != nullptr) pool = context->GetPool();

  // Execute target list
  for (uint32_t i = 0; i < target_size; i++) {
    dest->SetValue(col_ids[i], values[i]);
  }

  // Execute direct map
  for (auto dm : direct_list) {
    auto dest_col_id = dm.first;
    auto src_col_id = dm.second.second;

    type::Value value = (tuple->GetValue(src_col_id));
    dest->SetValue(dest_col_id, value, pool);
  }
}

bool PerformUpdatePrimaryKey(concurrency::Transaction *current_txn,
                             bool is_owner,
                             storage::TileGroupHeader *tile_group_hdr,
                             storage::DataTable *target_table,
                             oid_t physical_tuple_id, ItemPointer &old_location,
                             storage::TileGroup *tile_group,
                             type::Value *values, uint32_t *col_ids,
                             uint32_t target_size, DirectMapList direct_list_,
                             executor::ExecutorContext *exec_context) {
  auto &txn_mgr = concurrency::TransactionManagerFactory::GetInstance();

  // Delete tuple/version chain
  ItemPointer new_location = target_table->InsertEmptyVersion();

  // PerformUpdate() will not be executed if the insertion failed.
  // There is a write lock acquired, but since it is not in the write set,
  // because we haven't yet put them into the write set.
  // the acquired lock can't be released when the txn is aborted.
  // the YieldOwnership() function helps us release the acquired write lock.
  if (new_location.IsNull() == true) {
    LOG_TRACE("Fail to insert new tuple. Set txn failure.");
    if (is_owner == false) {
      // If the ownership is acquire inside this update executor, we
      // release it here
      txn_mgr.YieldOwnership(current_txn, tile_group_hdr, physical_tuple_id);
    }
    txn_mgr.SetTransactionResult(current_txn, ResultType::FAILURE);
    return false;
  }
  txn_mgr.PerformDelete(current_txn, old_location, new_location);

  ////////////////////////////////////////////
  // Insert tuple rather than install version
  ////////////////////////////////////////////
  auto target_table_schema = target_table->GetSchema();

  storage::Tuple new_tuple(target_table_schema, true);

  expression::ContainerTuple<storage::TileGroup> old_tuple(tile_group,
                                                           physical_tuple_id);

  DoProjection(&new_tuple, &old_tuple, values, col_ids, target_size,
               direct_list_, exec_context);

  // insert tuple into the table.
  ItemPointer *index_entry_ptr = nullptr;
  peloton::ItemPointer location =
      target_table->InsertTuple(&new_tuple, current_txn, &index_entry_ptr);

  // it is possible that some concurrent transactions have inserted the
  // same tuple.
  // in this case, abort the transaction.
  if (location.block == INVALID_OID) {
    txn_mgr.SetTransactionResult(current_txn,
                                             peloton::ResultType::FAILURE);
    return false;
  }

  txn_mgr.PerformInsert(current_txn, location, index_entry_ptr);

  return true;
}

bool TransactionRuntime::PerformUpdate(
    concurrency::Transaction &txn, storage::DataTable *target_table,
    storage::TileGroup &tile_group, uint32_t physical_tuple_id,
    uint32_t *col_ids, type::Value *target_vals, bool update_primary_key,
    Target *target_list, uint32_t target_list_size, DirectMap *direct_list,
    uint32_t direct_list_size, executor::ExecutorContext *executor_context_) {

  storage::TileGroupHeader *tile_group_hdr= tile_group.GetHeader();
  auto tile_group_id = tile_group.GetTileGroupId();

  auto &txn_mgr =
      concurrency::TransactionManagerFactory::GetInstance();

  TargetList target_list_;
  for (uint32_t i = 0; i < target_list_size; i++) {
    target_list_.emplace_back(target_list[i]);
  }
  DirectMapList direct_list_;
  for (uint32_t i = 0; i < direct_list_size; i++) {
    direct_list_.emplace_back(direct_list[i]);
  }

  // Update tuple in a given table

  ItemPointer old_location(tile_group_id, physical_tuple_id);

  bool is_owner = txn_mgr.IsOwner(&txn, tile_group_hdr, physical_tuple_id);
  bool is_written = txn_mgr.IsWritten(&txn, tile_group_hdr, physical_tuple_id);

  PL_ASSERT((is_owner == false && is_written == true) == false);

  // Prepare to examine primary key
  bool ret = false;

  if (is_owner == true && is_written == true) {
    if (update_primary_key) {
      // Update primary key
      ret = PerformUpdatePrimaryKey(
          &txn, is_owner, tile_group_hdr, target_table,
          physical_tuple_id, old_location, &tile_group, target_vals, col_ids,
          target_list_size, direct_list_, executor_context_);

      // Examine the result
      if (ret == true) {
        executor_context_->num_processed += 1;  // updated one
      }
      // When fail, ownership release is done inside PerformUpdatePrimaryKey
      else {
        return false;
      }
    }

    // Normal update (no primary key)
    else {
      // We have already owned a version

      // Make a copy of the original tuple and allocate a new tuple
      expression::ContainerTuple<storage::TileGroup> old_tuple(
          &tile_group, physical_tuple_id);
      // Execute the projections
      DoProjection(&old_tuple, &old_tuple, target_vals, col_ids,
                   target_list_size, direct_list_, executor_context_);

      txn_mgr.PerformUpdate(&txn, old_location);
    }
  }
  // if we have already got the ownership
  else {
    // Skip the IsOwnable and AcquireOwnership if we have already got the
    // ownership
    bool is_ownable = is_owner ||
                      txn_mgr.IsOwnable(
                          &txn, tile_group_hdr, physical_tuple_id);

    if (is_ownable == true) {
      // if the tuple is not owned by any transaction and is visible to
      // current transaction.

      bool acquire_ownership_success =
          is_owner ||
          txn_mgr.AcquireOwnership(&txn, tile_group_hdr,
                                               physical_tuple_id);

      if (acquire_ownership_success == false) {
        LOG_TRACE("Fail to insert new tuple. Set txn failure.");
        txn_mgr.SetTransactionResult(&txn,
                                                 ResultType::FAILURE);
        return false;
      }

      if (update_primary_key) {
        // Update primary key
        ret = PerformUpdatePrimaryKey(
            &txn, is_owner, tile_group_hdr, target_table,
            physical_tuple_id, old_location, &tile_group, target_vals, col_ids,
            target_list_size, direct_list_, executor_context_);

        if (ret == true) {
          executor_context_->num_processed += 1;  // updated one
        }
        // When fail, ownership release is done inside PerformUpdatePrimaryKey
        else {
          return false;
        }
      }

      // Normal update (no primary key)
      else {
        // if it is the latest version and not locked by other threads, then
        // insert a new version.

        // acquire a version slot from the table.
        ItemPointer new_location = target_table->AcquireVersion();

        auto &manager = catalog::Manager::GetInstance();
        auto new_tile_group = manager.GetTileGroup(new_location.block);

        expression::ContainerTuple<storage::TileGroup> new_tuple(
            new_tile_group.get(), new_location.offset);

        expression::ContainerTuple<storage::TileGroup> old_tuple(
            &tile_group, physical_tuple_id);

        // perform projection from old version to new version.
        // this triggers in-place update, and we do not need to allocate
        // another version.
        DoProjection(&new_tuple, &old_tuple, target_vals, col_ids,
                     target_list_size, direct_list_, executor_context_);

        // get indirection.
        ItemPointer *indirection =
            tile_group_hdr->GetIndirection(old_location.offset);
        // finally install new version into the table
        ret = target_table->InstallVersion(&new_tuple, &target_list_,
                                            &txn, indirection);

        // PerformUpdate() will not be executed if the insertion failed.
        // There is a write lock acquired, but since it is not in the write
        // set,
        // because we haven't yet put them into the write set.
        // the acquired lock can't be released when the txn is aborted.
        // the YieldOwnership() function helps us release the acquired write
        // lock.
        if (ret == false) {
          LOG_TRACE("Fail to insert new tuple. Set txn failure.");
          if (is_owner == false) {
            // If the ownership is acquire inside this update executor, we
            // release it here
            txn_mgr.YieldOwnership(&txn, tile_group_hdr,
                                               physical_tuple_id);
          }
          txn_mgr.SetTransactionResult(&txn,
                                                   ResultType::FAILURE);
          return false;
        }

        LOG_TRACE("perform update old location: %u, %u", old_location.block,
                  old_location.offset);
        LOG_TRACE("perform update new location: %u, %u", new_location.block,
                  new_location.offset);
        txn_mgr.PerformUpdate(&txn, old_location,
                                          new_location);

        // TODO: Why don't we also do this in the if branch above?
        executor_context_->num_processed += 1;  // updated one
      }
    } else {
      // transaction should be aborted as we cannot update the latest version.
      LOG_TRACE("Fail to update tuple. Set txn failure.");
      txn_mgr.SetTransactionResult(&txn,
                                               ResultType::FAILURE);
      return false;
    }
  }
  return true;
}


void TransactionRuntime::IncreaseNumProcessed(
    executor::ExecutorContext *executor_context) {
  executor_context->num_processed++;
}

}  // namespace codegen
}  // namespace peloton
