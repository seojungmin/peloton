//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater.cpp
//
// Identification: src/codegen/updater.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/updater.h"
#include "codegen/transaction_runtime.h"
#include "type/types.h"

namespace peloton {
namespace codegen {

// Set the values required in the update process
void Updater::Init(concurrency::Transaction *txn, storage::DataTable *table,
                   Target *target_vector, uint32_t target_vector_size,
                   DirectMap *direct_map_vector,
                   uint32_t direct_map_vector_size, bool update_primary_key) {
  PL_ASSERT(txn != nullptr && table != nullptr);
  //PL_ASSERT(target_vector != nullptr && direct_map_vector != nullptr);

  txn_ = txn;
  table_ = table;
  update_primary_key_ = update_primary_key;

#if 0
  target_vector_ = target_vector;
  target_vector_size_ = target_vector_size;
  direct_map_vector_ = direct_map_vector;
  direct_map_vector_size_ = direct_map_vector_size;
#endif
}

// Do the projection temporilily
void Updater::Materialize(storage::TileGroup *tile_group, uint32_t tuple_offset,
                          uint32_t *col_ids, type::Value *target_vals,
                          executor::ExecutorContext *executor_context) {
  PL_ASSERT(txn_ != nullptr && table_ != nullptr);

  auto *tile_group_header = tile_group->GetHeader();
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  bool is_written = txn_manager.IsWritten(&txn_, tile_group_header,
                                          tuple_offset);

  
  if (update_primary_key_ == true) {
    // DoProjection
    storage::Tuple tuple = 
  }
  else if (is_written == true) {
  }
  else {
  }
  tuple_ = &
  // Get memory address of the tuple to be projected

  // Do the projection

  // For the time being 
}

// Call the transaction runtime to handle the access control and the job
void Updater::Update(storage::TileGroup *tile_group, uint32_t tuple_offset,
                     executor::ExecutorContext *executor_context) {

  PL_ASSERT(txn_ != nullptr && table_ != nullptr);
  auto result = TransactionRuntime::PerformUpdate(*txn_, *table_, tile_group,
                                                  tuple_offset,
                                                  tuple_, update_primary_key_);
  if (result == true) {
    TransactionRuntime::IncreaseNumProcessed(executor_context);
  }
}

}  // namespace codegen
}  // namespace peloton
