//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime.h
//
// Identification: src/include/codegen/transaction_runtime.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

#include "concurrency/transaction.h"
#include "concurrency/transaction_manager_factory.h"
#include "common/container_tuple.h"
#include "catalog/manager.h"
#include "executor/executor_context.h"
#include "storage/tuple.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class TileGroup;
}  // namespace storage

namespace codegen {

//===----------------------------------------------------------------------===//
// This class contains common runtime functions needed during query execution.
// These functions are used exclusively by the codegen component.
//===----------------------------------------------------------------------===//
class TransactionRuntime {

public:
  // Perform a read operation for all tuples in the given tile group with IDs
  // in the range [tid_start, tid_end) in the context of the given transaction
  static uint32_t PerformVectorizedRead(concurrency::Transaction &txn,
                                        storage::TileGroup &tile_group,
                                        uint32_t tid_start, uint32_t tid_end,
                                        uint32_t *selection_vector);

  // Perform a delete operation
  static bool PerformDelete(uint32_t tuple_id, concurrency::Transaction *txn,
                            storage::DataTable *table,
                            storage::TileGroup *tile_group);

  // Perform an update operation
  static bool PerformUpdate(concurrency::Transaction &txn,
                            storage::DataTable &table,
                            storage::TileGroup *tile_group,
                            uint32_t tuple_offset, uint32_t *col_ids,
                            type::Value *target_vals, bool update_primary_key,
                            Target *target_vector, uint32_t target_vector_size, 
                            DirectMap *direct_map_vector,
                            uint32_t direct_map_size,
                            executor::ExecutorContext *executor_context);

  static void IncreaseNumProcessed(executor::ExecutorContext *executor_context);

};

}  // namespace codegen
}  // namespace peloton
