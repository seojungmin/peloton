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
#include "concurrency/transaction_manager_factory.h"
#include "storage/data_table.h"
#include "storage/tuple.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace codegen {

void Updater::Init(concurrency::Transaction *txn, storage::DataTable *table,
                   bool update_primary_key) {
  PL_ASSERT(txn != nullptr && table != nullptr);
  txn_ = txn;
  table_ = table;
  update_primary_key_ = update_primary_key;
}

void Updater::Update(uint32_t tile_group_id, uint32_t tuple_offset) {

  
  TransactionRuntime::PerformUpdate(*txn_, *table_, tile_group_id, tupel_offset,
                                    col_ids, target_vals, update_primary_key

}

}  // namespace codegen
}  // namespace peloton
