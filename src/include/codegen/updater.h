//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater.h
//
// Identification: src/include/codegen/updater.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/consumer_context.h"

namespace peloton {

namespace concurrency {
class Transaction;
}  // namespace concurrency

namespace storage {
class DataTable;
class Tuple;
}  // namespace storage

namespace codegen {
// This class handles insertion of tuples from generated code. This avoids
// passing along information through translators, and is intialized once
// through its Init() outside the main loop
class Updater {
 public:
  // Initializes the instance
  void Init(concurrency::Transaction *txn, storage::DataTable *table,
            bool update_primary_key);

  // Update a tuple
  void Update(const storage::Tuple *tuple);

 private:
  // No external constructor
  Updater(): txn_(nullptr), table_(nullptr), update_primary_key_(false) {}

 private:
  // These are provided by its insert translator
  concurrency::Transaction *txn_;
  storage::DataTable *table_;
  bool update_primary_key_;

 private:
  DISALLOW_COPY_AND_MOVE(Updater);
};

}  // namespace codegen
}  // namespace peloton
