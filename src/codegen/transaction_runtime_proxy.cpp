//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_runtime_proxy.cpp
//
// Identification: src/codegen/transaction_runtime_proxy.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/transaction_runtime_proxy.h"

#include "codegen/direct_map_proxy.h"
#include "codegen/executor_context_proxy.h"
#include "codegen/data_table_proxy.h"
#include "codegen/target_proxy.h"
#include "codegen/tile_group_proxy.h"
#include "codegen/transaction_proxy.h"
#include "codegen/value_proxy.h"

namespace peloton {
namespace codegen {

const std::string &
TransactionRuntimeProxy::_PerformVectorizedRead::GetFunctionName() {
  static const std::string kPerformVectorizedReadFnName =
      "__ZN7peloton7codegen18TransactionRuntime21PerformVectorizedReadERNS_"
      "11concurrency11TransactionERNS_7storage9TileGroupEjjPj";
  return kPerformVectorizedReadFnName;
}

llvm::Function *TransactionRuntimeProxy::_PerformVectorizedRead::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  llvm::Type *ret_type = codegen.Int32Type();
  std::vector<llvm::Type *> arg_types = {
      TransactionProxy::GetType(codegen)->getPointerTo(),  // txn *
      TileGroupProxy::GetType(codegen)->getPointerTo(),    // tile_group *
      codegen.Int32Type(),                                 // tid_start
      codegen.Int32Type(),                                 // tid_end
      codegen.Int32Type()->getPointerTo()};                // selection_vector
  auto *fn_type = llvm::FunctionType::get(ret_type, arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &TransactionRuntimeProxy::_PerformDelete::GetFunctionName() {
  static const std::string performDeleteFnName =
      "_ZN7peloton7codegen18TransactionRuntime13PerformDelete"
      "EjPNS_11concurrency11TransactionEPNS_7storage9DataTableEPNS5_"
      "9TileGroupE";
  return performDeleteFnName;
}

llvm::Function *TransactionRuntimeProxy::_PerformDelete::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> fn_args{codegen.Int32Type(),
      TransactionProxy::GetType(codegen)->getPointerTo(),
      DataTableProxy::GetType(codegen)->getPointerTo(),
      TileGroupProxy::GetType(codegen)->getPointerTo()};
  llvm::FunctionType *fn_type = llvm::FunctionType::get(codegen.BoolType(),
                                                        fn_args, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &TransactionRuntimeProxy::_PerformUpdate::GetFunctionName() {
  static const std::string kPerformUpdateFnName =
      "_ZN7peloton7codegen18TransactionRuntime13PerformUpdateERNS_"
      "11concurrency11TransactionEPNS_7storage9DataTableERNS5_"
      "9TileGroupEjPjPNS_4type5ValueEbPSt4pairIjKNS_"
      "7planner16DerivedAttributeEEjPSE_IjSE_IjjEEjPNS_"
      "8executor15ExecutorContextE";
  return kPerformUpdateFnName;
}

llvm::Function *TransactionRuntimeProxy::_PerformUpdate::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> arg_types = {
      TransactionProxy::GetType(codegen)->getPointerTo(), // txn *
      DataTableProxy::GetType(codegen)->getPointerTo(),   // target_table_ *
      TileGroupProxy::GetType(codegen)->getPointerTo(),   // tile_group *
      codegen.Int32Type(),                                // physical_tuple_id
      codegen.Int64Type()->getPointerTo(),                // col_ids
      ValueProxy::GetType(codegen)->getPointerTo(),       // target_vals
      codegen.BoolType(),                                 // update_primary_key
      TargetProxy::GetType(codegen)->getPointerTo(),      // target_list *
      codegen.Int64Type(),                                // target_list_size
      DirectMapProxy::GetType(codegen)->getPointerTo(),   // direct_list *
      codegen.Int64Type(),                                // direct_list_size
      ExecutorContextProxy::GetType(codegen)->getPointerTo() // exec_context *
  };
  auto *fn_type = llvm::FunctionType::get(codegen.BoolType(), arg_types,
                                          false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

const std::string &
TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunctionName() {
  static const std::string kIncreaseNumProcessedFnName =
      "_ZN7peloton7codegen18TransactionRuntime20IncreaseNumProcessedEPNS_"
      "8executor15ExecutorContextE";
  return kIncreaseNumProcessedFnName;
}

llvm::Function *TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunction(
    CodeGen &codegen) {
  const std::string &fn_name = GetFunctionName();

  // Has the function already been registered?
  llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
  if (llvm_fn != nullptr) {
    return llvm_fn;
  }

  std::vector<llvm::Type *> arg_types = {
      ExecutorContextProxy::GetType(codegen)->getPointerTo()};

  auto *fn_type = llvm::FunctionType::get(codegen.VoidType(), arg_types, false);
  return codegen.RegisterFunction(fn_name, fn_type);
}

}  // namespace codegen
}  // namespace peloton
