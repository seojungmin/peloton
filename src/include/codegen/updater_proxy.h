//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// updater_proxy.h
//
// Identification: src/include/codegen/updater_proxy.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/codegen.h"
#include "codegen/data_table_proxy.h"
#include "codegen/updater.h"
#include "codegen/transaction_proxy.h"

namespace peloton {
namespace codegen {

class UpdaterProxy {
 public:
  static llvm::Type *GetType(CodeGen &codegen) {
    static const std::string kUpdaterTypeName = "peloton::codegen::Updater";
    // Check if the data table type has already been registered in the current
    // codegen context
    auto updater_type = codegen.LookupTypeByName(kUpdaterTypeName);
    if (updater_type != nullptr) {
      return updater_type;
    }
  
    // Type isn't cached, create a new one
    auto *opaque_arr_type =
        codegen.VectorType(codegen.Int8Type(), sizeof(Updater));
    return llvm::StructType::create(codegen.GetContext(), {opaque_arr_type},
                                  kUpdaterTypeName);
  }

  struct _Init {
    static const std::string &GetFunctionName() {
      static const std::string kInitFnName =
#ifdef __APPLE__
          "";
#else
          "";
#endif
      return kInitFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          UpdaterProxy::GetType(codegen)->getPointerTo(),
          TransactionProxy::GetType(codegen)->getPointerTo(),
          DataTableProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };

  struct _Update {
    static const std::string &GetFunctionName() {
      static const std::string kInsertFnName =
#ifdef __APPLE__
          "";
#else
          "";
#endif
      return kInsertFnName;
    }
    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string &fn_name = GetFunctionName();

      llvm::Function *llvm_fn = codegen.LookupFunction(fn_name);
      if (llvm_fn != nullptr) {
        return llvm_fn;
      }

      std::vector<llvm::Type *> fn_args = {
          UpdaterProxy::GetType(codegen)->getPointerTo(),
          TupleProxy::GetType(codegen)->getPointerTo()};
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.VoidType(), fn_args, false);
      return codegen.RegisterFunction(fn_name, fn_type);
    }
  };
};

}  // namespace codegen
}  // namespace peloton
