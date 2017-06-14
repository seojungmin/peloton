//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.cpp
//
// Identification: src/codegen/update_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/catalog_proxy.h"
#include "codegen/direct_map_proxy.h"
#include "codegen/target_proxy.h"
#include "codegen/transaction_runtime_proxy.h"
#include "codegen/update_translator.h"
#include "codegen/value_proxy.h"
#include "codegen/values_runtime_proxy.h"
#include "storage/data_table.h"

namespace peloton {
namespace codegen {

UpdateTranslator::UpdateTranslator(const planner::UpdatePlan &update_plan,
                                   CompilationContext &context,
                                   Pipeline &pipeline)
    : OperatorTranslator(context, pipeline),
      update_plan_(update_plan),
      table_(*update_plan.GetTable()) {
  // Also create the translator for our child.
  context.Prepare(*update_plan.GetChild(0), pipeline);

  const auto *project_info = update_plan_.GetProjectInfo();

  // Retrieve all the required information for update
  target_table_ = update_plan.GetTable();
  update_primary_key_ = update_plan.GetUpdatePrimaryKey();
  PL_ASSERT(project_info != nullptr && target_table_ != nullptr);
  for (uint32_t i = 0; i < project_info->GetTargetList().size(); i++) {
    target_list_.emplace_back(project_info->GetTargetList()[i]);
  }
  direct_map_list_ = project_info->GetDirectMapList();

  // Prepare translator for target list
  for (const auto &target : target_list_) {
    const auto &derived_attribute = target.second;
    PL_ASSERT(derived_attribute.expr != nullptr);
    context.Prepare(*derived_attribute.expr);
  }

  // Store runtime params for update operation
  context.StoreTargetList(target_list_);
  context.StoreDirectList(direct_map_list_);

  // Set RuntimeState
  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  target_val_vec_id_ = runtime_state.RegisterState( "updateTargetVec",
      codegen.VectorType(ValueProxy::GetType(codegen), target_list_.size()),
      true);
  col_id_vec_id_ = runtime_state.RegisterState( "updateColVec",
      codegen.VectorType(codegen.Int64Type(), target_list_.size()), true);
  target_list_state_id_ = runtime_state.RegisterState( "targetList",
      TargetProxy::GetType(codegen)->getPointerTo());
  direct_map_list_state_id_ = runtime_state.RegisterState( "directMapList",
      DirectMapProxy::GetType(codegen)->getPointerTo());
}

void UpdateTranslator::Produce() const {
  GetCompilationContext().Produce(*update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  CompilationContext &context = GetCompilationContext();
  CodeGen &codegen = GetCodeGen();

  // Preparation: Set all the arguments
  llvm::Value *tid = row.GetTID(codegen);
  llvm::Value *catalog_ptr = context.GetCatalogPtr();
  llvm::Value *txn_ptr = context.GetTransactionPtr();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {catalog_ptr, codegen.Const32(target_table_->GetDatabaseOid()),
       codegen.Const32(target_table_->GetOid())});
  llvm::Value *tile_group = table_.GetTileGroup(codegen, table_ptr,
                                                row.GetTileGroupID());
  llvm::Value *update_primary_key = codegen.ConstBool(update_primary_key_);

  // vector for collecting results from executing target list
  llvm::Value *target_vec = LoadStateValue(target_val_vec_id_);

  // vector for collecting col ids that are targeted to update
  Vector col_vec{LoadStateValue(col_id_vec_id_),
                 (uint32_t)target_list_.size(), codegen.Int64Type()};

  llvm::Value *target_list_ptr = LoadStateValue(target_list_state_id_);
  llvm::Value *target_list_size = codegen.Const64(target_list_.size());

  /* Loop for collecting target col ids and corresponding derived values */
  for (uint32_t i = 0; i < target_list_.size(); i++) {
    auto target_list_id = codegen.Const64(i);

    // collect col id
    col_vec.SetValue(codegen, target_list_id,
                     codegen.Const64(target_list_[i].first));

    // Derive value using target list (execute target list expr)
    const auto &derived_attribute = target_list_[i].second;
    codegen::Value val = row.DeriveValue(codegen, *derived_attribute.expr);
    // collect derived value
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARBINARY: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarbinary::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue(), val.GetLength()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
            {target_vec, target_list_id, val.GetValue(), val.GetLength()});
        break;
      }
      default: {
        std::string msg =
            StringUtil::Format("Can't serialize value type '%s' at position %u",
                               TypeIdToString(val.GetType()).c_str(), i);
        throw Exception{msg};
      }
    }
  }
  llvm::Value *col_ids = col_vec.GetVectorPtr();

  llvm::Value *direct_map_list_ptr = LoadStateValue(direct_map_list_state_id_);
  llvm::Value *direct_map_list_size = codegen.Const64(direct_map_list_.size());
  llvm::Value *exec_context = context.GetExecutorContextPtr();

  // Action: Call Update!!
  codegen.CallFunc(
      TransactionRuntimeProxy::_PerformUpdate::GetFunction(codegen),
      {txn_ptr, table_ptr, tile_group, tid, col_ids, target_vec,
       update_primary_key, target_list_ptr, target_list_size,
       direct_map_list_ptr, direct_map_list_size, exec_context});
}

}  // namespace codegen
}  // namespace peloton
