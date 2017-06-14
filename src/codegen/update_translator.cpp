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
#include "codegen/updater_proxy.h"
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
  target_table_ = update_plan.GetTable();
  const auto *project_info = update_plan_.GetProjectInfo();
  for (const auto &target : project_info->GetTargetList()) {
    const auto &derived_attribute = target.second;
    context.Prepare(*derived_attribute.expr);
  }

  // Set RuntimeState
  auto &codegen = GetCodeGen();
  auto &runtime_state = context.GetRuntimeState();
  auto column_num = projection_info->GetTargetList().size();

#if 0
  target_val_vec_id_ = runtime_state.RegisterState( "updateTargetValVec",
      codegen.VectorType(ValueProxy::GetType(codegen), column_num), true);

  column_ids_vec_id_ = runtime_state.RegisterState( "updateColumnIdsVec",
      codegen.VectorType(codegen.Int64Type(), column_num), true);
#endif

  updater_state_id_ = runtime.RegitsterState("updater",
                                             UpdaterProxy::GetType(codegen));
}

void UpdateTranslator::InitializeState() {
  auto &codegen = GetCodeGen();
  auto &context = GetCompilationContext();

  llvm::Value *txn_ptr = context.GetTransactionPtr();

  storage::DataTable *table = insert_plan_.GetTable();
  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {GetCatalogPtr(), codegen.Const32(table->GetDatabaseOid()),
       codegen.Const32(table->GetOid())});

  llvm::Value *update_primary_key_ptr = codegen.ConstBool(
      update_plan_.GetUpdatePrimaryKey);

  const auto *project_info = update_plan_.GetProjectInfo();
  auto *target_list_ptr = &project_info->GetTargetList();
  auto *target_list_ptr_int = codegen.Const64((int64_t)target_list_ptr);
  llvm::Value *target_list_ptr_ptr = codegen-CreateIntToPtr(target_list_ptr_int,
      TargetListProxy::GetType(codegen)->getPointerTo());

  auto *direct_map_list_ptr = &project_info->GetDirectMapList();
  auto *direct_map_ptr_int = codegen.Const64((int64_t)direct_map_list_ptr);
  llvm::Value *direct_map_list_ptr_ptr = codegen-CreateIntToPtr(
      direct_map_list_ptr_int,
      TargetListProxy::GetType(codegen)->getPointerTo());

  // Initialize the inserter with txn and table
  llvm::Value *inserter = LoadStatePtr(inserter_state_id_);
  std::vector<llvm::Value *> args = {updater, txn_ptr, table_ptr,
                                     target_list_ptr_ptr,
                                     target_list_size_ptr,
                                     direct_map_list_ptr_ptr,
                                     direct_map_list_size_ptr,
                                     update_primary_key_ptr};
  codegen.CallFunc(UpdaterProxy::_Init::GetFunction(codegen), args);
}

void UpdateTranslator::Produce() const {
  GetCompilationContext().Produce(*update_plan_.GetChild(0));
}

void UpdateTranslator::Consume(ConsumerContext &, RowBatch::Row &row) const {
  CodeGen &codegen = GetCodeGen();

#if 0
  // vector for collecting results from executing target list
  llvm::Value *target_val_vec = LoadStateValue(target_val_vec_id_);

  // vector for collecting col ids that are targeted to update
  uint32_t column_num = target_list_.size();
  Vector column_ids{LoadStateValue(column_ids_vec_id_), column_num,
                    codegen.Int64Type()};

  llvm::Value *target_list_ptr = LoadStateValue(target_list_state_id_);
  /* Loop for collecting target col ids and corresponding derived values */
  for (uint32_t i = 0; i < target_list_.size(); i++) {
    auto target_id = codegen.Const64(i);

    // Collect column ids
    column_ids.SetValue(codegen, target_id,
                        codegen.Const64(target_list_[i].first));

    // Derive value using target list (execute target list expr)
    const auto &derived_attribute = target_list_[i].second;
    codegen::Value val = row.DeriveValue(codegen, *derived_attribute.expr);
    // collect derived value
    switch (val.GetType()) {
      case type::Type::TypeId::TINYINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTinyInt::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::SMALLINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputSmallInt::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DATE:
      case type::Type::TypeId::INTEGER: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputInteger::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::TIMESTAMP: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputTimestamp::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::BIGINT: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputBigInt::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::DECIMAL: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputDouble::GetFunction(codegen),
            {target_vec, target_id, val.GetValue()});
        break;
      }
      case type::Type::TypeId::VARBINARY: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarbinary::GetFunction(codegen),
            {target_vec, target_id, val.GetValue(), val.GetLength()});
        break;
      }
      case type::Type::TypeId::VARCHAR: {
        codegen.CallFunc(
            ValuesRuntimeProxy::_OutputVarchar::GetFunction(codegen),
            {target_vec, target_id, val.GetValue(), val.GetLength()});
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
  llvm::Value *column_ids_ptr = column_ids.GetVectorPtr();
#endif

  llvm::Value *executor_context = context.GetExecutorContextPtr();

  // Call Update!!
  auto *updater = LoadStatePtr(updater_state_id_);
  auto *update_func = UpdaterProxy::_Update::GetFunction(codegen);
  codegen.CallFunc(update_func, {updater, row.GetTileGroupID(), row.GetTID,
                                 column_ids_ptr, target_vec, executor_context});
  // Increase the counter
  auto *processed_func =
      TransactionRuntimeProxy::_IncreaseNumProcessed::GetFunction(codegen);
  codegen.CallFunc(processed_func, {executor_context});
}

}  // namespace codegen
}  // namespace peloton
