//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// update_translator.h
//
// Identification: src/include/codegen/update/update_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"
#include "codegen/pipeline.h"
#include "codegen/runtime_state.h"
#include "codegen/table.h"
#include "planner/update_plan.h"

namespace peloton {
namespace codegen {

class UpdateTranslator : public OperatorTranslator {
 public:
  // Constructor
  UpdateTranslator(const planner::UpdatePlan &plan, CompilationContext &context,
                   Pipeline &pipeline);

  // No state initialization
  void InitializeState() override;

  // No helper functions
  void DefineAuxiliaryFunctions() override {}

  // Produce
  void Produce() const override;

  // Consume : No Cosume() override for Batch
  void Consume(ConsumerContext &context, RowBatch::Row &row) const override;

  // No state finalization
  void TearDownState() override;

  // Get the stringified name of this translator
  std::string GetName() const override { return "Update"; }

 private:
  const planner::UpdatePlan &update_plan_;

  storage::DataTable *target_table_;
  bool update_primary_key_;
  TargetList target_list_;
  DirectMapList direct_map_list_;

  RuntimeState::StateID updater_state_id_;

//
  std::unique_ptr<Target[]> target_array;
  std::unique_ptr<DirectMap[]> direct_array;
  RuntimeState::StateID target_val_vec_id_;
  RuntimeState::StateID col_id_vec_id_;

  RuntimeState::StateID target_list_state_id_;
  RuntimeState::StateID direct_map_list_state_id_;

  codegen::Table table_; // usage?
};

}  // namespace codegen
}  // namespace peloton
