//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// insert_plan.h
//
// Identification: src/include/planner/insert_plan.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"
#include "planner/project_info.h"

namespace peloton {

namespace storage {
class DataTable;
class Tuple;
}

namespace planner {
class InsertPlan : public AbstractPlan {
 public:
  // Constructor for INSERT INTO SELECT - used by the optimizer
  InsertPlan(storage::DataTable *table, oid_t bulk_insert_count = 1);

  // Constuctor for a direct tuple insertion
  InsertPlan(storage::DataTable *table, std::unique_ptr<storage::Tuple> &&tuple,
             oid_t bulk_insert_count = 1);

  // Constructor for a ProjectInfo specification
  InsertPlan(storage::DataTable *table,
             std::unique_ptr<const planner::ProjectInfo> &&project_info,
             oid_t bulk_insert_count = 1);

  // Constructor for INSERT with column values - used by the optimizer
  InsertPlan(storage::DataTable *table, std::vector<char *> *columns,
      std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
          insert_values);

  inline PlanNodeType GetPlanNodeType() const { return PlanNodeType::INSERT; }

  const std::string GetInfo() const { return "InsertPlan"; }

  storage::DataTable *GetTable() const { return target_table_; }

  const planner::ProjectInfo *GetProjectInfo() const {
    return project_info_.get();
  }

  oid_t GetBulkInsertCount() const { return bulk_insert_count; }

  type::AbstractPool *GetPlanPool();

  const storage::Tuple *GetTuple(int tuple_idx) const {
    if (tuple_idx >= (int)tuples_.size()) {
      return nullptr;
    }
    return tuples_[tuple_idx].get();
  }

  void SetParameterValues(std::vector<type::Value> *values);

  std::unique_ptr<AbstractPlan> Copy() const {
    // TODO: Add copying mechanism
    std::unique_ptr<AbstractPlan> dummy;
    return dummy;
  }

 private:
  storage::DataTable *target_table_ = nullptr;

  std::unique_ptr<const planner::ProjectInfo> project_info_;

  std::vector<std::unique_ptr<storage::Tuple>> tuples_;

  // <tuple_index, tuple_column_index, parameter_index>
  std::unique_ptr<std::vector<std::tuple<oid_t, oid_t, oid_t>>>
      parameter_vector_;

  // Parameter values
  std::unique_ptr<std::vector<type::TypeId>> params_value_type_;

  // Number of times to insert
  oid_t bulk_insert_count;

  // Pool for variable length types
  std::unique_ptr<type::AbstractPool> pool_;

 private:
  DISALLOW_COPY_AND_MOVE(InsertPlan);
};

}  // namespace planner
}  // namespace peloton
