//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// insert_plan.cpp
//
// Identification: src/planner/insert_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/insert_plan.h"
#include "catalog/catalog.h"
#include "expression/constant_value_expression.h"
#include "storage/data_table.h"
#include "type/ephemeral_pool.h"

namespace peloton {
namespace planner {

InsertPlan::InsertPlan(storage::DataTable *table, oid_t bulk_insert_count)
    : target_table_(table), bulk_insert_count(bulk_insert_count) {
  LOG_TRACE("Creating an Insert Plan");
}

InsertPlan::InsertPlan(storage::DataTable *table,
    std::unique_ptr<const planner::ProjectInfo> &&project_info,
    oid_t bulk_insert_count)
    : target_table_(table),
      project_info_(std::move(project_info)),
      bulk_insert_count(bulk_insert_count) {
  LOG_TRACE("Creating an Insert Plan with a project info");
}

InsertPlan::InsertPlan(storage::DataTable *table,
                       std::unique_ptr<storage::Tuple> &&tuple,
                       oid_t bulk_insert_count)
    : target_table_(table), bulk_insert_count(bulk_insert_count) {
  LOG_TRACE("Creating an Insert Plan with a tuple");
  tuples_.push_back(std::move(tuple));
}

InsertPlan::InsertPlan(storage::DataTable *table, std::vector<char *> *columns,
    std::vector<std::vector<peloton::expression::AbstractExpression *> *> *
        insert_values)
    : target_table_(table), bulk_insert_count(insert_values->size()) {
  PL_ASSERT(target_table_ != nullptr);
  LOG_TRACE("Creating an Insert Plan with column values");

  parameter_vector_.reset(new std::vector<std::tuple<oid_t, oid_t, oid_t>>());
  params_value_type_.reset(new std::vector<type::TypeId>);

  const catalog::Schema *schema = target_table_->GetSchema();
  std::vector<oid_t> query_column_ids;
  size_t query_columns_cnt;
  if (columns == nullptr) {
    for (oid_t id = 0; id < schema->GetColumns().size(); id++)
      query_column_ids.push_back(id);
    query_columns_cnt = (*insert_values)[0]->size();
  }
  else {
    for (auto col_name : *columns)
      query_column_ids.push_back(schema->GetColumnID(col_name));
    query_columns_cnt = columns->size();
  }

  for (uint32_t tuple_id = 0; tuple_id < insert_values->size(); tuple_id++) {
    auto values = (*insert_values)[tuple_id];
    // columns has to be less than or equal that of schema
    PL_ASSERT(query_columns_cnt <= schema->GetColumnCount());
    std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(schema, true));
    int param_index = 0;
    size_t pos = 0;
    for (auto col_id : query_column_ids) {
      PL_ASSERT(col_id != INVALID_OID);

      LOG_TRACE("Column %d found in INSERT query, ExpressionType: %s", col_id,
          ExpressionTypeToString(values->at(pos)->GetExpressionType()).c_str());

      expression::AbstractExpression *elem = values->at(pos);
      if (elem->GetExpressionType() == ExpressionType::VALUE_PARAMETER) {
        std::tuple<oid_t, oid_t, oid_t> pair = std::make_tuple(tuple_id, col_id,                                                               param_index);
        parameter_vector_->push_back(pair);
        params_value_type_->push_back(schema->GetColumn(col_id).GetType());
        param_index++;
      } else {
        expression::ConstantValueExpression *const_expr_elem =
            dynamic_cast<expression::ConstantValueExpression *>(elem);
        auto const_expr_elem_val = const_expr_elem->GetValue();
        switch (const_expr_elem->GetValueType()) {
          case type::TypeId::VARCHAR:
          case type::TypeId::VARBINARY:
            tuple->SetValue(col_id, const_expr_elem_val, GetPlanPool());
            break;
          default: {
            tuple->SetValue(col_id, const_expr_elem_val, nullptr);
          }
        }
      }
      pos++;
    }

    // Insert a null value for non-specified columns
    auto &table_columns = schema->GetColumns();
    auto table_columns_cnt = schema->GetColumnCount();
    if (query_columns_cnt < table_columns_cnt) {
      for (size_t col_id = 0; col_id < table_columns_cnt; col_id++) {
        auto col = table_columns[col_id];
        if (std::find(columns->begin(), columns->end(), col.GetName()) ==
            columns->end()) {
          tuple->SetValue(col_id, type::ValueFactory::GetNullValueByType(
              col.GetType()), nullptr);
        }
      }
    }
    LOG_TRACE("Tuple to be inserted: %s", tuple->GetInfo().c_str());
    tuples_.push_back(std::move(tuple));
  }
}

type::AbstractPool *InsertPlan::GetPlanPool() {
  if (pool_.get() == nullptr) {
    pool_.reset(new type::EphemeralPool());
  }
  return pool_.get();
}

void InsertPlan::SetParameterValues(std::vector<type::Value> *values) {
  PL_ASSERT(values->size() == parameter_vector_->size());
  LOG_TRACE("Set Parameter Values in Insert");
  for (unsigned int i = 0; i < values->size(); ++i) {
    auto param_type = params_value_type_->at(i);
    auto &put_loc = parameter_vector_->at(i);
    auto value = values->at(std::get<2>(put_loc));
    switch (param_type) {
      case type::TypeId::VARBINARY:
      case type::TypeId::VARCHAR: {
        type::Value val = (value.CastAs(param_type));
        tuples_[std::get<0>(put_loc)]
            ->SetValue(std::get<1>(put_loc), val, GetPlanPool());
        break;
      }
      default: {
        type::Value val = (value.CastAs(param_type));
        tuples_[std::get<0>(put_loc)]
            ->SetValue(std::get<1>(put_loc), val, nullptr);
      }
    }
  }
}

}  // namespace planner
}  // namespace peloton
