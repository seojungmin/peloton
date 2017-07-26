//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// hash_plan.cpp
//
// Identification: src/planner/hash_plan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/hash_plan.h"

namespace peloton {
namespace planner {

void HashPlan::PerformBinding(BindingContext &binding_context) {
  // Let the children bind
  AbstractPlan::PerformBinding(binding_context);

  // Now us
  for (auto &hash_key : hash_keys_) {
    auto *key_expr =
        const_cast<expression::AbstractExpression *>(hash_key.get());
    key_expr->PerformBinding({&binding_context});
  }
}

bool HashPlan::Equals(planner::AbstractPlan &plan) const {
  if (GetPlanNodeType() != plan.GetPlanNodeType())
    return false;

  auto &other = reinterpret_cast<planner::HashPlan &>(plan);
  auto hash_key_size = GetHashKeys().size();
  if (hash_key_size != other.GetHashKeys().size())
    return false;
  for (size_t i = 0; i < hash_key_size; i++) {
    if (!GetHashKeys().at(i).get()->Equals(other.GetHashKeys().at(i).get()))
      return false;
  }

  return AbstractPlan::Equals(plan);
}

}  // namespace planner
}  // namespace peloton
