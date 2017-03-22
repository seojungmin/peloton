//===----------------------------------------------------------------------===//
//
//                         PelotonDB
//
// case_expression.h
//
// Identification: src/include/expression/case_expression.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "expression/abstract_expression.h"
#include "expression/comparison_expression.h"
#include "common/sql_node_visitor.h"

namespace peloton {
namespace expression {

//===----------------------------------------------------------------------===//
// CaseExpression
//===----------------------------------------------------------------------===//

class CaseExpression : public AbstractExpression {
 public:
  using AbsExprPtr = std::unique_ptr<AbstractExpression>;
  using WhenClause = std::pair<AbsExprPtr,AbsExprPtr>;

  CaseExpression(type::Type::TypeId type_id, AbsExprPtr argument,
                 std::vector<WhenClause> &when_clauses,
                 AbsExprPtr dflt_expression)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR, type_id),
        argument_(std::move(argument)),
        clauses_(std::move(when_clauses)),
        dfltexpr_(std::move(dflt_expression)) {
    if (argument_ != nullptr) {
      for (int i = 0; i < (int)GetClauseSize(); i++)
		clauses_[i].first.reset(
           new peloton::expression::ComparisonExpression(
              peloton::ExpressionType::COMPARE_EQUAL, argument_->Copy(),
              clauses_[i].first->Copy()));
    }
  }
  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    for (auto &clause : clauses_) {
      auto result = clause.first->Evaluate(tuple1, tuple2, context);
      if (result.IsTrue())
        return clause.second->Evaluate(tuple1, tuple2, context);
    }
    if (dfltexpr_ == nullptr)
      return type::ValueFactory::GetNullValueByType(return_value_type_);
    return dfltexpr_->Evaluate(tuple1, tuple2, context);
  }

  AbstractExpression* Copy() const override { 
    std::vector<WhenClause> copies;
    for (auto &clause : clauses_)
      copies.push_back(WhenClause(AbsExprPtr(clause.first->Copy()),
                                  AbsExprPtr(clause.second->Copy())));
	return new CaseExpression(return_value_type_, nullptr, copies,
                              dfltexpr_ != nullptr ?
                                  AbsExprPtr(dfltexpr_->Copy()) : nullptr);
  }

  virtual void Accept(SqlNodeVisitor* v) { v->Visit(this); }

  size_t GetClauseSize() const { return clauses_.size(); }

  AbstractExpression *GetModifiableClause(int index) const {
    if (index < 0 || index >= (int)clauses_.size())
      return nullptr;
    return clauses_[index].first.get();
  }

 private:
  AbsExprPtr argument_;
  std::vector<WhenClause> clauses_;
  AbsExprPtr dfltexpr_;
};

}  // End expression namespace
}  // End peloton namespace
