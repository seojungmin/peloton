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
  CaseExpression(type::Type::TypeId type_id, AbstractExpression *arg,
                 std::vector<AbstractExpression *> *clauses,
                 AbstractExpression *dftresult)
      : AbstractExpression(ExpressionType::OPERATOR_CASE_EXPR),
        arg_(arg),
        clauses_(clauses),
        dftresult_(dftresult) { 
    return_value_type_ = type_id; 
    if (arg_ != nullptr) {
      for (int i = 0; i < (int)(*clauses_).size(); i++) {
		(*clauses_)[i]->SetChild(0,
            new peloton::expression::ComparisonExpression(
            peloton::ExpressionType::COMPARE_EQUAL, arg_->Copy(),
            (*clauses_)[i]->GetModifiableChild(0)->Copy()));
      }
    }
  }

  virtual ~CaseExpression() {
    delete arg_;
    for (auto clause : *clauses_) {
		delete clause;
	}
    delete clauses_;
    delete dftresult_;
  }

  type::Value Evaluate(const AbstractTuple *tuple1, const AbstractTuple *tuple2,
                 executor::ExecutorContext *context) const override {
    for (auto clause : *clauses_) {
      try {
        return clause->Evaluate(tuple1, tuple2, context);
      } catch (int ExceptionCode) {
        if (ExceptionCode == 0) continue;
        throw Exception("Unknown Exception code in CaseExpression.");
      }
    }
    if (dftresult_ == nullptr) 
      return type::ValueFactory::GetNullValueByType(return_value_type_);
    return dftresult_->Evaluate(tuple1, tuple2, context);
  }

  AbstractExpression* Copy() const override { 
    auto *cp = new std::vector<AbstractExpression *>();
    for (auto clause : *clauses_) {
		cp->push_back(clause->Copy());
    }
    return new CaseExpression(return_value_type_, nullptr, cp, 
                              dftresult_->CopyUtil(dftresult_));
  }

  virtual void Accept(SqlNodeVisitor* v) { v->Visit(this); }

  size_t GetClauseSize() const { return (*clauses_).size(); }

  AbstractExpression *GetModifiableClause(int index) const {
    if (index < 0 || index >= (int)(*clauses_).size()) {
      return nullptr;
    }
    return (*clauses_)[index];
  }

 protected:
  AbstractExpression *arg_;
  std::vector<AbstractExpression *> *clauses_;
  AbstractExpression *dftresult_;
};

}  // End expression namespace
}  // End peloton namespace
