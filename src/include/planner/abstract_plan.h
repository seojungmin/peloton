//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// abstract_plan.h
//
// Identification: src/include/planner/abstract_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>
#include <memory>
#include <vector>

#include "catalog/schema.h"
#include "common/printable.h"
#include "planner/binding_context.h"
#include "type/serializeio.h"
#include "type/serializer.h"
#include "type/types.h"
#include "type/value.h"
#include "util/hash_util.h"

namespace peloton {

namespace executor {
class AbstractExecutor;
class LogicalTile;
}

namespace catalog {
class Schema;
}

namespace expression {
class AbstractExpression;
class Parameter;
}

namespace planner {

//===--------------------------------------------------------------------===//
// Abstract Plan
//===--------------------------------------------------------------------===//

class AbstractPlan : public Printable {
 public:
  AbstractPlan();

  virtual ~AbstractPlan();

  //===--------------------------------------------------------------------===//
  // Children + Parent Helpers
  //===--------------------------------------------------------------------===//

  void AddChild(std::unique_ptr<AbstractPlan> &&child);

  const std::vector<std::unique_ptr<AbstractPlan>> &GetChildren() const;

  const AbstractPlan *GetChild(uint32_t child_index) const;

  const AbstractPlan *GetParent() const;

  //===--------------------------------------------------------------------===//
  // Accessors
  //===--------------------------------------------------------------------===//

  // Each sub-class will have to implement this function to return their type
  // This is better than having to store redundant types in all the objects
  virtual PlanNodeType GetPlanNodeType() const = 0;

  // Setting values of the parameters in the prepare statement
  virtual void SetParameterValues(std::vector<type::Value> *values);

  //===--------------------------------------------------------------------===//
  // Utilities
  //===--------------------------------------------------------------------===//

  // Binding allows a plan to track the source of an attribute/column regardless
  // of its position in a tuple.  This binding allows a plan to know the types
  // of all the attributes it uses *before* execution. This is primarily used
  // by the codegen component since attributes are not positional.
  virtual void PerformBinding(BindingContext &binding_context) {
    for (auto &child : GetChildren()) {
      child->PerformBinding(binding_context);
    }
  }

  virtual void GetOutputColumns(std::vector<oid_t> &columns UNUSED_ATTRIBUTE)
      const { return; }

  // Get a string representation for debugging
  const std::string GetInfo() const;

  virtual std::unique_ptr<AbstractPlan> Copy() const = 0;

  // A plan will be sent to anther node via serialization
  // So serialization should be implemented by the derived classes

  //===--------------------------------------------------------------------===//
  // Serialization/Deserialization
  // Each sub-class will have to implement these functions
  // After the implementation for each sub-class, we should set these to pure
  // virtual
  //===--------------------------------------------------------------------===//
  virtual bool SerializeTo(SerializeOutput &output UNUSED_ATTRIBUTE) const {
    return false;
  }
  virtual bool DeserializeFrom(SerializeInput &input UNUSED_ATTRIBUTE) {
    return false;
  }
  virtual int SerializeSize() { return 0; }

  virtual hash_t Hash() const;

  virtual bool operator==(const AbstractPlan &rhs) const;
  virtual bool operator!=(const AbstractPlan &rhs) const {
    return !(*this == rhs);
  }

  virtual void ExtractParameters(std::vector<expression::Parameter> &parameters,
      std::unordered_map<const expression::AbstractExpression *, size_t>
          &index) const {
    for (auto &child : GetChildren()) {
      child->ExtractParameters(parameters, index);
    }
  }

 protected:
  // only used by its derived classes (when deserialization)
  AbstractPlan *Parent() { return parent_; }

 private:
  // A plan node can have multiple children
  std::vector<std::unique_ptr<AbstractPlan>> children_;

  AbstractPlan *parent_ = nullptr;

 private:
  DISALLOW_COPY_AND_MOVE(AbstractPlan);
};

class Equal {
 public:
  bool operator()(const std::shared_ptr<planner::AbstractPlan> &a,
                  const std::shared_ptr<planner::AbstractPlan> &b) const {
    return *a.get() == *b.get();
  }
};

class Hash {
 public:
  size_t operator()(const std::shared_ptr<planner::AbstractPlan> &plan) const {
    return static_cast<size_t>(plan->Hash());
  }
};

}  // namespace planner
}  // namespace peloton
