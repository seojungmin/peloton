# Codegen Cache & Parameterization

## Overview

This feature provides performance enhancement of the query compilation in Peloton codegen, by caching compiled queries and reusing them in the next user requests. The constant and parameter value expressions are parameterized, and the cached queries with a "similar" plan can also get a benefit of caching.

## Scope

The query cache feature is based on plan/expression comparisons, including schema/table comparisons.  These objects provide hash functions and equality(and non-equality) operators, in order for the query cache to compare plans using a map.  The plans also provide a function to retrieve parameter values out of them.

In a brief summary, it contains modifications on the following modules:

[Comparisons]
  - catalog: Hash/equality checks on `Schema`
  - storage: Hash/equality checks on `DataTable`
  - expression: Hash/equality checks on expressions such as `AbstractExpression`, `CaseExpression`, `ConstantValueExpression`, `ParameterValueExpression` and `TupleValueExpression`
  - planner: Hash/equality checks and parameter retrieval on plans such as `AggregatePlan`, `DeletePlan`, `HashJoinPlan`, `HashPlan`, `InsertPlan`, `OrderByPlan`, `ProjectInfo`, `ProjectionPlan`, `SeqScanPlan` and `UpdatePlan`

[Parameter Retrieval]
  - codegen: Parameter value/information retrieval 

[Query Cache/Execution]
  - execution: Query cache checks before executing a query
  - codegen: Query cache, parameter value cache, translator additions and changes including operators/expressions

## Glossary

  - Query cache: cache of the query objects with compiled object for codegen execution, which is different from physical plan cache or cache

## Architectural Design

The entire flow of a query execution stays the same as before, but only bypass the compilation stage only when there has been a similar query executed.  A similar query here is defined as a query with different constant values or different parameter values. This, in a high level, is done in `executor::PlanExecutor` before a query execution. 

`QueryCache` keeps all the compiled queries in a hash table and this is global in Peloton. A search for an equal query is executed by `Hash()` and `operation==` that are provided by the plan that is going to be compared. Once the comparison succeds, `PlanExecutor` obtains the previously compiled query, a `Query` object, from the cache and the query object is executed through its `Execute()` function.  The `Query` contains a set of compiled functions that can be executed inside LLVM.

`QueryParameters` 

`ParameterCache` 


 +------------+    +-------+
 | QueryCache | -- | Query |
 +------------+    +-------+

>Explain the input and output of the component, describe interactions and breakdown the smaller components if any. Include diagrams if appropriate.

## Design Rationale

The goal of this design is to provide a first usable query cache that removes the codegen compilation overhead of each query.  It does not remove the compilation overhead of a query comes in for the first time, but  

It does not change a plan in order to optimize the cache performance, i.e. 2+2 is different from 4 in the perspectivve of QueryCache. In addition, it does not re-order the expressions to find a match in the cache. We think that these are a role of the optimizer.

>Explain the goals of this design and how the design achieves these goals. Present alternatives considered and document why they are not chosen.

## Testing Plan

[Comparisons]

[Cache]

[Query Execution]

## Trade-offs and Potential Problems

>Write down any conscious trade-off you made that can be problematic in the future, or any problems discovered during the design process that remain unaddressed (technical debts).

## Future Work

We could pre-populate the cache with some essential queries when a table is generated.  One obvious query is an Insert.

>Write down future work to fix known problems or otherwise improve the component.

