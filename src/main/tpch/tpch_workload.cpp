//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// tpch_workload.cpp
//
// Identification: src/main/tpch/tpch_workload.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "benchmark/tpch/tpch_workload.h"

#include "concurrency/transaction_manager_factory.h"

namespace peloton {
namespace benchmark {
namespace tpch {

TPCHBenchmark::TPCHBenchmark(const Configuration &config, TPCHDatabase &db)
    : config_(config), db_(db) {
  query_configs_ = {
      {"Q1", QueryId::Q1, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q2", QueryId::Q2, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q3", QueryId::Q3, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q4", QueryId::Q4, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q5", QueryId::Q5, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q6", QueryId::Q6, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q7", QueryId::Q7, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q8", QueryId::Q8, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q9", QueryId::Q9, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q10", QueryId::Q10, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q11", QueryId::Q11, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q12", QueryId::Q12, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q13", QueryId::Q13, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q14", QueryId::Q14, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q15", QueryId::Q15, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q16", QueryId::Q16, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q17", QueryId::Q17, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q18", QueryId::Q18, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q19", QueryId::Q19, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q20", QueryId::Q20, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q21", QueryId::Q21, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
      {"Q22", QueryId::Q22, {TableId::Lineitem}, [&](){return ConstructQ1Plan();}},
  };
}

void TPCHBenchmark::RunBenchmark() {
  for (uint32_t i = 0; i < 22; i++) {
    const auto &query_config = query_configs_[i];
    if (config_.ShouldRunQuery(query_config.query_id)) {
      RunQuery(query_config);
    }
  }
}

void TPCHBenchmark::RunQuery(const TPCHBenchmark::QueryConfig &query_config) {
  LOG_INFO("Running TPCH %s", query_config.query_name.c_str());

  // Load all the necessary tables
  for (auto tid : query_config.required_tables) {
    db_.LoadTable(tid);
  }

  // Construct the plan for Q1
  std::unique_ptr<planner::AbstractPlan> plan = query_config.PlanConstructor();

  // Do attribute binding
  planner::BindingContext binding_context;
  plan->PerformBinding(binding_context);

  // The consumer
  CountingConsumer counter;

  // Compile
  codegen::QueryCompiler::CompileStats compile_stats;
  codegen::QueryCompiler compiler;
  auto compiled_query = compiler.Compile(*plan, counter, &compile_stats);

  // Execute the query
  auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
  auto *txn = txn_manager.BeginTransaction();

  codegen::QueryStatement::RuntimeStats runtime_stats;
  compiled_query->Execute(*txn, nullptr, &runtime_stats);

  txn_manager.CommitTransaction(txn);

  LOG_INFO("%s: ==============================================",
           query_config.query_name.c_str());
  LOG_INFO("Setup: %.2lf, IR Gen: %.2lf, Compile: %.2lf",
           compile_stats.setup_ms, compile_stats.ir_gen_ms,
           compile_stats.jit_ms);
  LOG_INFO("Init: %.2lf ms, Plan: %.2lf ms, TearDown: %.2lf ms",
           runtime_stats.init_ms, runtime_stats.plan_ms,
           runtime_stats.tear_down_ms);
}

//===----------------------------------------------------------------------===//
// COUNTING CONSUMER
//===----------------------------------------------------------------------===//

void CountingConsumer::Prepare(codegen::CompilationContext &ctx) {
  auto &codegen = ctx.GetCodeGen();
  auto &runtime_state = ctx.GetRuntimeState();
  counter_state_id_ =
      runtime_state.RegisterState("consumerState", codegen.Int64Type());
}

void CountingConsumer::InitializeState(codegen::CompilationContext &context) {
  auto &codegen = context.GetCodeGen();
  auto *state_ptr = GetCounterState(codegen, context.GetRuntimeState());
  codegen->CreateStore(codegen.Const64(0), state_ptr);
}

// Increment the counter
void CountingConsumer::ConsumeResult(codegen::ConsumerContext &context,
                                     codegen::RowBatch::Row &) const {
  auto &codegen = context.GetCodeGen();

  auto *counter_ptr = GetCounterState(codegen, context.GetRuntimeState());
  auto *new_count =
      codegen->CreateAdd(codegen->CreateLoad(counter_ptr), codegen.Const64(1));
  codegen->CreateStore(new_count, counter_ptr);
}

llvm::Value *CountingConsumer::GetCounterState(
    codegen::CodeGen &codegen, codegen::RuntimeState &runtime_state) const {
  return runtime_state.GetStatePtr(codegen, counter_state_id_);
}

}  // namespace tpch
}  // namespace benchmark
}  // namespace peloton