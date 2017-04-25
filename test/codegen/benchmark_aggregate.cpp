//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// benchmark_scan.cpp
//
// Identification: test/codegen/benchmark_scan.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <numeric>

#include "catalog/column.h"
#include "catalog/schema.h"
#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "executor/aggregate_executor.h"
#include "executor/seq_scan_executor.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/aggregate_plan.h"
#include "storage/table_factory.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

enum AggregateComplexity { SIMPLE, MODERATE, COMPLEX, WTF };

struct TestConfig {
  LayoutType layout = LayoutType::LAYOUT_TYPE_ROW;
  uint32_t column_count = 8;
  uint32_t tuples_per_tilegroup = 20000;
  uint32_t scale_factor = 20;
  AggregateComplexity aggregate_complexity = MODERATE;
  uint32_t num_aggregates = 1;
  uint32_t num_groups = 100000;
};

struct Stats {
  codegen::QueryCompiler::CompileStats compile_stats{0.0, 0.0, 0.0};
  codegen::QueryStatement::RuntimeStats runtime_stats{0.0, 0.0, 0.0};
  double num_samples = 0.0;
  int32_t tuple_result_size = -1;

  void Merge(codegen::QueryCompiler::CompileStats &o_compile_stats,
             codegen::QueryStatement::RuntimeStats &o_runtime_stats,
             int32_t o_tuple_result_size) {
    compile_stats.ir_gen_ms += o_compile_stats.ir_gen_ms;
    compile_stats.jit_ms += o_compile_stats.jit_ms;
    compile_stats.setup_ms += o_compile_stats.setup_ms;

    runtime_stats.init_ms += o_runtime_stats.init_ms;
    runtime_stats.plan_ms += o_runtime_stats.plan_ms;
    runtime_stats.tear_down_ms += o_runtime_stats.tear_down_ms;

    if (tuple_result_size < 0) {
      tuple_result_size = o_tuple_result_size;
    } else if (tuple_result_size != o_tuple_result_size) {
      throw Exception{
          "ERROR: tuple result size should not"
          " vary for the same test!"};
    }

    num_samples++;
  }

  void Finalize() {
    compile_stats.ir_gen_ms /= num_samples;
    compile_stats.jit_ms /= num_samples;
    compile_stats.setup_ms /= num_samples;

    runtime_stats.init_ms /= num_samples;
    runtime_stats.plan_ms /= num_samples;
    runtime_stats.tear_down_ms /= num_samples;
  }
};

class BenchmarkAggregateTest : public PelotonTest {
 public:
  ~BenchmarkAggregateTest() { DropDatabase(); }

  void CreateDatabase() {
    assert(database == nullptr);
    database = new storage::Database(0);
    catalog::Manager::GetInstance().AddDatabase(database);
  }

  void DropDatabase() {
    if (database != nullptr) {
      catalog::Manager::GetInstance().DropDatabaseWithOid(database->GetOid());
      database = nullptr;
    }
  }

  void CreateTable(TestConfig &config) {
    // First set the layout of the table before loading
    peloton_layout_mode = config.layout;

    const bool is_inlined = true;

    // Create schema first
    std::vector<catalog::Column> columns;

    for (oid_t col_itr = 0; col_itr < config.column_count; col_itr++) {
      auto column =
          catalog::Column(VALUE_TYPE_INTEGER, GetTypeSize(VALUE_TYPE_INTEGER),
                          "COL_" + std::to_string(col_itr), is_inlined);

      columns.push_back(column);
    }

    catalog::Schema *table_schema = new catalog::Schema(columns);
    std::string table_name("BENCHMARK_SCAN_TABLE");

    /////////////////////////////////////////////////////////
    // Create table.
    /////////////////////////////////////////////////////////

    bool own_schema = true;
    bool adapt_table = true;
    auto *table = storage::TableFactory::GetDataTable(
        GetDatabase().GetOid(), 0, table_schema, table_name,
        config.tuples_per_tilegroup, own_schema, adapt_table);

    /////////////////////////////////////////////////////////
    // Add table to database.
    /////////////////////////////////////////////////////////

    GetDatabase().AddTable(table);
  }

  void LoadTable(TestConfig &config) {
    const int tuple_count = config.scale_factor * config.tuples_per_tilegroup;

    auto table_schema = GetTable().GetSchema();

    /////////////////////////////////////////////////////////
    // Load in the data
    /////////////////////////////////////////////////////////

    // Insert tuples into tile_group.
    auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    const bool allocate = true;
    auto txn = txn_manager.BeginTransaction();
    std::unique_ptr<VarlenPool> pool(new VarlenPool(BACKEND_TYPE_MM));

    int rowid;
    for (rowid = 0; rowid < tuple_count; rowid++) {
      int populate_value = rowid;

      storage::Tuple tuple(table_schema, allocate);

      for (oid_t col_itr = 0; col_itr < config.column_count; col_itr++) {
        Value value;
        if (col_itr == 0) {
          value =
              ValueFactory::GetIntegerValue(populate_value % config.num_groups);
        } else {
          value = ValueFactory::GetIntegerValue(populate_value + col_itr);
        }
        tuple.SetValue(col_itr, value, pool.get());
      }

      ItemPointer tuple_slot_id = GetTable().InsertTuple(&tuple);
      assert(tuple_slot_id.block != INVALID_OID);
      assert(tuple_slot_id.offset != INVALID_OID);
      txn->RecordInsert(tuple_slot_id);
    }

    txn_manager.CommitTransaction();
  }

  void CreateAndLoadTable(TestConfig &config) {
    CreateTable(config);
    LoadTable(config);
  }

  storage::Database &GetDatabase() const { return *database; }

  storage::DataTable &GetTable() const {
    return *GetDatabase().GetTableWithOid(0);
  }

  expression::AbstractExpression *ConstructSimplePredicate(__attribute((unused))
                                                           TestConfig &config) {
    // For count(*) just use a TVE
    return new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, 0);
  }

  expression::AbstractExpression *ConstructModeratePredicate(
      TestConfig &config) {
    return ConstructSimplePredicate(config);
  }

  expression::AbstractExpression *ConstructComplexPredicate(
      __attribute((unused)) TestConfig &config) {
    return ConstructSimplePredicate(config);
  }

  std::vector<planner::AggregatePlan::AggTerm> ConstructAggregates(
      TestConfig &config) {
    std::vector<planner::AggregatePlan::AggTerm> agg_terms;  //= {
    //        {EXPRESSION_TYPE_AGGREGATE_COUNT_STAR, expr}};

    for (uint32_t i = 0; i < config.num_aggregates; i++) {
      auto *expr =
          new expression::TupleValueExpression(VALUE_TYPE_INTEGER, 0, i);
      planner::AggregatePlan::AggTerm term{EXPRESSION_TYPE_AGGREGATE_SUM,
                                           expr};
      agg_terms.push_back(term);
    }

    return agg_terms;
  }

  std::unique_ptr<const planner::ProjectInfo> ConstructProjection(
      TestConfig &config) {
    planner::ProjectInfo::DirectMapList direct_map_list = {{0, {0, 0}}};
    for (uint32_t i = 0; i < config.num_aggregates; i++) {
      direct_map_list.push_back(std::make_pair(i + 1, std::make_pair(1, i)));
    }
    return std::unique_ptr<planner::ProjectInfo>{new planner::ProjectInfo(
        planner::ProjectInfo::TargetList(), std::move(direct_map_list))};
  }

  std::shared_ptr<const catalog::Schema> ConstructOutputSchema(
      TestConfig &config) {
    std::vector<catalog::Column> cols = {{VALUE_TYPE_INTEGER, 4, "COL_A"}};
    for (uint32_t i = 0; i < config.num_aggregates; i++) {
      cols.emplace_back(VALUE_TYPE_BIGINT, 8, "COUNT_A");
    }
    return std::shared_ptr<const catalog::Schema>{new catalog::Schema(cols)};
  }

  std::unique_ptr<planner::AggregatePlan> ConstructAggregatePlan(
      TestConfig &config) {
    auto agg_terms = ConstructAggregates(config);
    auto proj_info = ConstructProjection(config);

    // The grouping column
    std::vector<oid_t> gb_cols = {0};

    // The output schema
    auto output_schema = ConstructOutputSchema(config);

    // 5) The aggregation node
    std::unique_ptr<planner::AggregatePlan> agg_plan{new planner::AggregatePlan(
        std::move(proj_info), nullptr, std::move(agg_terms), std::move(gb_cols),
        output_schema, AGGREGATE_TYPE_HASH)};

    // 6) The scan that feeds the aggregation
    std::unique_ptr<planner::AbstractPlan> scan_plan{
        new planner::SeqScanPlan(&GetTable(), nullptr, {0, 1, 2, 3, 4, 5})};

    agg_plan->AddChild(std::move(scan_plan));

    return agg_plan;
  }

  Stats RunCompiledExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      // Create fresh database, table and loaded data
      CreateDatabase();
      CreateAndLoadTable(config);

      auto scan = ConstructAggregatePlan(config);

      // Do binding
      planner::BindingContext context;
      scan->PerformBinding(context);

      // We collect the results of the query into an in-memory buffer
      std::vector<oid_t> col_ids;
      col_ids.resize(config.num_aggregates + 1);
      std::iota(col_ids.begin(), col_ids.end(), 0);
      BufferingConsumer buffer{col_ids, context};

      // COMPILE and execute
      codegen::QueryCompiler compiler;
      codegen::QueryCompiler::CompileStats compile_stats;
      auto query_statement = compiler.Compile(*scan, buffer, &compile_stats);

      codegen::QueryStatement::RuntimeStats runtime_stats;
      query_statement->Execute(*catalog::Catalog::GetInstance(),
                               reinterpret_cast<char *>(buffer.GetState()),
                               &runtime_stats);

      stats.Merge(compile_stats, runtime_stats,
                  buffer.GetOutputTuples().size());

      // Cleanup
      DropDatabase();
    }

    stats.Finalize();
    return stats;
  }

  Stats RunInterpretedExperiment(TestConfig &config, uint32_t num_runs = 5) {
    // Keep one copy of compile and runtime stats
    Stats stats;

    //
    for (uint32_t i = 0; i < num_runs; i++) {
      std::vector<std::vector<Value>> vals;

      codegen::QueryCompiler::CompileStats compile_stats;
      codegen::QueryStatement::RuntimeStats runtime_stats;

      // Create fresh database, table and loaded data
      CreateDatabase();
      CreateAndLoadTable(config);

      auto scan = ConstructAggregatePlan(config);

      auto &txn_manager = concurrency::TransactionManagerFactory::GetInstance();
      auto txn = peloton::concurrency::current_txn;
      // This happens for single statement queries in PG
      if (txn == nullptr) {
        txn = txn_manager.BeginTransaction();
      }

      executor::ExecutorContext ctx{txn};
      executor::AggregateExecutor agg_exec{scan.get(), &ctx};
      executor::SeqScanExecutor scan_exec{scan->GetChild(0), &ctx};

      agg_exec.AddChild(&scan_exec);

      agg_exec.Init();

      common::StopWatch sw{true};
      while (agg_exec.Execute()) {
        auto tile = agg_exec.GetOutput();
        for (oid_t tuple_id : *tile) {
          const expression::ContainerTuple<executor::LogicalTile> tuple{
              tile, tuple_id};
          std::vector<Value> tv;
          for (oid_t col_id = 0; col_id < config.num_aggregates + 1; col_id++) {
            tv.push_back(tuple.GetValue(col_id));
          }
          vals.push_back(std::move(tv));
        }
      }
      runtime_stats.plan_ms = sw.elapsedMillis(true);

      stats.Merge(compile_stats, runtime_stats, vals.size());

      // Cleanup
      DropDatabase();
    }

    // stats.Finalize();
    return stats;
  }

  void PrintName(std::string test_name) {
    std::cerr << "NAME:\n===============\n" << test_name << std::endl;
  }

  void PrintConfig(TestConfig &config) {
    fprintf(stderr, "CONFIGURATION:\n===============\n");
    fprintf(stderr,
            "Layout: %d, # Cols: %d, # Tuples/tilegroup: %d, Scale factor: %d, "
            "Aggregate complexity: %d, # Aggregates: %d\n",
            config.layout, config.column_count, config.tuples_per_tilegroup,
            config.scale_factor, config.aggregate_complexity,
            config.num_aggregates);
  }

  void PrintStats(Stats &stats) {
    auto &compile_stats = stats.compile_stats;
    auto &runtime_stats = stats.runtime_stats;
    auto &tuple_result_size = stats.tuple_result_size;
    fprintf(
        stderr,
        "Setup time: %.2f ms, IR Gen time: %.2f ms, Compile time: %.2f ms\n",
        compile_stats.setup_ms, compile_stats.ir_gen_ms, compile_stats.jit_ms);
    fprintf(stderr,
            "Initialization time: %.2f ms, execution time: %.2f ms, Tear down "
            "time: %.2f ms\n",
            runtime_stats.init_ms, runtime_stats.plan_ms,
            runtime_stats.tear_down_ms);
    fprintf(stderr, "Tuple result size: %d\n", tuple_result_size);
  }

 private:
  storage::Database *database = nullptr;
};

TEST_F(BenchmarkAggregateTest, PredicateComplexityTestWithCompilation) {
  // AggregateComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("AGGREGATE_COMPLEXITY: COMPILATION");
  // for (AggregateComplexity complexity : complexities) {
  for (const int num_aggs : {1, 2, 3, 4, 5}) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.num_aggregates = num_aggs;
    config.scale_factor = 50;

    auto stats = RunCompiledExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

TEST_F(BenchmarkAggregateTest, PredicateComplexityTestWithInterpretation) {
  // AggregateComplexity complexities[] = {SIMPLE, MODERATE, COMPLEX};

  PrintName("AGGREGATE_COMPLEXITY: INTERPRETATION");
  for (const int num_aggs : {1, 2, 3, 4, 5}) {
    TestConfig config;
    config.layout = LAYOUT_ROW;
    config.num_aggregates = num_aggs;
    config.scale_factor = 50;

    auto stats = RunInterpretedExperiment(config);
    PrintConfig(config);
    PrintStats(stats);
  }
}

}  // namespace test
}  // namespace peloton