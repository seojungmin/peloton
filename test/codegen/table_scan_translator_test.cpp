//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// table_scan_translator_test.cpp
//
// Identification: test/codegen/table_scan_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <immintrin.h>

#include "catalog/catalog.h"
#include "codegen/data_table_proxy.h"
#include "codegen/if.h"
#include "codegen/loop.h"
#include "codegen/query_compiler.h"
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "expression/comparison_expression.h"
#include "expression/conjunction_expression.h"
#include "expression/constant_value_expression.h"
#include "expression/operator_expression.h"
#include "expression/tuple_value_expression.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"
#include "executor/testing_executor_util.h"

namespace peloton {
namespace test {

//===----------------------------------------------------------------------===//
// This class contains code to test code generation and compilation of table
// scan query plans. All the tests use a single table created and loaded during
// SetUp().  The schema of the table is as follows:
//
// +---------+---------+---------+-------------+
// | A (int) | B (int) | C (int) | D (varchar) |
// +---------+---------+---------+-------------+
//
// The database and tables are created in CreateDatabase() and
// CreateTestTables(), respectively.
//
// By default, the table is loaded with 64 rows of random values.
//===----------------------------------------------------------------------===//

class TableScanTranslatorTest : public PelotonTest {
 public:
  TableScanTranslatorTest()
      : test_db(new storage::Database(CodegenTestUtils::kTestDbOid)),
        num_rows_to_insert(64) {
    // Create test table
    auto* test_table = CreateTestTable();

    // Add table to test DB
    test_db->AddTable(test_table, false);

    // Add DB to catalog
    catalog::Catalog::GetInstance()->AddDatabase(test_db.get());

    // Load test table
    LoadTestTable(num_rows_to_insert);
  }

  storage::DataTable* CreateTestTable() {
    const int tuples_per_tilegroup = 32;
    return TestingExecutorUtil::CreateTable(tuples_per_tilegroup, false,
                                            CodegenTestUtils::kTestTable1Oid);
  }

  void LoadTestTable(uint32_t num_rows) {
    auto& test_table = GetTestTable();

    auto& txn_manager = concurrency::TransactionManagerFactory::GetInstance();
    auto* txn = txn_manager.BeginTransaction();

    TestingExecutorUtil::PopulateTable(&test_table, num_rows, false, false,
                                       false, txn);
    txn_manager.CommitTransaction(txn);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  storage::DataTable& GetTestTable() {
    return *test_db->GetTableWithOid(CodegenTestUtils::kTestTable1Oid);
  }

 private:
  std::unique_ptr<storage::Database> test_db;
  uint32_t num_rows_to_insert = 64;
};

TEST_F(TableScanTranslatorTest, AllColumnsScan) {
  //
  // SELECT a, b, c FROM table;
  //

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), nullptr, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // Printing consumer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check that we got all the results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable());
}

TEST_F(TableScanTranslatorTest, SimplePredicate) {
  //
  // SELECT a, b, c FROM table where a >= 20;
  //

  // Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_gt_20, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable() - 2);
}

TEST_F(TableScanTranslatorTest, PredicateOnNonOutputColumn) {
  //
  // SELECT b FROM table where a >= 40;
  //

  // 1) Setup the predicate
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_40_exp = CodegenTestUtils::ConstIntExpression(40);
  auto* a_gt_40 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_40_exp);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_gt_40, {0, 1}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0}, context};

  // 4) COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable() - 4);
}

TEST_F(TableScanTranslatorTest, ScanWithConjunctionPredicate) {
  //
  // SELECT a, b, c FROM table where a >= 20 and b = 21;
  //

  // 1) Construct the components of the predicate

  // a >= 20
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_20_exp = CodegenTestUtils::ConstIntExpression(20);
  auto* a_gt_20 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_GREATERTHANOREQUALTO, a_col_exp, const_20_exp);

  // b = 21
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_21_exp = CodegenTestUtils::ConstIntExpression(21);
  auto* b_eq_21 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, const_21_exp);

  // a >= 20 AND b = 21
  auto* conj_eq = new expression::ConjunctionExpression(
      ExpressionType::CONJUNCTION_AND, b_eq_21, a_gt_20);

  // 2) Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), conj_eq, {0, 1, 2}};

  // 3) Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // 4) COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
                                type::ValueFactory::GetIntegerValue(20)));
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(1).CompareEquals(
                                type::ValueFactory::GetIntegerValue(21)));
}

TEST_F(TableScanTranslatorTest, ScanWithAddPredicate) {
  //
  // SELECT a, b FROM table where b = a + 1;
  //

  // Construct the components of the predicate

  // a + 1
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_1_exp = CodegenTestUtils::ConstIntExpression(1);
  auto* a_plus_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, a_col_exp,
      const_1_exp);

  // b = a + 1
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_eq_a_plus_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_col_exp, a_plus_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), b_eq_a_plus_1, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable());
}

TEST_F(TableScanTranslatorTest, ScanWithAddColumnsPredicate) {
  //
  // SELECT a, b FROM table where b = a + b;
  //

  // Construct the components of the predicate

  // a + b
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* b_rhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* a_plus_b = new expression::OperatorExpression(
      ExpressionType::OPERATOR_PLUS, type::Type::TypeId::INTEGER, a_col_exp,
      b_rhs_col_exp);

  // b = a + b
  auto* b_lhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_eq_a_plus_b = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_lhs_col_exp, a_plus_b);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), b_eq_a_plus_b, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 1);
}

TEST_F(TableScanTranslatorTest, ScanWithSubtractPredicate) {
  //
  // SELECT a, b FROM table where a = b - 1;
  //

  // Construct the components of the predicate

  // b - 1
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_1_exp = CodegenTestUtils::ConstIntExpression(1);
  auto* b_minus_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::Type::TypeId::INTEGER, b_col_exp,
      const_1_exp);

  // a = b - 1
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_b_minus_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, b_minus_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_eq_b_minus_1, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable());
}

TEST_F(TableScanTranslatorTest, ScanWithSubtractColumnsPredicate) {
  //
  // SELECT a, b FROM table where b = b - a;
  //

  // Construct the components of the predicate

  // b - a
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* b_rhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_minus_a = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MINUS, type::Type::TypeId::INTEGER,
      b_rhs_col_exp, a_col_exp);

  // b = b - a
  auto* b_lhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* b_eq_b_minus_a = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, b_lhs_col_exp, b_minus_a);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), b_eq_b_minus_a, {0, 1}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 1);
}

TEST_F(TableScanTranslatorTest, ScanWithDividePredicate) {
  //
  //   SELECT a, b, c FROM table where a = a / 1;
  //

  // Construct the components of the predicate

  // a / 1
  auto* a_rhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* const_1_exp = CodegenTestUtils::ConstIntExpression(1);
  auto* a_div_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_DIVIDE, type::Type::TypeId::DECIMAL,
      a_rhs_col_exp, const_1_exp);

  // a = a / 1
  auto* a_lhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_a_div_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_lhs_col_exp, a_div_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_eq_a_div_1, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable());
}

TEST_F(TableScanTranslatorTest, ScanWithMultiplyPredicate) {
  //
  // SELECT a, b, c FROM table where a = a * b;
  //

  // Construct the components of the predicate

  // a * b
  auto* a_rhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* a_mul_b = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MULTIPLY, type::Type::TypeId::BIGINT,
      a_rhs_col_exp, b_col_exp);

  // a = a * b
  auto* a_lhs_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_a_mul_b = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_lhs_col_exp, a_mul_b);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_eq_a_mul_b, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), 1);
}

TEST_F(TableScanTranslatorTest, ScanWithModuloPredicate) {
  //
  // SELECT a, b, c FROM table where a = b % 1;
  //

  // Construct the components of the predicate

  // b % 1
  auto* b_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 1);
  auto* const_1_exp = CodegenTestUtils::ConstIntExpression(1);
  auto* b_mod_1 = new expression::OperatorExpression(
      ExpressionType::OPERATOR_MOD, type::Type::TypeId::DECIMAL, b_col_exp,
      const_1_exp);

  // a = b % 1
  auto* a_col_exp =
      new expression::TupleValueExpression(type::Type::TypeId::INTEGER, 0, 0);
  auto* a_eq_b_mod_1 = new expression::ComparisonExpression(
      ExpressionType::COMPARE_EQUAL, a_col_exp, b_mod_1);

  // Setup the scan plan node
  planner::SeqScanPlan scan{&GetTestTable(), a_eq_b_mod_1, {0, 1, 2}};

  // Do binding
  planner::BindingContext context;
  scan.PerformBinding(context);

  // We collect the results of the query into an in-memory buffer
  BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  codegen::QueryCompiler compiler;
  auto query_statement = compiler.Compile(scan, buffer);
  query_statement->Execute(*catalog::Catalog::GetInstance(),
                           reinterpret_cast<char*>(buffer.GetState()));

  // Check output results
  const auto& results = buffer.GetOutputTuples();
  ASSERT_EQ(results.size(), 1);
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(0).CompareEquals(
                                type::ValueFactory::GetIntegerValue(0)));
  EXPECT_EQ(type::CMP_TRUE, results[0].GetValue(1).CompareEquals(
                                type::ValueFactory::GetIntegerValue(1)));
}

}  // namespace test
}  // namespace peloton