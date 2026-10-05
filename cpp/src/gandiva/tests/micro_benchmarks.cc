// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <stdlib.h>

#include <random>
#include <string>
#include <vector>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/testing/gtest_util.h"
#include "arrow/type_fwd.h"
#include "benchmark/benchmark.h"
#include "gandiva/decimal_type_util.h"
#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tests/timed_evaluate.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

static void TimedTestAdd3Impl(benchmark::State& state, bool with_validity_buffer) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_sum = field("add", int64());

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      int64());
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, int64());

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state,
      with_validity_buffer);
  ASSERT_OK(status);
}

static void TimedTestAdd3(benchmark::State& state) { TimedTestAdd3Impl(state, false); }

// Plain arithmetic over inputs that carry a validity buffer, as in Dremio.
static void TimedTestAdd3ValidityBuffer(benchmark::State& state) {
  TimedTestAdd3Impl(state, true);
}

static void TimedTestBigNestedImpl(benchmark::State& state, bool with_validity_buffer) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", int32());

  // build expression.
  // if (a < 10)
  //   10
  // else if (a < 20)
  //   20
  // ..
  // ..
  // else if (a < 190)
  //   190
  // else
  //   200
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto top_node = TreeExprBuilder::MakeLiteral(200);
  for (int thresh = 190; thresh > 0; thresh -= 10) {
    auto literal = TreeExprBuilder::MakeLiteral(thresh);
    auto condition =
        TreeExprBuilder::MakeFunction("less_than", {node_a, literal}, boolean());
    auto if_node = TreeExprBuilder::MakeIf(condition, literal, top_node, int32());
    top_node = if_node;
  }
  auto expr = TreeExprBuilder::MakeExpression(top_node, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  BoundedInt32DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int32Type, int32_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state,
      with_validity_buffer);
  ASSERT_TRUE(status.ok());
}

static void TimedTestBigNested(benchmark::State& state) {
  TimedTestBigNestedImpl(state, false);
}

// Inputs that carry a validity buffer, as they always do in Dremio.
static void TimedTestBigNestedValidityBuffer(benchmark::State& state) {
  TimedTestBigNestedImpl(state, true);
}

static void TimedTestIfElseFieldArms(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // if (f0 < f1) f1 else if (f0 < f2) f2 else f0, with nullable arms.
  auto node0 = TreeExprBuilder::MakeField(field0);
  auto node1 = TreeExprBuilder::MakeField(field1);
  auto node2 = TreeExprBuilder::MakeField(field2);
  auto inner = TreeExprBuilder::MakeIf(
      TreeExprBuilder::MakeFunction("less_than", {node0, node2}, boolean()), node2, node0,
      int64());
  auto outer = TreeExprBuilder::MakeIf(
      TreeExprBuilder::MakeFunction("less_than", {node0, node1}, boolean()), node1, inner,
      int64());
  auto expr = TreeExprBuilder::MakeExpression(outer, field("res", int64()));

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state,
      /*with_validity_buffer=*/true);
  ASSERT_OK(status);
}

namespace {

const std::vector<std::string>& MessageWords() {
  static const std::vector<std::string> kWords = {
      "failed",  "to",       "read",    "table",   "column",  "error",  "while",
      "query",   "the",      "of",      "file",    "access",  "denied", "cannot",
      "invoke",  "because",  "value",   "is",      "null",    "timeout", "remote",
      "fragment", "execution", "unable", "connect", "server",  "plan",   "invalid",
      "Schema",  "Dataset",  "Unknown", "type",    "request", "object", "not",
      "found",   "user",     "limit",   "exceeded", "memory", "node",   "Could"};
  return kWords;
}

// Makes a phrase of num_words random words.
std::string MakePhrase(std::default_random_engine& rng, int num_words) {
  const auto& words = MessageWords();
  std::string phrase;
  for (int i = 0; i < num_words; ++i) {
    if (i > 0) phrase += ' ';
    phrase += words[rng() % words.size()];
  }
  return phrase;
}

// Error-message-like strings of ~30 words; some contain one of the given phrases.
class MessageDataGenerator : public DataGenerator<std::string> {
 public:
  explicit MessageDataGenerator(const std::vector<std::string>& phrases)
      : phrases_(phrases) {}

  std::string GenerateData() override {
    std::string message = MakePhrase(rng_, 30);
    if (rng_() % 10 == 0) {
      message += " " + phrases_[rng_() % phrases_.size()];
    }
    return message;
  }

 private:
  const std::vector<std::string>& phrases_;
  std::default_random_engine rng_{7};
};

}  // namespace

// CASE WHEN POSITION(phrase_0 IN a) > 0 THEN 'phrase_0' WHEN ... ELSE 'other' END, the
// shape of queries that classify messages by the substrings they contain.
static void TimedTestCasePosition(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  std::default_random_engine rng(42);
  std::vector<std::string> phrases;
  for (int i = 0; i < 100; ++i) {
    phrases.push_back(MakePhrase(rng, 3));
  }

  auto node_a = TreeExprBuilder::MakeField(field_a);
  NodePtr case_node = TreeExprBuilder::MakeStringLiteral("other");
  for (auto it = phrases.rbegin(); it != phrases.rend(); ++it) {
    auto position = TreeExprBuilder::MakeFunction(
        "locate", {TreeExprBuilder::MakeStringLiteral(*it), node_a}, int32());
    auto condition = TreeExprBuilder::MakeFunction(
        "greater_than", {position, TreeExprBuilder::MakeLiteral(0)}, boolean());
    case_node = TreeExprBuilder::MakeIf(
        condition, TreeExprBuilder::MakeStringLiteral(*it), case_node, utf8());
  }
  auto expr = TreeExprBuilder::MakeExpression(case_node, field("res", utf8()));

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  MessageDataGenerator data_generator(phrases);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 64 * THOUSAND, 16 * THOUSAND, state,
      /*with_validity_buffer=*/true);
  ASSERT_OK(status);
}

// LIKE '%...%' over error-message-like strings.
static void TimedTestFilterLikeMessages(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  auto like = TreeExprBuilder::MakeFunction(
      "like",
      {TreeExprBuilder::MakeField(field_a),
       TreeExprBuilder::MakeStringLiteral("%access denied%")},
      boolean());
  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, TreeExprBuilder::MakeCondition(like),
                         TestConfiguration(), &filter));

  static const std::vector<std::string> kPhrases = {"access denied", "table not found"};
  MessageDataGenerator data_generator(kPhrases);
  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state,
      /*with_validity_buffer=*/true);
  ASSERT_OK(status);
}

// CASE WHEN a = 'v0' THEN 'g0' WHEN a = 'v1' ... over 20 short strings with a validity
// buffer, e.g. mapping a region code to a geography.
static void TimedTestCaseStringEquals(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  static const std::vector<std::string> kValues = {
      "us-east-1",      "us-east-2",      "us-west-1",      "us-west-2",
      "ca-central-1",   "sa-east-1",      "eu-west-1",      "eu-west-2",
      "eu-west-3",      "eu-central-1",   "eu-north-1",     "eu-south-1",
      "me-south-1",     "af-south-1",     "ap-south-1",     "ap-east-1",
      "ap-northeast-1", "ap-northeast-2", "ap-southeast-1", "ap-southeast-2"};
  auto node_a = TreeExprBuilder::MakeField(field_a);
  NodePtr case_node = TreeExprBuilder::MakeStringLiteral("Unknown");
  for (size_t i = kValues.size(); i-- > 0;) {
    auto condition = TreeExprBuilder::MakeFunction(
        "equal", {node_a, TreeExprBuilder::MakeStringLiteral(kValues[i])}, boolean());
    case_node = TreeExprBuilder::MakeIf(
        condition, TreeExprBuilder::MakeStringLiteral("geo" + std::to_string(i % 6)),
        case_node, utf8());
  }
  auto expr = TreeExprBuilder::MakeExpression(case_node, field("res", utf8()));

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  // Draw inputs from the same values, plus some that match none of them.
  class RegionGenerator : public DataGenerator<std::string> {
   public:
    std::string GenerateData() override {
      auto i = rng_() % (kValues.size() + 4);
      return i < kValues.size() ? kValues[i] : "xx-other-" + std::to_string(i);
    }

   private:
    std::default_random_engine rng_{11};
  };
  RegionGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state,
      /*with_validity_buffer=*/true);
  ASSERT_OK(status);
}

static void TimedTestExtractYear(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", arrow::date64());
  auto schema = arrow::schema({field0});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int64());

  // Build expression
  auto expr = TreeExprBuilder::MakeExpression("extractYear", {field0}, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::Date64Type, int64_t>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterAdd2(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // Build expression
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field0)},
      int64());
  auto less_than = TreeExprBuilder::MakeFunction(
      "less_than", {sum, TreeExprBuilder::MakeField(field2)}, boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  Int64DataGenerator data_generator;
  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestFilterProjectAdd2(benchmark::State& state) {
  // schema for input fields
  auto field0 = field("f0", int64());
  auto field1 = field("f1", int64());
  auto field2 = field("f2", int64());
  auto schema = arrow::schema({field0, field1, field2});
  auto pool_ = arrow::default_memory_pool();

  // filter : f0 < f2 (selects roughly half the rows)
  auto less_than = TreeExprBuilder::MakeFunction(
      "less_than",
      {TreeExprBuilder::MakeField(field0), TreeExprBuilder::MakeField(field2)},
      boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  // project : f0 + f1 over the selected rows
  auto sum_expr = TreeExprBuilder::MakeExpression(
      TreeExprBuilder::MakeFunction(
          "add", {TreeExprBuilder::MakeField(field0), TreeExprBuilder::MakeField(field1)},
          int64()),
      field("add", int64()));

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {sum_expr}, SelectionVector::MODE_UINT16,
                            TestConfiguration(), &projector));

  Int64DataGenerator data_generator;
  FilterProjectEvaluator evaluator(filter, projector);

  Status status = TimedEvaluate<arrow::Int64Type, int64_t>(
      schema, evaluator, data_generator, pool_, MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestLower(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", utf8());

  // lower(a) with a string output
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto lower = TreeExprBuilder::MakeFunction("lower", {node_a}, utf8());
  auto expr = TreeExprBuilder::MakeExpression(lower, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(64);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void TimedTestFilterLike(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // build expression.
  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto pattern_node = TreeExprBuilder::MakeStringLiteral("%yellow%");
  auto like_yellow =
      TreeExprBuilder::MakeFunction("like", {node_a, pattern_node}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(like_yellow);

  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));

  FastUtf8DataGenerator data_generator(32);
  FilterEvaluator evaluator(filter);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestCastFloatFromString(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool = arrow::default_memory_pool();

  auto field_result = field("res", arrow::float64());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto fn = TreeExprBuilder::MakeFunction("castFLOAT8", {node_a}, arrow::float64());
  auto expr = TreeExprBuilder::MakeExpression(fn, field_result);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  Utf8FloatDataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestCastIntFromString(benchmark::State& state) {
  auto field_a = field("a", utf8());
  auto schema = arrow::schema({field_a});
  auto pool = arrow::default_memory_pool();

  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto fn = TreeExprBuilder::MakeFunction("castINT", {node_a}, int32());
  auto expr = TreeExprBuilder::MakeExpression(fn, field_result);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  Utf8IntDataGenerator data_generator;
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestAllocs(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("a", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();

  // output field
  auto field_res = field("res", int32());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto length = TreeExprBuilder::MakeFunction("octet_length", {upper}, int32());
  auto expr = TreeExprBuilder::MakeExpression(length, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(64);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state);
  ASSERT_TRUE(status.ok());
}

static void TimedTestOutputStringAllocs(benchmark::State& state) {
  // schema for input fields
  auto field_a = field("abcdefghijklmnopqrstuvwxyz", arrow::utf8());
  auto schema = arrow::schema({field_a});
  auto pool_ = arrow::default_memory_pool();
  // output field
  auto field_res = field("res", utf8());

  // Build expression
  auto node_a = TreeExprBuilder::MakeField(field_a);
  auto upper = TreeExprBuilder::MakeFunction("upper", {node_a}, utf8());
  auto length = TreeExprBuilder::MakeFunction("octet_length", {upper}, int32());
  auto expr = TreeExprBuilder::MakeExpression(upper, field_res);

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(64);
  ProjectEvaluator evaluator(projector);

  ASSERT_OK((TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 1 * MILLION, 16 * THOUSAND, state)));
}
// following two tests are for benchmark optimization of
// in expr. will be used in follow-up PRs to optimize in expr.

static void TimedTestMultiOrImpl(benchmark::State& state, bool with_validity_buffer) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // booleanOr(a = string1, a = string2, ..)
  auto node_a = TreeExprBuilder::MakeField(fielda);

  NodeVector boolean_functions;
  FastUtf8DataGenerator data_generator1(250);
  for (int thresh = 1; thresh <= 32; thresh++) {
    auto literal = TreeExprBuilder::MakeStringLiteral(data_generator1.GenerateData());
    auto condition = TreeExprBuilder::MakeFunction("equal", {node_a, literal}, boolean());
    boolean_functions.push_back(condition);
  }

  auto boolean_or = TreeExprBuilder::MakeOr(boolean_functions);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);
  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 100 * THOUSAND, 16 * THOUSAND, state,
      with_validity_buffer);
  ASSERT_OK(status);
}

static void TimedTestMultiOr(benchmark::State& state) {
  TimedTestMultiOrImpl(state, false);
}

// Inputs that carry a validity buffer, as they always do in Dremio.
static void TimedTestMultiOrValidityBuffer(benchmark::State& state) {
  TimedTestMultiOrImpl(state, true);
}

static void TimedTestInExpr(benchmark::State& state) {
  // schema for input fields
  auto fielda = field("a", utf8());
  auto schema = arrow::schema({fielda});
  auto pool_ = arrow::default_memory_pool();

  // output fields
  auto field_result = field("res", boolean());

  // build expression.
  // a in (string1, string2, ..)
  auto node_a = TreeExprBuilder::MakeField(fielda);

  std::unordered_set<std::string> values;
  FastUtf8DataGenerator data_generator1(250);
  for (int i = 1; i <= 32; i++) {
    values.insert(data_generator1.GenerateData());
  }
  auto boolean_or = TreeExprBuilder::MakeInExpressionString(node_a, values);
  auto expr = TreeExprBuilder::MakeExpression(boolean_or, field_result);

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {expr}, TestConfiguration(), &projector));

  FastUtf8DataGenerator data_generator(250);
  ProjectEvaluator evaluator(projector);

  Status status = TimedEvaluate<arrow::StringType, std::string>(
      schema, evaluator, data_generator, pool_, 100 * THOUSAND, 16 * THOUSAND, state);

  ASSERT_OK(status);
}

static void DoDecimalAdd3(benchmark::State& state, int32_t precision, int32_t scale,
                          bool large = false) {
  // schema for input fields
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field0 = field("f0", decimal_type);
  auto field1 = field("f1", decimal_type);
  auto field2 = field("f2", decimal_type);
  auto schema = arrow::schema({field0, field1, field2});

  Decimal128TypePtr add2_type;
  auto status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                               {decimal_type, decimal_type}, &add2_type);

  Decimal128TypePtr output_type;
  status = DecimalTypeUtil::GetResultType(DecimalTypeUtil::kOpAdd,
                                          {add2_type, decimal_type}, &output_type);

  // output field
  auto field_sum = field("add", output_type);

  // Build expression
  auto part_sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field1), TreeExprBuilder::MakeField(field2)},
      add2_type);
  auto sum = TreeExprBuilder::MakeFunction(
      "add", {TreeExprBuilder::MakeField(field0), part_sum}, output_type);

  auto sum_expr = TreeExprBuilder::MakeExpression(sum, field_sum);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {sum_expr}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  Decimal128DataGenerator data_generator(large);
  ProjectEvaluator evaluator(projector);

  status = TimedEvaluate<arrow::Decimal128Type, arrow::Decimal128>(
      schema, evaluator, data_generator, arrow::default_memory_pool(), 1 * MILLION,
      16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void DoDecimalAdd2(benchmark::State& state, int32_t precision, int32_t scale,
                          bool large = false) {
  // schema for input fields
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  auto field0 = field("f0", decimal_type);
  auto field1 = field("f1", decimal_type);
  auto schema = arrow::schema({field0, field1});

  Decimal128TypePtr output_type;
  auto status = DecimalTypeUtil::GetResultType(
      DecimalTypeUtil::kOpAdd, {decimal_type, decimal_type}, &output_type);

  // output field
  auto field_sum = field("add", output_type);

  // Build expression
  auto sum = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);

  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {sum}, TestConfiguration(), &projector);
  EXPECT_TRUE(status.ok());

  Decimal128DataGenerator data_generator(large);
  ProjectEvaluator evaluator(projector);

  status = TimedEvaluate<arrow::Decimal128Type, arrow::Decimal128>(
      schema, evaluator, data_generator, arrow::default_memory_pool(), 1 * MILLION,
      16 * THOUSAND, state);
  ASSERT_OK(status);
}

static void TimedTestExprCompilation(benchmark::State& state) {
  int64_t iteration = 0;
  for (auto _ : state) {
    // schema for input fields
    auto field0 = field("f0", int64());
    auto field1 = field("f1", int64());
    auto literal = TreeExprBuilder::MakeLiteral(iteration);
    auto schema = arrow::schema({field0, field1});

    // output field
    auto field_add = field("c1", int64());
    auto field_less_than = field("c2", boolean());

    // Build expression
    auto add_func = TreeExprBuilder::MakeFunction(
        "add", {TreeExprBuilder::MakeField(field0), literal}, int64());
    auto less_than_func = TreeExprBuilder::MakeFunction(
        "less_than", {TreeExprBuilder::MakeField(field1), literal}, boolean());

    auto expr_0 = TreeExprBuilder::MakeExpression(add_func, field_add);
    auto expr_1 = TreeExprBuilder::MakeExpression(less_than_func, field_less_than);

    std::shared_ptr<Projector> projector;
    ASSERT_OK(Projector::Make(schema, {expr_0, expr_1}, TestConfiguration(), &projector));

    ++iteration;
  }
}

static void DecimalAdd2Fast(benchmark::State& state) {
  // use lesser precision to test the fast-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision - 6, 18);
}

static void DecimalAdd2LeadingZeroes(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 6);
}

static void DecimalAdd2LeadingZeroesWithDiv(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 18);
}

static void DecimalAdd2Large(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd2(state, DecimalTypeUtil::kMaxPrecision, 18, true);
}

static void DecimalAdd3Fast(benchmark::State& state) {
  // use lesser precision to test the fast-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision - 6, 18);
}

static void DecimalAdd3LeadingZeroes(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 6);
}

static void DecimalAdd3LeadingZeroesWithDiv(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 18);
}

static void DecimalAdd3Large(benchmark::State& state) {
  // use max precision to test the large-integer-path
  DoDecimalAdd3(state, DecimalTypeUtil::kMaxPrecision, 18, true);
}

BENCHMARK(TimedTestExprCompilation)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestAdd3)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestAdd3ValidityBuffer)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestBigNested)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestBigNestedValidityBuffer)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestIfElseFieldArms)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCasePosition)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCaseStringEquals)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestExtractYear)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterAdd2)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterProjectAdd2)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterLike)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestFilterLikeMessages)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCastFloatFromString)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestCastIntFromString)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestAllocs)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestOutputStringAllocs)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestLower)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestMultiOr)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestMultiOrValidityBuffer)->Unit(benchmark::kMicrosecond);
BENCHMARK(TimedTestInExpr)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2Fast)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2LeadingZeroes)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2LeadingZeroesWithDiv)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd2Large)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3Fast)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3LeadingZeroes)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3LeadingZeroesWithDiv)->Unit(benchmark::kMicrosecond);
BENCHMARK(DecimalAdd3Large)->Unit(benchmark::kMicrosecond);

}  // namespace gandiva
