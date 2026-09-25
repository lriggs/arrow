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

#include <gtest/gtest.h>

#include <optional>
#include <random>
#include "arrow/memory_pool.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/selection_vector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;

class TestFilterProject : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestFilterProject, TestSimple16) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, SelectionVector::MODE_UINT16,
                           configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt16(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimple32) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, SelectionVector::MODE_UINT32,
                           configuration, &projector);
  EXPECT_TRUE(status.ok());

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt32(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  ASSERT_OK(status);

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimple64) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto field2 = field("f2", int32());
  auto resultField = field("result", int32());
  auto schema = arrow::schema({field0, field1, field2});

  // Build condition f0 < f1
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto node_f2 = TreeExprBuilder::MakeField(field2);
  auto less_than_function =
      TreeExprBuilder::MakeFunction("less_than", {node_f0, node_f1}, arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_function);
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field1, field2}, resultField);

  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  std::shared_ptr<Projector> projector;

  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  status = Projector::Make(schema, {sum_expr}, SelectionVector::MODE_UINT64,
                           configuration, &projector);
  ASSERT_OK(status);

  // Create a row-batch with some sample data
  int num_records = 5;
  auto array0 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, true});
  auto array1 = MakeArrowArrayInt32({5, 9, 3, 17, 6}, {true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 2, 6, 40, 3}, {true, true, true, true, false});
  // expected output
  auto result = MakeArrowArrayInt32({6, 11, 0}, {true, true, false});
  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt64(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());
  // Evaluate expression
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate expression
  arrow::ArrayVector outputs;

  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(result, outputs.at(0));
}

TEST_F(TestFilterProject, TestSimpleIf) {
  // schema for input fields
  auto fielda = field("a", int32());
  auto fieldb = field("b", int32());
  auto fieldc = field("c", int32());
  auto schema = arrow::schema({fielda, fieldb, fieldc});

  // output fields
  auto field_result = field("res", int32());

  auto node_a = TreeExprBuilder::MakeField(fielda);
  auto node_b = TreeExprBuilder::MakeField(fieldb);
  auto node_c = TreeExprBuilder::MakeField(fieldc);

  auto greater_than_function =
      TreeExprBuilder::MakeFunction("greater_than", {node_a, node_b}, boolean());
  auto filter_condition = TreeExprBuilder::MakeCondition(greater_than_function);

  auto project_condition =
      TreeExprBuilder::MakeFunction("less_than", {node_b, node_c}, boolean());
  auto if_node = TreeExprBuilder::MakeIf(project_condition, node_b, node_c, int32());

  auto expr = TreeExprBuilder::MakeExpression(if_node, field_result);
  auto configuration = TestConfiguration();

  // Build a filter for the expressions.
  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, filter_condition, configuration, &filter);
  EXPECT_TRUE(status.ok());

  // Build a projector for the expressions.
  std::shared_ptr<Projector> projector;
  status = Projector::Make(schema, {expr}, SelectionVector::MODE_UINT32, configuration,
                           &projector);
  ASSERT_OK(status);

  // Create a row-batch with some sample data
  int num_records = 6;
  auto array0 =
      MakeArrowArrayInt32({10, 12, -20, 5, 21, 29}, {true, true, true, true, true, true});
  auto array1 =
      MakeArrowArrayInt32({5, 15, 15, 17, 12, 3}, {true, true, true, true, true, true});
  auto array2 = MakeArrowArrayInt32({1, 25, 11, 30, -21, 30},
                                    {true, true, true, true, true, false});

  // Create a selection vector
  std::shared_ptr<SelectionVector> selection_vector;
  status = SelectionVector::MakeInt32(num_records, pool_, &selection_vector);
  EXPECT_TRUE(status.ok());

  // expected output
  auto exp = MakeArrowArrayInt32({1, -21, 0}, {true, true, false});

  // prepare input record batch
  auto in_batch = arrow::RecordBatch::Make(schema, num_records, {array0, array1, array2});

  // Evaluate filter
  status = filter->Evaluate(*in_batch, selection_vector);
  EXPECT_TRUE(status.ok());

  // Evaluate project
  arrow::ArrayVector outputs;
  status = projector->Evaluate(*in_batch, selection_vector.get(), pool_, &outputs);
  EXPECT_TRUE(status.ok());

  // Validate results
  EXPECT_ARROW_ARRAY_EQUALS(exp, outputs.at(0));
}
// Boolean outputs are generated as bytes and packed into bitmaps after the jitted loop.
// Check the packing at sizes around byte/word boundaries, with nulls, several boolean
// outputs from one projector, and a projection over a filter's selection vector.
TEST_F(TestFilterProject, TestBooleanOutputsAtBoundarySizes) {
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  auto lt_expr = TreeExprBuilder::MakeExpression("less_than", {field0, field1},
                                                 field("lt", arrow::boolean()));
  auto eq_expr = TreeExprBuilder::MakeExpression("equal", {field0, field1},
                                                 field("eq", arrow::boolean()));
  auto add_expr =
      TreeExprBuilder::MakeExpression("add", {field0, field1}, field("sum", int32()));
  auto gt_expr = TreeExprBuilder::MakeExpression("greater_than", {field0, field1},
                                                 field("gt", arrow::boolean()));
  auto condition = TreeExprBuilder::MakeCondition("less_than", {field0, field1});

  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, {lt_expr, eq_expr, add_expr}, TestConfiguration(),
                            &projector));
  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, condition, TestConfiguration(), &filter));
  std::shared_ptr<Projector> sv_projector;
  ASSERT_OK(Projector::Make(schema, {gt_expr}, SelectionVector::MODE_UINT16,
                            TestConfiguration(), &sv_projector));

  std::mt19937 rng(42);
  for (int num_records : {1, 7, 8, 9, 15, 63, 64, 65, 127, 1000, 4097}) {
    std::vector<int32_t> v0(num_records), v1(num_records);
    std::vector<bool> valid0(num_records), valid1(num_records);
    for (int i = 0; i < num_records; ++i) {
      v0[i] = static_cast<int32_t>(rng() % 4);
      v1[i] = static_cast<int32_t>(rng() % 4);
      valid0[i] = rng() % 5 != 0;
      valid1[i] = rng() % 5 != 0;
    }
    auto in_batch = arrow::RecordBatch::Make(
        schema, num_records,
        {MakeArrowArrayInt32(v0, valid0), MakeArrowArrayInt32(v1, valid1)});

    std::vector<bool> lt(num_records), eq(num_records), gt(num_records),
        valid(num_records);
    std::vector<int32_t> sum(num_records);
    std::vector<int> selected;
    for (int i = 0; i < num_records; ++i) {
      valid[i] = valid0[i] && valid1[i];
      lt[i] = valid[i] && v0[i] < v1[i];
      eq[i] = valid[i] && v0[i] == v1[i];
      gt[i] = valid[i] && v0[i] > v1[i];
      sum[i] = valid[i] ? v0[i] + v1[i] : 0;
      if (lt[i]) {
        selected.push_back(i);
      }
    }

    arrow::ArrayVector outputs;
    ASSERT_OK(projector->Evaluate(*in_batch, pool_, &outputs));
    EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool(lt, valid), outputs.at(0));
    EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool(eq, valid), outputs.at(1));
    EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayInt32(sum, valid), outputs.at(2));

    std::shared_ptr<SelectionVector> selection_vector;
    ASSERT_OK(SelectionVector::MakeInt16(num_records, pool_, &selection_vector));
    ASSERT_OK(filter->Evaluate(*in_batch, selection_vector));
    ASSERT_EQ(selection_vector->GetNumSlots(), static_cast<int64_t>(selected.size()));
    for (size_t i = 0; i < selected.size(); ++i) {
      ASSERT_EQ(selection_vector->GetIndex(i), static_cast<uint64_t>(selected[i]));
    }

    if (!selected.empty()) {
      std::vector<bool> exp_gt, exp_valid;
      for (int idx : selected) {
        exp_gt.push_back(gt[idx]);
        exp_valid.push_back(valid[idx]);
      }
      arrow::ArrayVector sv_outputs;
      ASSERT_OK(
          sv_projector->Evaluate(*in_batch, selection_vector.get(), pool_, &sv_outputs));
      EXPECT_ARROW_ARRAY_EQUALS(MakeArrowArrayBool(exp_gt, exp_valid), sv_outputs.at(0));
    }
  }
}

// The generated code reads input validity and local bitmaps as one byte per row, and
// builds cheap if-else chains with selects. Compare against a reference evaluation, with
// nulls in every input, sliced inputs (non-zero bit offsets), and a selection vector.
TEST_F(TestFilterProject, TestNullableExprsWithValidityBuffers) {
  auto fa = field("a", int32());
  auto fb = field("b", int32());
  auto fc = field("c", int32());
  auto fs = field("s", arrow::utf8());
  auto schema = arrow::schema({fa, fb, fc, fs});
  auto a = TreeExprBuilder::MakeField(fa);
  auto b = TreeExprBuilder::MakeField(fb);
  auto c = TreeExprBuilder::MakeField(fc);
  auto s = TreeExprBuilder::MakeField(fs);
  auto lit = [](int32_t v) { return TreeExprBuilder::MakeLiteral(v); };
  auto fn = [](const std::string& name, const NodeVector& args, DataTypePtr type) {
    return TreeExprBuilder::MakeFunction(name, args, type);
  };
  auto boolean = arrow::boolean();

  // if (a < b) a else if (a > 1) b else c : speculatable, built with selects.
  auto if_select = TreeExprBuilder::MakeIf(
      fn("less_than", {a, b}, boolean), a,
      TreeExprBuilder::MakeIf(fn("greater_than", {a, lit(1)}, boolean), b, c, int32()),
      int32());
  // if (a < b) negative(a) else c : not speculatable, built with branches.
  auto if_branch = TreeExprBuilder::MakeIf(fn("less_than", {a, b}, boolean),
                                           fn("negative", {a}, int32()), c, int32());
  // (a < 2 and b > 1) or isnull(c)
  auto and_or = TreeExprBuilder::MakeOr(
      {TreeExprBuilder::MakeAnd({fn("less_than", {a, lit(2)}, boolean),
                                 fn("greater_than", {b, lit(1)}, boolean)}),
       fn("isnull", {c}, boolean)});
  // s = "x" or s = "yy" or s = "zzz"
  auto str_or = TreeExprBuilder::MakeOr(
      {fn("equal", {s, TreeExprBuilder::MakeStringLiteral("x")}, boolean),
       fn("equal", {s, TreeExprBuilder::MakeStringLiteral("yy")}, boolean),
       fn("equal", {s, TreeExprBuilder::MakeStringLiteral("zzz")}, boolean)});

  ExpressionVector exprs = {
      TreeExprBuilder::MakeExpression(if_select, field("if_select", int32())),
      TreeExprBuilder::MakeExpression(if_branch, field("if_branch", int32())),
      TreeExprBuilder::MakeExpression(and_or, field("and_or", boolean)),
      TreeExprBuilder::MakeExpression(str_or, field("str_or", boolean))};
  std::shared_ptr<Projector> projector;
  ASSERT_OK(Projector::Make(schema, exprs, TestConfiguration(), &projector));
  std::shared_ptr<Projector> sv_projector;
  ASSERT_OK(Projector::Make(schema, exprs, SelectionVector::MODE_UINT16,
                            TestConfiguration(), &sv_projector));
  std::shared_ptr<Filter> filter;
  ASSERT_OK(Filter::Make(schema, TreeExprBuilder::MakeCondition(and_or),
                         TestConfiguration(), &filter));

  using OptInt = std::optional<int32_t>;
  using OptBool = std::optional<bool>;
  const std::vector<std::string> strings = {"x", "yy", "zzz", "w", "", "xx"};

  std::mt19937 rng(7);
  for (int total : {9, 64, 200, 1001}) {
    std::vector<int32_t> va(total), vb(total), vc(total);
    std::vector<std::string> vs(total);
    std::vector<bool> na(total), nb(total), nc(total), ns(total);
    for (int i = 0; i < total; ++i) {
      va[i] = static_cast<int32_t>(rng() % 4);
      vb[i] = static_cast<int32_t>(rng() % 4);
      vc[i] = static_cast<int32_t>(rng() % 4);
      vs[i] = strings[rng() % strings.size()];
      na[i] = rng() % 4 != 0;
      nb[i] = rng() % 4 != 0;
      nc[i] = rng() % 4 != 0;
      ns[i] = rng() % 4 != 0;
    }
    auto full_batch = arrow::RecordBatch::Make(
        schema, total,
        {MakeArrowArrayInt32(va, na), MakeArrowArrayInt32(vb, nb),
         MakeArrowArrayInt32(vc, nc), MakeArrowArrayUtf8(vs, ns)});

    for (int offset : {0, 3, 8}) {
      if (offset >= total) continue;
      auto batch = full_batch->Slice(offset);
      const int n = static_cast<int>(batch->num_rows());

      std::vector<OptInt> exp_select(n), exp_branch(n);
      std::vector<OptBool> exp_and_or(n), exp_str_or(n);
      for (int r = 0; r < n; ++r) {
        const int i = r + offset;
        OptInt oa = na[i] ? OptInt(va[i]) : std::nullopt;
        OptInt ob = nb[i] ? OptInt(vb[i]) : std::nullopt;
        OptInt oc = nc[i] ? OptInt(vc[i]) : std::nullopt;
        bool a_lt_b = oa && ob && *oa < *ob;
        exp_select[r] = a_lt_b ? oa : ((oa && *oa > 1) ? ob : oc);
        exp_branch[r] = a_lt_b ? OptInt(-*oa) : oc;
        // Three-valued logic.
        OptBool x = oa ? OptBool(*oa < 2) : std::nullopt;
        OptBool y = ob ? OptBool(*ob > 1) : std::nullopt;
        OptBool x_and_y = (x == false || y == false) ? OptBool(false)
                          : (x && y)                 ? OptBool(true)
                                                     : std::nullopt;
        bool c_null = !oc.has_value();
        exp_and_or[r] = (x_and_y == true || c_null) ? OptBool(true)
                        : x_and_y.has_value()       ? OptBool(false)
                                                    : std::nullopt;
        exp_str_or[r] = ns[i] ? OptBool(vs[i] == "x" || vs[i] == "yy" || vs[i] == "zzz")
                              : std::nullopt;
      }

      auto int_array = [](const std::vector<OptInt>& v, const std::vector<int>& rows) {
        std::vector<int32_t> values;
        std::vector<bool> valid;
        for (int r : rows) {
          values.push_back(v[r].value_or(0));
          valid.push_back(v[r].has_value());
        }
        return MakeArrowArrayInt32(values, valid);
      };
      auto bool_array = [](const std::vector<OptBool>& v, const std::vector<int>& rows) {
        std::vector<bool> values, valid;
        for (int r : rows) {
          values.push_back(v[r].value_or(false));
          valid.push_back(v[r].has_value());
        }
        return MakeArrowArrayBool(values, valid);
      };

      std::vector<int> all_rows(n), selected;
      for (int r = 0; r < n; ++r) {
        all_rows[r] = r;
        if (exp_and_or[r] == true) selected.push_back(r);
      }

      arrow::ArrayVector outputs;
      ASSERT_OK(projector->Evaluate(*batch, pool_, &outputs));
      EXPECT_ARROW_ARRAY_EQUALS(int_array(exp_select, all_rows), outputs.at(0));
      EXPECT_ARROW_ARRAY_EQUALS(int_array(exp_branch, all_rows), outputs.at(1));
      EXPECT_ARROW_ARRAY_EQUALS(bool_array(exp_and_or, all_rows), outputs.at(2));
      EXPECT_ARROW_ARRAY_EQUALS(bool_array(exp_str_or, all_rows), outputs.at(3));

      std::shared_ptr<SelectionVector> selection_vector;
      ASSERT_OK(SelectionVector::MakeInt16(n, pool_, &selection_vector));
      ASSERT_OK(filter->Evaluate(*batch, selection_vector));
      ASSERT_EQ(selection_vector->GetNumSlots(), static_cast<int64_t>(selected.size()));
      if (selected.empty()) continue;
      arrow::ArrayVector sv_outputs;
      ASSERT_OK(
          sv_projector->Evaluate(*batch, selection_vector.get(), pool_, &sv_outputs));
      EXPECT_ARROW_ARRAY_EQUALS(int_array(exp_select, selected), sv_outputs.at(0));
      EXPECT_ARROW_ARRAY_EQUALS(int_array(exp_branch, selected), sv_outputs.at(1));
      EXPECT_ARROW_ARRAY_EQUALS(bool_array(exp_and_or, selected), sv_outputs.at(2));
      EXPECT_ARROW_ARRAY_EQUALS(bool_array(exp_str_or, selected), sv_outputs.at(3));
    }
  }
}

}  // namespace gandiva
