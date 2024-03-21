
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

#ifndef M_PI
#define M_PI 3.14159265358979323846
#endif

#include "gandiva/filter.h"
#include "gandiva/projector.h"

#include <gtest/gtest.h>

#include <cmath>

#include "arrow/memory_pool.h"
#include "gandiva/literal_holder.h"
#include "gandiva/node.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

namespace gandiva {

using arrow::boolean;
using arrow::float32;
using arrow::int32;
using arrow::int64;
using arrow::utf8;

// compare the string representation of the arrow buffers
struct compare {
  bool operator()(const std::shared_ptr<arrow::Buffer>& lhs,
                  const std::shared_ptr<arrow::Buffer>& rhs) const {
    return lhs->ToString().compare(rhs->ToString()) < 0;
  }
};

class TestExprCache : public ::testing::Test {
 public:
  void SetUp() {
    pool_ = arrow::default_memory_pool();
    // Setup arrow log severity threshold to debug level.
    arrow::util::ArrowLog::StartArrowLog("", arrow::util::ArrowLogLevel::ARROW_DEBUG);
  }

 protected:
  arrow::MemoryPool* pool_;
};

TEST_F(TestExprCache, TestProjectSecCache) {
  for (int i = 0; i < 100; i++)
  {
  //std::cout << "test1" << std::endl;
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f2", int32());
  auto schema = arrow::schema({field0, field1});
//std::cout << "test2" << std::endl;
  // output fields
  auto field_sum = field("add", int32());
  auto field_sub = field("subtract", int32());

  // Build expression
  auto sum_expr = TreeExprBuilder::MakeExpression("add", {field0, field1}, field_sum);
  //std::cout << "test3" << std::endl;
  auto sub_expr =
      TreeExprBuilder::MakeExpression("subtract", {field0, field1}, field_sub);
//std::cout << "test4" << std::endl;
  auto configuration = TestConfiguration();

  std::shared_ptr<Projector> projector;
  auto status = Projector::Make(schema, {sum_expr, sub_expr}, configuration,
                                &projector);
  ASSERT_OK(status);
  status = Projector::Make(schema, {sum_expr, sub_expr}, configuration,
                                &projector);
  ASSERT_OK(status);
  //EXPECT_FALSE(projector->GetBuiltFromCache());
//std::cout << "test5" << std::endl;
  // everything is same, should return the same projector.
  auto schema_same = arrow::schema({field0, field1});
  std::shared_ptr<Projector> cached_projector;
  //std::cout << "test5b" << std::endl;
  status = Projector::Make(schema_same, {sum_expr, sub_expr}, configuration,
                           &cached_projector);
  //                         std::cout << "test6" << std::endl;
  ASSERT_OK(status);
  //EXPECT_TRUE(cached_projector->GetBuiltFromCache());
//std::cout << "test7" << std::endl;
  // schema is different should return a new projector.
  auto field2 = field("f2", int32());
  auto different_schema = arrow::schema({field0, field1, field2});
  std::shared_ptr<Projector> should_be_new_projector;
  status = Projector::Make(different_schema, {sum_expr, sub_expr}, configuration,
                           &should_be_new_projector);
  ASSERT_OK(status);
  //EXPECT_FALSE(should_be_new_projector->GetBuiltFromCache());
//std::cout << "test8" << std::endl;
  // expression list is different should return a new projector.
  std::shared_ptr<Projector> should_be_new_projector1;
  status = Projector::Make(schema, {sum_expr}, configuration,
                           &should_be_new_projector1);
  ASSERT_OK(status);
  //EXPECT_FALSE(should_be_new_projector1->GetBuiltFromCache());

  // another instance of the same configuration, should return the same projector.
  status = Projector::Make(schema, {sum_expr, sub_expr}, TestConfiguration(),
                           &cached_projector);
  //                         std::cout << "test9" << std::endl;
  ASSERT_OK(status);
  //EXPECT_TRUE(cached_projector->GetBuiltFromCache());
  }
}

TEST_F(TestExprCache, TestProjectCacheDecimalCast) {
  auto field_float64 = field("float64", arrow::float64());
  auto schema = arrow::schema({field_float64});

  auto res_31_13 = field("result", arrow::decimal(31, 13));
  auto expr0 = TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_13);
  std::shared_ptr<Projector> projector0;
  ASSERT_OK(
      Projector::Make(schema, {expr0}, TestConfiguration(), &projector0));
  //EXPECT_FALSE(projector0->GetBuiltFromCache());

  // if the output scale is different, the cache can't be used.
  auto res_31_14 = field("result", arrow::decimal(31, 14));
  auto expr1 = TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_14);
  std::shared_ptr<Projector> projector1;
  ASSERT_OK(
      Projector::Make(schema, {expr1}, TestConfiguration(), &projector1));
  //EXPECT_FALSE(projector1->GetBuiltFromCache());

  // if the output scale/precision are same, should get a cache hit.
  auto res_31_13_alt = field("result", arrow::decimal(31, 13));
  auto expr2 =
      TreeExprBuilder::MakeExpression("castDECIMAL", {field_float64}, res_31_13_alt);
  std::shared_ptr<Projector> projector2;
  ASSERT_OK(
      Projector::Make(schema, {expr2}, TestConfiguration(), &projector2));
  //EXPECT_TRUE(projector2->GetBuiltFromCache());
}

TEST_F(TestExprCache, TestFilterCache) {
  // schema for input fields
  auto field0 = field("f0", int32());
  auto field1 = field("f1", int32());
  auto schema = arrow::schema({field0, field1});

  // Build condition f0 + f1 < 10
  auto node_f0 = TreeExprBuilder::MakeField(field0);
  auto node_f1 = TreeExprBuilder::MakeField(field1);
  auto sum_func =
      TreeExprBuilder::MakeFunction("add", {node_f0, node_f1}, arrow::int32());
  auto literal_10 = TreeExprBuilder::MakeLiteral((int32_t)10);
  auto less_than_10 = TreeExprBuilder::MakeFunction("less_than", {sum_func, literal_10},
                                                    arrow::boolean());
  auto condition = TreeExprBuilder::MakeCondition(less_than_10);
  auto configuration = TestConfiguration();

  std::shared_ptr<Filter> filter;
  auto status = Filter::Make(schema, condition, configuration, &filter);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(filter->GetBuiltFromCache());
  //filter->Clear();

  // same schema and condition, should return the same filter as above.
  std::shared_ptr<Filter> cached_filter;
  status = Filter::Make(schema, condition, configuration, &cached_filter);
  EXPECT_TRUE(status.ok());
  EXPECT_TRUE(cached_filter->GetBuiltFromCache());
  //cached_filter->Clear();

  // schema is different should return a new filter.
  auto field2 = field("f2", int32());
  auto different_schema = arrow::schema({field0, field1, field2});
  std::shared_ptr<Filter> should_be_new_filter;
  status = Filter::Make(different_schema, condition, configuration,
                        &should_be_new_filter);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(should_be_new_filter->GetBuiltFromCache());

  // condition is different, should return a new filter.
  auto greater_than_10 = TreeExprBuilder::MakeFunction(
      "greater_than", {sum_func, literal_10}, arrow::boolean());
  auto new_condition = TreeExprBuilder::MakeCondition(greater_than_10);
  std::shared_ptr<Filter> should_be_new_filter1;
  status = Filter::Make(schema, new_condition, configuration,
                        &should_be_new_filter1);
  EXPECT_TRUE(status.ok());
  EXPECT_FALSE(should_be_new_filter->GetBuiltFromCache());
}
}  // namespace gandiva
