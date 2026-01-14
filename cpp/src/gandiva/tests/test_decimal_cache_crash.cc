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
#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/decimal.h"

#include "gandiva/projector.h"
#include "gandiva/tests/test_util.h"
#include "gandiva/tree_expr_builder.h"

using arrow::Decimal128;

namespace gandiva {

class TestDecimalCacheCrash : public ::testing::Test {
 public:
  void SetUp() { pool_ = arrow::default_memory_pool(); }

 protected:
  arrow::MemoryPool* pool_;
};

// This test reproduces the cache-related crash where:
// 1. Two queries with same expression types but different field names
// 2. Both hit the same cache entry (because cache key includes schema)
// 3. Second query uses buffer offsets compiled for first query's schema
// 4. Crashes when accessing wrong memory locations
TEST_F(TestDecimalCacheCrash, TestCacheWithDifferentFieldNames) {
  // Test configuration with caching enabled
  auto config = TestConfiguration();
  
  // ========== FIRST QUERY: field name "price" ==========
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 6;
  auto decimal_type = std::make_shared<arrow::Decimal128Type>(precision, scale);
  
  auto field_price = field("price", decimal_type);
  auto schema1 = arrow::schema({field_price});
  
  // Expression: castFLOAT8(floor(price))
  auto floor_result_type = arrow::decimal128(precision, 0);
  auto node_price = TreeExprBuilder::MakeField(field_price);
  auto floor_node = TreeExprBuilder::MakeFunction("floor", {node_price}, floor_result_type);
  auto cast_node = TreeExprBuilder::MakeFunction("castFLOAT8", {floor_node}, arrow::float64());
  auto expr1 = TreeExprBuilder::MakeExpression(cast_node, field("result", arrow::float64()));
  
  // Build first projector
  std::shared_ptr<Projector> projector1;
  auto status = Projector::Make(schema1, {expr1}, config, &projector1);
  ASSERT_TRUE(status.ok()) << status.message();
  EXPECT_FALSE(projector1->GetBuiltFromCache());  // First time, not from cache
  
  // Create test data for first query
  std::vector<Decimal128> decimal_values1;
  decimal_values1.push_back(Decimal128("123456789"));  // 123.456789 with scale 6
  decimal_values1.push_back(Decimal128("987654321"));  // 987.654321 with scale 6
  
  auto array1 = MakeArrowArrayDecimal(decimal_type, decimal_values1, {true, true});
  auto in_batch1 = arrow::RecordBatch::Make(schema1, 2, {array1});
  
  // Evaluate first query - should work fine
  arrow::ArrayVector outputs1;
  status = projector1->Evaluate(*in_batch1, pool_, &outputs1);
  ASSERT_TRUE(status.ok()) << status.message();
  
  // Verify first query results
  auto expected1 = MakeArrowArrayFloat64({123.0, 987.0}, {true, true});
  EXPECT_ARROW_ARRAY_EQUALS(expected1, outputs1[0]);
  
  // ========== SECOND QUERY: field name "cost" (different name, same type) ==========
  auto field_cost = field("cost", decimal_type);
  auto schema2 = arrow::schema({field_cost});
  
  // Same expression structure, but with different field name
  auto node_cost = TreeExprBuilder::MakeField(field_cost);
  auto floor_node2 = TreeExprBuilder::MakeFunction("floor", {node_cost}, floor_result_type);
  auto cast_node2 = TreeExprBuilder::MakeFunction("castFLOAT8", {floor_node2}, arrow::float64());
  auto expr2 = TreeExprBuilder::MakeExpression(cast_node2, field("result", arrow::float64()));
  
  // Build second projector - THIS SHOULD HIT THE CACHE (BUG!)
  std::shared_ptr<Projector> projector2;
  status = Projector::Make(schema2, {expr2}, config, &projector2);
  ASSERT_TRUE(status.ok()) << status.message();
  
  // In the buggy version, this will be TRUE because cache key includes schema
  // In the fixed version, this should be TRUE because cache key is type-based
  bool from_cache = projector2->GetBuiltFromCache();
  std::cerr << "Second projector built from cache: " << from_cache << std::endl;
  
  // Create test data for second query
  std::vector<Decimal128> decimal_values2;
  decimal_values2.push_back(Decimal128("555666777"));  // 555.666777 with scale 6
  decimal_values2.push_back(Decimal128("111222333"));  // 111.222333 with scale 6
  
  auto array2 = MakeArrowArrayDecimal(decimal_type, decimal_values2, {true, true});
  auto in_batch2 = arrow::RecordBatch::Make(schema2, 2, {array2});
  
  // Evaluate second query - THIS MAY CRASH if cache bug exists!
  // The cached code has buffer offsets for "price" field, but we're passing "cost" field
  arrow::ArrayVector outputs2;
  status = projector2->Evaluate(*in_batch2, pool_, &outputs2);
  
  // If we get here without crashing, check the results
  if (status.ok()) {
    auto expected2 = MakeArrowArrayFloat64({555.0, 111.0}, {true, true});
    EXPECT_ARROW_ARRAY_EQUALS(expected2, outputs2[0]);
    std::cerr << "Test PASSED - no crash!" << std::endl;
  } else {
    std::cerr << "Test FAILED with error: " << status.message() << std::endl;
    FAIL() << "Evaluation failed: " << status.message();
  }
}

}  // namespace gandiva

