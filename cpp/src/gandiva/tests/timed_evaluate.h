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

#include <memory>
#include <vector>
#include "arrow/util/bit_util.h"
#include "benchmark/benchmark.h"
#include "gandiva/arrow.h"
#include "gandiva/filter.h"
#include "gandiva/projector.h"
#include "gandiva/tests/generate_data.h"

#pragma once

#define THOUSAND (1024)
#define MILLION (1024 * 1024)
#define NUM_BATCHES 16

namespace gandiva {

template <typename C_TYPE>
std::vector<C_TYPE> GenerateData(int num_records, DataGenerator<C_TYPE>& data_generator) {
  std::vector<C_TYPE> data;

  for (int i = 0; i < num_records; i++) {
    data.push_back(data_generator.GenerateData());
  }

  return data;
}

class BaseEvaluator {
 public:
  virtual ~BaseEvaluator() = default;

  virtual Status Evaluate(arrow::RecordBatch& batch, arrow::MemoryPool* pool) = 0;
};

class ProjectEvaluator : public BaseEvaluator {
 public:
  explicit ProjectEvaluator(std::shared_ptr<Projector> projector)
      : projector_(projector) {}

  Status Evaluate(arrow::RecordBatch& batch, arrow::MemoryPool* pool) override {
    arrow::ArrayVector outputs;
    return projector_->Evaluate(batch, pool, &outputs);
  }

 private:
  std::shared_ptr<Projector> projector_;
};

class FilterEvaluator : public BaseEvaluator {
 public:
  explicit FilterEvaluator(std::shared_ptr<Filter> filter) : filter_(filter) {}

  Status Evaluate(arrow::RecordBatch& batch, arrow::MemoryPool* pool) override {
    if (selection_ == nullptr || selection_->GetMaxSlots() < batch.num_rows()) {
      auto status = SelectionVector::MakeInt16(batch.num_rows(), pool, &selection_);
      if (!status.ok()) {
        return status;
      }
    }
    return filter_->Evaluate(batch, selection_);
  }

 private:
  std::shared_ptr<Filter> filter_;
  std::shared_ptr<SelectionVector> selection_;
};

// Runs a filter, then evaluates the projector over the rows the filter selected.
class FilterProjectEvaluator : public BaseEvaluator {
 public:
  FilterProjectEvaluator(std::shared_ptr<Filter> filter,
                         std::shared_ptr<Projector> projector)
      : filter_(filter), projector_(projector) {}

  Status Evaluate(arrow::RecordBatch& batch, arrow::MemoryPool* pool) override {
    if (selection_ == nullptr || selection_->GetMaxSlots() < batch.num_rows()) {
      ARROW_RETURN_NOT_OK(
          SelectionVector::MakeInt16(batch.num_rows(), pool, &selection_));
    }
    ARROW_RETURN_NOT_OK(filter_->Evaluate(batch, selection_));
    arrow::ArrayVector outputs;
    return projector_->Evaluate(batch, selection_.get(), pool, &outputs);
  }

 private:
  std::shared_ptr<Filter> filter_;
  std::shared_ptr<Projector> projector_;
  std::shared_ptr<SelectionVector> selection_;
};

template <typename TYPE, typename C_TYPE>
Status TimedEvaluate(SchemaPtr schema, BaseEvaluator& evaluator,
                     DataGenerator<C_TYPE>& data_generator, arrow::MemoryPool* pool,
                     int num_records, int batch_size, benchmark::State& state,
                     bool with_validity_buffer = false) {
  int num_remaining = num_records;
  int num_fields = schema->num_fields();
  int num_calls = 0;
  Status status;
  int64_t total_bytes_processed = 0;
  int64_t total_items_processed = 0;

  // Generate batches of data
  std::shared_ptr<arrow::RecordBatch> batches[NUM_BATCHES];
  for (int i = 0; i < NUM_BATCHES; i++) {
    // generate data for all columns in the schema
    std::vector<ArrayPtr> columns;
    int64_t batch_bytes = 0;
    for (int col = 0; col < num_fields; col++) {
      std::vector<C_TYPE> data = GenerateData<C_TYPE>(batch_size, data_generator);
      std::vector<bool> validity(batch_size, true);
      ArrayPtr col_data =
          MakeArrowArray<TYPE, C_TYPE>(schema->field(col)->type(), data, validity);
      if (with_validity_buffer) {
        // Arrow builders leave out the validity buffer when there are no nulls, but
        // engines like Dremio always provide one. Attach an all-valid bitmap.
        auto array_data = col_data->data()->Copy();
        ARROW_ASSIGN_OR_RAISE(array_data->buffers[0],
                              arrow::AllocateBitmap(batch_size, pool));
        arrow::bit_util::SetBitsTo(array_data->buffers[0]->mutable_data(), 0, batch_size,
                                   true);
        array_data->null_count = 0;
        col_data = arrow::MakeArray(array_data);
      }

      columns.push_back(col_data);
      batch_bytes += data.size() * sizeof(C_TYPE);
    }

    // make the record batch
    std::shared_ptr<arrow::RecordBatch> batch =
        arrow::RecordBatch::Make(schema, batch_size, columns);
    batches[i] = batch;
    total_bytes_processed += batch_bytes;
  }

  for (auto _ : state) {
    int num_in_batch = batch_size;
    num_remaining = num_records;
    while (num_remaining > 0) {
      if (batch_size > num_remaining) {
        num_in_batch = num_remaining;
      }

      status = evaluator.Evaluate(*(batches[num_calls % NUM_BATCHES]), pool);
      if (!status.ok()) {
        state.SkipWithError("Evaluation of the batch failed");
        return status;
      }

      num_calls++;
      num_remaining -= num_in_batch;
      total_items_processed += num_in_batch;
    }
  }

  state.SetBytesProcessed(total_bytes_processed);
  state.SetItemsProcessed(total_items_processed);

  return Status::OK();
}

}  // namespace gandiva
