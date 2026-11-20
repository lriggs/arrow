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

#include "gandiva/projector.h"

#include <algorithm>
#include <condition_variable>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <queue>
#include <thread>
#include <utility>
#include <vector>

#include "arrow/util/logging.h"

#include "gandiva/cache.h"
#include "gandiva/expr_validator.h"
#include "gandiva/expression_cache_key.h"
#include "gandiva/gandiva_object_cache.h"
#include "gandiva/llvm_generator.h"
#include "gandiva/node.h"

namespace gandiva {

namespace {
// Async compile is opt-in via the GANDIVA_ASYNC_COMPILE environment variable. When set
// (to any non-empty value other than "0"), a cold optimized Projector build returns an
// unoptimized projector immediately and upgrades it to -O3 out of band. Read once.
bool AsyncCompileEnabled() {
  static const bool enabled = [] {
    const char* v = std::getenv("GANDIVA_ASYNC_COMPILE");
    return v != nullptr && v[0] != '\0' && std::string(v) != "0";
  }();
  return enabled;
}

// Minimum estimated compile cost (see EstimateCompileCost) for the async upgrade to be
// worthwhile. Cheap expression sets compile fast at -O3 anyway, so paying for an async
// unoptimized-then-optimized round trip only adds latency and relaxes cache coherence
// (the synchronous build populates the object cache in-line; the async build does not
// until the background task lands). Default tuned so trivial arithmetic / single casts
// stay synchronous. Override with GANDIVA_ASYNC_COMPILE_MIN_COST.
size_t MinAsyncCost() {
  static const size_t cost = [] {
    const char* v = std::getenv("GANDIVA_ASYNC_COMPILE_MIN_COST");
    if (v != nullptr && v[0] != '\0') {
      long n = std::atol(v);
      if (n >= 0) return static_cast<size_t>(n);
    }
    return static_cast<size_t>(256);
  }();
  return cost;
}

// Cheap, uniform proxy for "how expensive is this expression set to compile at -O3":
// the summed length of each expression's serialized form. ToString() is defined on every
// Node, grows with tree size/complexity, and is computed only once per Make (negligible
// next to the LLVM compile it gates). Avoids a bespoke node-counting visitor.
size_t EstimateCompileCost(const ExpressionVector& exprs) {
  size_t cost = 0;
  for (const auto& expr : exprs) {
    if (expr != nullptr && expr->root() != nullptr) {
      cost += expr->root()->ToString().size();
    }
  }
  return cost;
}

// Fixed-size pool of worker threads that run the background -O3 upgrades. A shared pool
// (rather than one std::thread per projector) bounds thread count when many projectors
// are built concurrently. Intentionally leaked: a never-destroyed singleton avoids
// static-destruction-order hazards with worker threads at process exit.
class AsyncCompilePool {
 public:
  static AsyncCompilePool& Instance() {
    static AsyncCompilePool* pool = new AsyncCompilePool(DefaultThreads());
    return *pool;
  }

  std::future<void> Submit(std::function<void()> fn) {
    auto task = std::make_shared<std::packaged_task<void()>>(std::move(fn));
    std::future<void> fut = task->get_future();
    {
      std::lock_guard<std::mutex> lk(mutex_);
      queue_.emplace([task] { (*task)(); });
    }
    cv_.notify_one();
    return fut;
  }

 private:
  explicit AsyncCompilePool(size_t num_threads) {
    for (size_t i = 0; i < num_threads; ++i) {
      workers_.emplace_back([this] { WorkerLoop(); });
    }
  }

  static size_t DefaultThreads() {
    const char* v = std::getenv("GANDIVA_ASYNC_COMPILE_THREADS");
    if (v != nullptr && v[0] != '\0') {
      int n = std::atoi(v);
      if (n > 0) return static_cast<size_t>(n);
    }
    unsigned hc = std::thread::hardware_concurrency();
    return std::max(1u, hc / 2);
  }

  void WorkerLoop() {
    for (;;) {
      std::function<void()> job;
      {
        std::unique_lock<std::mutex> lk(mutex_);
        cv_.wait(lk, [this] { return !queue_.empty(); });
        job = std::move(queue_.front());
        queue_.pop();
      }
      job();
    }
  }

  std::mutex mutex_;
  std::condition_variable cv_;
  std::queue<std::function<void()>> queue_;
  std::vector<std::thread> workers_;
};
}  // namespace

Projector::Projector(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                     const FieldVector& output_fields,
                     std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      output_fields_(output_fields),
      configuration_(configuration) {}

Projector::~Projector() {
  // A background -O3 upgrade (if any) holds a raw `this` and writes our generators. Tell
  // a not-yet-started task to bail, then wait for an in-flight one to finish before we
  // destroy members. The wait also establishes happens-before for the task's writes.
  if (async_cancel_) {
    async_cancel_->store(true, std::memory_order_relaxed);
  }
  if (async_upgrade_future_.valid()) {
    async_upgrade_future_.wait();
  }
}

Status Projector::Make(SchemaPtr schema, const ExpressionVector& exprs,
                       std::shared_ptr<Projector>* projector) {
  return Projector::Make(schema, exprs, SelectionVector::Mode::MODE_NONE,
                         ConfigurationBuilder::DefaultConfiguration(), projector);
}

Status Projector::Make(SchemaPtr schema, const ExpressionVector& exprs,
                       std::shared_ptr<Configuration> configuration,
                       std::shared_ptr<Projector>* projector) {
  return Projector::Make(schema, exprs, SelectionVector::Mode::MODE_NONE, configuration,
                         projector);
}

Status Projector::Make(SchemaPtr schema, const ExpressionVector& exprs,
                       SelectionVector::Mode selection_vector_mode,
                       std::shared_ptr<Configuration> configuration,
                       std::shared_ptr<Projector>* projector) {
  ARROW_RETURN_IF(schema == nullptr, Status::Invalid("Schema cannot be null"));
  ARROW_RETURN_IF(exprs.empty(), Status::Invalid("Expressions cannot be empty"));
  ARROW_RETURN_IF(configuration == nullptr,
                  Status::Invalid("Configuration cannot be null"));

  // see if equivalent projector was already built
  std::shared_ptr<Cache<ExpressionCacheKey, std::shared_ptr<llvm::MemoryBuffer>>> cache =
      LLVMGenerator::GetCache();

  ExpressionCacheKey cache_key(schema, configuration, exprs, selection_vector_mode);

  bool is_cached = false;

  std::shared_ptr<llvm::MemoryBuffer> prev_cached_obj;
  prev_cached_obj = cache->GetObjectCode(cache_key);

  // Verify if previous projector obj code was cached
  if (prev_cached_obj != nullptr) {
    is_cached = true;
  }

  GandivaObjectCache obj_cache(cache, cache_key);

  // Async compile (GANDIVA_ASYNC_COMPILE): when this is a cold, optimized build and the
  // expression set is expensive enough to compile (EstimateCompileCost >= MinAsyncCost),
  // compile an UNoptimized version now for low latency and upgrade to -O3 in the
  // background. Cheap expressions stay synchronous so they still populate the object
  // cache in-line.
  const bool use_async = AsyncCompileEnabled() && !is_cached &&
                         configuration->optimize() &&
                         EstimateCompileCost(exprs) >= MinAsyncCost();

  std::unique_ptr<LLVMGenerator> llvm_gen;
  if (use_async) {
    auto fast_config = std::make_shared<Configuration>(*configuration);
    fast_config->set_optimize(false);
    ARROW_ASSIGN_OR_RAISE(llvm_gen, LLVMGenerator::Make(fast_config, /*cached=*/false));
  } else {
    ARROW_ASSIGN_OR_RAISE(llvm_gen,
                          LLVMGenerator::Make(configuration, is_cached, obj_cache));
  }

  // Run the validation on the expressions.
  // Return if any of the expression is invalid since
  // we will not be able to process further.
  if (!is_cached) {
    ExprValidator expr_validator(llvm_gen->types(), schema,
                                 configuration->function_registry());
    for (auto& expr : exprs) {
      ARROW_RETURN_NOT_OK(expr_validator.Validate(expr));
    }
  }

  // Pre-load the already-compiled object from the cache lookup above, if there was
  // one. Reuses prev_cached_obj instead of letting SetLLVMObjectCache re-query the
  // shared cache, so this can never disagree with the is_cached snapshot that
  // decided whether Build() below still compiles fresh IR (see GH-601). The async
  // path's primary is always a fresh unoptimized build, so it skips this.
  if (!use_async) {
    ARROW_RETURN_NOT_OK(llvm_gen->SetLLVMObjectCache(prev_cached_obj));
  }

  ARROW_RETURN_NOT_OK(llvm_gen->Build(exprs, selection_vector_mode));

  // save the output field types. Used for validation at Evaluate() time.
  std::vector<FieldPtr> output_fields;
  output_fields.reserve(exprs.size());
  for (auto& expr : exprs) {
    output_fields.push_back(expr->result());
  }

  // Instantiate the projector with the completely built llvm generator
  *projector = std::shared_ptr<Projector>(
      new Projector(std::move(llvm_gen), schema, output_fields, configuration));
  projector->get()->SetBuiltFromCache(is_cached);

  // Kick off the background -O3 upgrade that hot-swaps optimized code into the running
  // projector (also populates the shared object cache for future calls).
  if (use_async) {
    projector->get()->StartAsyncUpgrade(exprs, selection_vector_mode, configuration);
  }

  return Status::OK();
}

Status Projector::Evaluate(const arrow::RecordBatch& batch,
                           const ArrayDataVector& output_data_vecs) const {
  return Evaluate(batch, nullptr, output_data_vecs);
}

Status Projector::Evaluate(const arrow::RecordBatch& batch,
                           const SelectionVector* selection_vector,
                           const ArrayDataVector& output_data_vecs) const {
  ARROW_RETURN_NOT_OK(ValidateEvaluateArgsCommon(batch));

  if (output_data_vecs.size() != output_fields_.size()) {
    std::stringstream ss;
    ss << "number of buffers for output_data_vecs is " << output_data_vecs.size()
       << ", expected " << output_fields_.size();
    return Status::Invalid(ss.str());
  }

  int idx = 0;
  for (auto& array_data : output_data_vecs) {
    if (array_data == nullptr) {
      std::stringstream ss;
      ss << "array for output field " << output_fields_[idx]->name() << "is null.";
      return Status::Invalid(ss.str());
    }

    auto num_rows =
        selection_vector == nullptr ? batch.num_rows() : selection_vector->GetNumSlots();

    ARROW_RETURN_NOT_OK(
        ValidateArrayDataCapacity(*array_data, *(output_fields_[idx]), num_rows));
    ++idx;
  }
  return llvm_generator_->Execute(batch, selection_vector, output_data_vecs);
}

Status Projector::Evaluate(const arrow::RecordBatch& batch, arrow::MemoryPool* pool,
                           arrow::ArrayVector* output) const {
  return Evaluate(batch, nullptr, pool, output);
}

Status Projector::Evaluate(const arrow::RecordBatch& batch,
                           const SelectionVector* selection_vector,
                           arrow::MemoryPool* pool, arrow::ArrayVector* output) const {
  ARROW_RETURN_NOT_OK(ValidateEvaluateArgsCommon(batch));
  ARROW_RETURN_IF(output == nullptr, Status::Invalid("Output must be non-null."));
  ARROW_RETURN_IF(pool == nullptr, Status::Invalid("Memory pool must be non-null."));

  auto num_rows =
      selection_vector == nullptr ? batch.num_rows() : selection_vector->GetNumSlots();
  // Allocate the output data vecs.
  ArrayDataVector output_data_vecs;
  for (auto& field : output_fields_) {
    ArrayDataPtr output_data;

    ARROW_RETURN_NOT_OK(AllocArrayData(field->type(), num_rows, pool, &output_data));
    output_data_vecs.push_back(output_data);
  }

  // Execute the expression(s).
  ARROW_RETURN_NOT_OK(
      llvm_generator_->Execute(batch, selection_vector, output_data_vecs));

  // Create and return array arrays.
  output->clear();
  for (auto& array_data : output_data_vecs) {
    output->push_back(arrow::MakeArray(array_data));
  }
  return Status::OK();
}

// TODO : handle complex vectors (list/map/..)
Status Projector::AllocArrayData(const DataTypePtr& type, int64_t num_records,
                                 arrow::MemoryPool* pool,
                                 ArrayDataPtr* array_data) const {
  arrow::Status astatus;
  std::vector<std::shared_ptr<arrow::Buffer>> buffers;

  // The output vector always has a null bitmap.
  int64_t size = arrow::bit_util::BytesForBits(num_records);
  ARROW_ASSIGN_OR_RAISE(auto bitmap_buffer, arrow::AllocateBuffer(size, pool));
  buffers.push_back(std::move(bitmap_buffer));

  // String/Binary vectors have an offsets array.
  auto type_id = type->id();
  if (arrow::is_binary_like(type_id)) {
    auto offsets_len = arrow::bit_util::BytesForBits((num_records + 1) * 32);

    ARROW_ASSIGN_OR_RAISE(auto offsets_buffer, arrow::AllocateBuffer(offsets_len, pool));
    buffers.push_back(std::move(offsets_buffer));
  }

  // The output vector always has a data array.
  int64_t data_len;
  if (arrow::is_primitive(type_id) || type_id == arrow::Type::DECIMAL) {
    const auto& fw_type = static_cast<const arrow::FixedWidthType&>(*type);
    data_len = arrow::bit_util::BytesForBits(num_records * fw_type.bit_width());
  } else if (arrow::is_binary_like(type_id)) {
    // we don't know the expected size for varlen output vectors.
    data_len = 0;
  } else {
    return Status::Invalid("Unsupported output data type " + type->ToString());
  }
  ARROW_ASSIGN_OR_RAISE(auto data_buffer, arrow::AllocateResizableBuffer(data_len, pool));

  // This is not strictly required but valgrind gets confused and detects this
  // as uninitialized memory access. See arrow::util::SetBitTo().
  if (type->id() == arrow::Type::BOOL) {
    memset(data_buffer->mutable_data(), 0, data_len);
  }
  buffers.push_back(std::move(data_buffer));

  *array_data = arrow::ArrayData::Make(type, num_records, std::move(buffers));
  return Status::OK();
}

Status Projector::ValidateEvaluateArgsCommon(const arrow::RecordBatch& batch) const {
  ARROW_RETURN_IF(!batch.schema()->Equals(*schema_),
                  Status::Invalid("Schema in RecordBatch must match schema in Make()"));
  ARROW_RETURN_IF(batch.num_rows() == 0,
                  Status::Invalid("RecordBatch must be non-empty."));

  return Status::OK();
}

Status Projector::ValidateArrayDataCapacity(const arrow::ArrayData& array_data,
                                            const arrow::Field& field,
                                            int64_t num_records) const {
  ARROW_RETURN_IF(array_data.buffers.size() < 2,
                  Status::Invalid("ArrayData must have at least 2 buffers"));

  int64_t min_bitmap_len = arrow::bit_util::BytesForBits(num_records);
  int64_t bitmap_len = array_data.buffers[0]->capacity();
  ARROW_RETURN_IF(
      bitmap_len < min_bitmap_len,
      Status::Invalid("Bitmap buffer too small for ", field.name(), " expected minimum ",
                      min_bitmap_len, " actual size ", bitmap_len));

  auto type_id = field.type()->id();
  if (arrow::is_binary_like(type_id)) {
    // validate size of offsets buffer.
    int64_t min_offsets_len = arrow::bit_util::BytesForBits((num_records + 1) * 32);
    int64_t offsets_len = array_data.buffers[1]->capacity();
    ARROW_RETURN_IF(
        offsets_len < min_offsets_len,
        Status::Invalid("offsets buffer too small for ", field.name(),
                        " minimum required ", min_offsets_len, " actual ", offsets_len));

    // check that it's resizable.
    auto resizable = dynamic_cast<arrow::ResizableBuffer*>(array_data.buffers[2].get());
    ARROW_RETURN_IF(
        resizable == nullptr,
        Status::Invalid("data buffer for varlen output vectors must be resizable"));
  } else if (arrow::is_primitive(type_id) || type_id == arrow::Type::DECIMAL) {
    // verify size of data buffer.
    const auto& fw_type = static_cast<const arrow::FixedWidthType&>(*field.type());
    int64_t min_data_len =
        arrow::bit_util::BytesForBits(num_records * fw_type.bit_width());
    int64_t data_len = array_data.buffers[1]->capacity();
    ARROW_RETURN_IF(data_len < min_data_len,
                    Status::Invalid("Data buffer too small for ", field.name()));
  } else {
    return Status::Invalid("Unsupported output data type " + field.type()->ToString());
  }

  return Status::OK();
}

const std::string& Projector::DumpIR() { return llvm_generator_->ir(); }

void Projector::SetBuiltFromCache(bool flag) { built_from_cache_ = flag; }

bool Projector::GetBuiltFromCache() { return built_from_cache_; }

void Projector::StartAsyncUpgrade(const ExpressionVector& exprs,
                                  SelectionVector::Mode mode,
                                  std::shared_ptr<Configuration> optimized_config) {
  // Build the optimized replacement on a background worker and hot-swap it in when ready.
  // The task captures a raw `this`; ~Projector waits on async_upgrade_future_ so the task
  // can never touch a destroyed projector. exprs/optimized_config are captured by value
  // (shared_ptr copies) so they stay alive regardless of the caller.
  async_cancel_ = std::make_shared<std::atomic<bool>>(false);
  auto cancel = async_cancel_;
  async_upgrade_future_ = AsyncCompilePool::Instance().Submit(
      [this, exprs, mode, optimized_config, cancel] {
        if (cancel->load(std::memory_order_relaxed)) return;  // projector already dying

        auto cache = LLVMGenerator::GetCache();
        ExpressionCacheKey cache_key(schema_, optimized_config, exprs, mode);
        std::shared_ptr<llvm::MemoryBuffer> prev_cached_obj =
            cache->GetObjectCode(cache_key);
        const bool is_cached = (prev_cached_obj != nullptr);
        GandivaObjectCache obj_cache(cache, cache_key);

        auto maybe_gen = LLVMGenerator::Make(optimized_config, is_cached, obj_cache);
        if (!maybe_gen.ok()) return;  // keep the fast unoptimized code; no swap
        std::shared_ptr<LLVMGenerator> optimized = maybe_gen.MoveValueUnsafe();

        if (!optimized->SetLLVMObjectCache(prev_cached_obj).ok()) return;
        if (!optimized->Build(exprs, mode).ok()) return;

        // Re-check: skip mutating the projector if it is being torn down. (~Projector
        // still waits on us, so the generators are alive either way.)
        if (cancel->load(std::memory_order_relaxed)) return;

        // Publish the optimized JIT pointers into the running generator (atomic per
        // mode), then retain the optimized generator: its engine owns the JIT memory
        // those pointers target.
        llvm_generator_->AdoptJITFunctions(*optimized);
        optimized_llvm_generator_ = std::move(optimized);
      });
}

}  // namespace gandiva
