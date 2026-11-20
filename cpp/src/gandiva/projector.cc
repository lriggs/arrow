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

#include <cstdlib>
#include <memory>
#include <utility>
#include <vector>

#include "arrow/util/logging.h"

#include "gandiva/cache.h"
#include "gandiva/expr_validator.h"
#include "gandiva/expression_cache_key.h"
#include "gandiva/gandiva_object_cache.h"
#include "gandiva/llvm_generator.h"

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
}  // namespace

Projector::Projector(std::unique_ptr<LLVMGenerator> llvm_generator, SchemaPtr schema,
                     const FieldVector& output_fields,
                     std::shared_ptr<Configuration> configuration)
    : llvm_generator_(std::move(llvm_generator)),
      schema_(schema),
      output_fields_(output_fields),
      configuration_(configuration) {}

Projector::~Projector() {}

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

  // Async compile (GANDIVA_ASYNC_COMPILE): when this is a cold, optimized build, compile
  // an UNoptimized version now for low latency and upgrade to -O3 in the background.
  const bool use_async =
      AsyncCompileEnabled() && !is_cached && configuration->optimize();

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
  // Increment 2: build the optimized replacement synchronously here and hot-swap it in
  // before returning. The atomic fn-ptr publish (CompiledExpr) and keep-alive handling
  // are the parts that must be correct; moving this onto a background thread is a later,
  // isolated change. Any failure leaves the fast unoptimized projector in place.
  auto cache = LLVMGenerator::GetCache();
  ExpressionCacheKey cache_key(schema_, optimized_config, exprs, mode);
  std::shared_ptr<llvm::MemoryBuffer> prev_cached_obj = cache->GetObjectCode(cache_key);
  const bool is_cached = (prev_cached_obj != nullptr);
  GandivaObjectCache obj_cache(cache, cache_key);

  auto maybe_gen = LLVMGenerator::Make(optimized_config, is_cached, obj_cache);
  if (!maybe_gen.ok()) return;
  std::shared_ptr<LLVMGenerator> optimized = maybe_gen.MoveValueUnsafe();

  if (!optimized->SetLLVMObjectCache(prev_cached_obj).ok()) return;
  if (!optimized->Build(exprs, mode).ok()) return;

  // Publish the optimized JIT pointers into the running generator (atomic per mode), then
  // retain the optimized generator: its engine owns the JIT memory those pointers target.
  llvm_generator_->AdoptJITFunctions(*optimized);
  optimized_llvm_generator_ = std::move(optimized);
}

}  // namespace gandiva
