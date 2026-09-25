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

#include "gandiva/selection_vector.h"

#include <memory>
#include <sstream>
#include <utility>
#include <vector>

#include "arrow/util/bit_util.h"
#include "arrow/util/endian.h"

#include "gandiva/selection_vector_impl.h"

namespace gandiva {

constexpr SelectionVector::Mode SelectionVector::kAllModes[kNumModes];

namespace {

// Appends the index of every set bit in bitmap (up to and including max_bitmap_index)
// through set_index. Returns the number of indices written, or -1 if more than
// max_slots bits are set.
template <typename SetIndexFn>
int64_t PopulateIndicesFromBitMap(const uint8_t* bitmap, int64_t bitmap_size,
                                  int64_t max_bitmap_index, int64_t max_slots,
                                  SetIndexFn&& set_index) {
  // jump 8-bytes at a time, add the index corresponding to each valid bit to the
  // the selection vector.
  int64_t selection_idx = 0;
  const uint64_t* bitmap_64 = reinterpret_cast<const uint64_t*>(bitmap);
  for (int64_t bitmap_idx = 0; bitmap_idx < bitmap_size / 8; ++bitmap_idx) {
    uint64_t current_word = arrow::bit_util::ToLittleEndian(bitmap_64[bitmap_idx]);

    while (current_word != 0) {
      int pos_in_word = arrow::bit_util::CountTrailingZeros(current_word);

      int64_t pos_in_bitmap = bitmap_idx * 64 + pos_in_word;
      if (pos_in_bitmap > max_bitmap_index) {
        // the bitmap may be slightly larger for alignment/padding.
        return selection_idx;
      }

      if (ARROW_PREDICT_FALSE(selection_idx >= max_slots)) {
        return -1;
      }

      set_index(selection_idx, pos_in_bitmap);
      ++selection_idx;

      // clear the lowest set bit.
      current_word &= current_word - 1;
    }
  }
  return selection_idx;
}

template <typename C_TYPE>
int64_t PopulateTypedIndicesFromBitMap(SelectionVector* selection_vector,
                                       const uint8_t* bitmap, int64_t bitmap_size,
                                       int64_t max_bitmap_index) {
  auto* raw_data =
      reinterpret_cast<C_TYPE*>(selection_vector->GetBuffer().mutable_data());
  return PopulateIndicesFromBitMap(
      bitmap, bitmap_size, max_bitmap_index, selection_vector->GetMaxSlots(),
      [raw_data](int64_t index, int64_t value) {
        raw_data[index] = static_cast<C_TYPE>(value);
      });
}

}  // namespace

Status SelectionVector::PopulateFromBitMap(const uint8_t* bitmap, int64_t bitmap_size,
                                           int64_t max_bitmap_index) {
  const uint64_t max_idx = static_cast<uint64_t>(max_bitmap_index);
  ARROW_RETURN_IF(bitmap_size % 8, Status::Invalid("Bitmap size ", bitmap_size,
                                                   " must be aligned to 64-bit size"));
  ARROW_RETURN_IF(max_bitmap_index < 0,
                  Status::Invalid("Max bitmap index must be positive"));
  ARROW_RETURN_IF(
      max_idx > GetMaxSupportedValue(),
      Status::Invalid("max_bitmap_index ", max_idx, " must be <= maxSupportedValue ",
                      GetMaxSupportedValue(), " in selection vector"));

  // Write the indices straight into the buffer when possible, avoiding a virtual
  // SetIndex() call per selected row.
  auto generic_set_index = [this](int64_t index, int64_t value) {
    SetIndex(index, value);
  };
  const auto mode = GetBuffer().is_mutable() ? GetMode() : MODE_NONE;
  int64_t num_slots;
  switch (mode) {
    case MODE_UINT16:
      num_slots = PopulateTypedIndicesFromBitMap<uint16_t>(this, bitmap, bitmap_size,
                                                           max_bitmap_index);
      break;
    case MODE_UINT32:
      num_slots = PopulateTypedIndicesFromBitMap<uint32_t>(this, bitmap, bitmap_size,
                                                           max_bitmap_index);
      break;
    case MODE_UINT64:
      num_slots = PopulateTypedIndicesFromBitMap<uint64_t>(this, bitmap, bitmap_size,
                                                           max_bitmap_index);
      break;
    default:
      num_slots = PopulateIndicesFromBitMap(bitmap, bitmap_size, max_bitmap_index,
                                            GetMaxSlots(), generic_set_index);
      break;
  }
  ARROW_RETURN_IF(num_slots < 0,
                  Status::Invalid("selection vector has no remaining slots"));

  SetNumSlots(num_slots);
  return Status::OK();
}

Status SelectionVector::MakeInt16(int64_t max_slots,
                                  std::shared_ptr<arrow::Buffer> buffer,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  ARROW_RETURN_NOT_OK(SelectionVectorInt16::ValidateBuffer(max_slots, buffer));
  *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt16(int64_t max_slots, arrow::MemoryPool* pool,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_RETURN_NOT_OK(SelectionVectorInt16::AllocateBuffer(max_slots, pool, &buffer));
  *selection_vector = std::make_shared<SelectionVectorInt16>(max_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeImmutableInt16(
    int64_t num_slots, std::shared_ptr<arrow::Buffer> buffer,
    std::shared_ptr<SelectionVector>* selection_vector) {
  *selection_vector =
      std::make_shared<SelectionVectorInt16>(num_slots, num_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt32(int64_t max_slots,
                                  std::shared_ptr<arrow::Buffer> buffer,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  ARROW_RETURN_NOT_OK(SelectionVectorInt32::ValidateBuffer(max_slots, buffer));
  *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);

  return Status::OK();
}

Status SelectionVector::MakeInt32(int64_t max_slots, arrow::MemoryPool* pool,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_RETURN_NOT_OK(SelectionVectorInt32::AllocateBuffer(max_slots, pool, &buffer));
  *selection_vector = std::make_shared<SelectionVectorInt32>(max_slots, buffer);

  return Status::OK();
}

Status SelectionVector::MakeImmutableInt32(
    int64_t num_slots, std::shared_ptr<arrow::Buffer> buffer,
    std::shared_ptr<SelectionVector>* selection_vector) {
  *selection_vector =
      std::make_shared<SelectionVectorInt32>(num_slots, num_slots, buffer);
  return Status::OK();
}

Status SelectionVector::MakeInt64(int64_t max_slots,
                                  std::shared_ptr<arrow::Buffer> buffer,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  ARROW_RETURN_NOT_OK(SelectionVectorInt64::ValidateBuffer(max_slots, buffer));
  *selection_vector = std::make_shared<SelectionVectorInt64>(max_slots, buffer);

  return Status::OK();
}

Status SelectionVector::MakeInt64(int64_t max_slots, arrow::MemoryPool* pool,
                                  std::shared_ptr<SelectionVector>* selection_vector) {
  std::shared_ptr<arrow::Buffer> buffer;
  ARROW_RETURN_NOT_OK(SelectionVectorInt64::AllocateBuffer(max_slots, pool, &buffer));
  *selection_vector = std::make_shared<SelectionVectorInt64>(max_slots, buffer);

  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE, SelectionVector::Mode mode>
Status SelectionVectorImpl<C_TYPE, A_TYPE, mode>::AllocateBuffer(
    int64_t max_slots, arrow::MemoryPool* pool, std::shared_ptr<arrow::Buffer>* buffer) {
  auto buffer_len = max_slots * sizeof(C_TYPE);
  ARROW_ASSIGN_OR_RAISE(*buffer, arrow::AllocateBuffer(buffer_len, pool));

  return Status::OK();
}

template <typename C_TYPE, typename A_TYPE, SelectionVector::Mode mode>
Status SelectionVectorImpl<C_TYPE, A_TYPE, mode>::ValidateBuffer(
    int64_t max_slots, std::shared_ptr<arrow::Buffer> buffer) {
  ARROW_RETURN_IF(!buffer->is_mutable(),
                  Status::Invalid("buffer for selection vector must be mutable"));

  const int64_t min_len = max_slots * sizeof(C_TYPE);
  ARROW_RETURN_IF(buffer->size() < min_len,
                  Status::Invalid("Buffer for selection vector is too small"));

  return Status::OK();
}

}  // namespace gandiva
