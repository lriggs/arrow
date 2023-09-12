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

#include "gandiva/annotator.h"

#include <iostream>
#include <memory>
#include <string>

#include "gandiva/field_descriptor.h"

namespace gandiva {

FieldDescriptorPtr Annotator::CheckAndAddInputFieldDescriptor(FieldPtr field) {
  // If the field is already in the map, return the entry.
  auto found = in_name_to_desc_.find(field->name());
  if (found != in_name_to_desc_.end()) {
    return found->second;
  }

  auto desc = MakeDesc(field, false /*is_output*/);
  in_name_to_desc_[field->name()] = desc;
  return desc;
}

FieldDescriptorPtr Annotator::AddOutputFieldDescriptor(FieldPtr field) {
  auto desc = MakeDesc(field, true /*is_output*/);
  out_descs_.push_back(desc);
  return desc;
}

FieldDescriptorPtr Annotator::MakeDesc(FieldPtr field, bool is_output) {
  int data_idx = buffer_count_++;
  int validity_idx = buffer_count_++;
  int offsets_idx = FieldDescriptor::kInvalidIdx;
  int child_offsets_idx = FieldDescriptor::kInvalidIdx;
  if (arrow::is_binary_like(field->type()->id())) {
    offsets_idx = buffer_count_++;
  }

  if (field->type()->id() == arrow::Type::LIST) {
    //std::cout << "LR Annotator::MakeDesc 1" << std::endl;
    offsets_idx = buffer_count_++;
    if (arrow::is_binary_like(field->type()->field(0)->type()->id())) {
      child_offsets_idx = buffer_count_++;
    }
  }
  int data_buffer_ptr_idx = FieldDescriptor::kInvalidIdx;
  if (is_output) {
    data_buffer_ptr_idx = buffer_count_++;
  }
  return std::make_shared<FieldDescriptor>(field, data_idx, validity_idx, offsets_idx,
                                           data_buffer_ptr_idx, child_offsets_idx);
}

int Annotator::AddHolderPointer(void* holder) {
  int size = static_cast<int>(holder_pointers_.size());
  holder_pointers_.push_back(holder);
  return size;
}

void Annotator::PrepareBuffersForField(const FieldDescriptor& desc,
                                       const arrow::ArrayData& array_data,
                                       EvalBatch* eval_batch, bool is_output) const {
  int buffer_idx = 0;

  // The validity buffer is optional. Use nullptr if it does not have one.
  if (array_data.buffers[buffer_idx]) {
    uint8_t* validity_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
    std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -6 " << &validity_buf << std::endl;
    eval_batch->SetBuffer(desc.validity_idx(), validity_buf, array_data.offset);
  } else {
    std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -5 null " << std::endl;
    eval_batch->SetBuffer(desc.validity_idx(), nullptr, array_data.offset);
  }
  ++buffer_idx;

  if (desc.HasOffsetsIdx()) {
    uint8_t* offsets_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
    std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -4 " << &offsets_buf << std::endl;
    eval_batch->SetBuffer(desc.offsets_idx(), offsets_buf, array_data.offset);

    if (desc.HasChildOffsetsIdx()) {
      //std::cout << "LR Annotator::PrepareBuffersForField 1 for field " << desc.Name() << " type is " << array_data.type->id() << std::endl;
      if (is_output) {
        // if list field is output field, we should put buffer pointer into eval batch
        // for resizing
        uint8_t* child_offsets_buf = reinterpret_cast<uint8_t*>(
            array_data.child_data.at(0)->buffers[buffer_idx].get());
        std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -3 " << &child_offsets_buf << std::endl;
        eval_batch->SetBuffer(desc.child_data_offsets_idx(), child_offsets_buf,
                              array_data.child_data.at(0)->offset);
      } else {
        //std::cout << "LR Annotator::PrepareBuffersForField 2" << std::endl;
        // if list field is input field, just put buffer data into eval batch
        uint8_t* child_offsets_buf = const_cast<uint8_t*>(
            array_data.child_data.at(0)->buffers[buffer_idx]->data());
        std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -2 " << &child_offsets_buf << std::endl;
        eval_batch->SetBuffer(desc.child_data_offsets_idx(), child_offsets_buf,
                              array_data.child_data.at(0)->offset);
      }
    }
    if (array_data.type->id() != arrow::Type::LIST ||
        arrow::is_binary_like(array_data.type->field(0)->type()->id())) {
          //std::cout << "LR Annotator::PrepareBuffersForField 3" << std::endl;
        
        // primitive type list data buffer index is 1
        // binary like type list data buffer index is 2
        ++buffer_idx;
        }
  }

  if (array_data.type->id() != arrow::Type::LIST) {
    //std::cout << "LR Annotator::PrepareBuffersForField 4" << std::endl;

    //std::cout << "LR Annotator::PrepareBuffersForField 4 buffer_idx " << buffer_idx << std::endl;
    uint8_t* data_buf = const_cast<uint8_t*>(array_data.buffers[buffer_idx]->data());
    //std::cout << "LR Annotator::PrepareBuffersForField 4a" << std::endl;
    std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer -1 " << &data_buf << std::endl;
    eval_batch->SetBuffer(desc.data_idx(), data_buf, array_data.offset);
    //std::cout << "LR Annotator::PrepareBuffersForField 4b" << std::endl;
  } else {
    //std::cout << "LR Annotator::PrepareBuffersForField 5 " << desc.Name() << " buffer_idx " << buffer_idx << std::endl;
    //std::cout << "LR Annotator::PrepareBuffersForField 5 array_data child size " << array_data.child_data.size() << std::endl;
    
    uint8_t* data_buf =
        const_cast<uint8_t*>(array_data.child_data.at(0)->buffers[buffer_idx]->data());
    std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer 0 " << &data_buf << std::endl;
    eval_batch->SetBuffer(desc.data_idx(), data_buf, array_data.child_data.at(0)->offset);
    //std::cout << "LR Annotator::PrepareBuffersForField 5a" << std::endl;
  }

  if (is_output) {
    // pass in the Buffer object for output data buffers. Can be used for resizing.

    if (array_data.type->id() != arrow::Type::LIST) {
      uint8_t* data_buf_ptr =
          reinterpret_cast<uint8_t*>(array_data.buffers[buffer_idx].get());
      std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer 1 " << &data_buf_ptr << std::endl;
      eval_batch->SetBuffer(desc.data_buffer_ptr_idx(), data_buf_ptr, array_data.offset);
    } else {
        //std::cout << "LR Annotator::PrepareBuffersForField is_output index " << desc.data_buffer_ptr_idx() << std::endl;
  
      // list data buffer is in child data buffer
      uint8_t* data_buf_ptr = reinterpret_cast<uint8_t*>(
          array_data.child_data.at(0)->buffers[buffer_idx].get());
      std::cout << "LR Annotator::PrepareBuffersForField setting eval buffer 2 " << &data_buf_ptr << std::endl;
  
      eval_batch->SetBuffer(desc.data_buffer_ptr_idx(), data_buf_ptr,
                            array_data.child_data.at(0)->offset);
    }
  }
  
}

EvalBatchPtr Annotator::PrepareEvalBatch(const arrow::RecordBatch& record_batch,
                                         const ArrayDataVector& out_vector) const {
  EvalBatchPtr eval_batch = std::make_shared<EvalBatch>(
      record_batch.num_rows(), buffer_count_, local_bitmap_count_);

  //std::cout << "LR PrepareEvalBatch 1" << std::endl;
  // Fill in the entries for the input fields.
  for (int i = 0; i < record_batch.num_columns(); ++i) {
    const std::string& name = record_batch.column_name(i);
    auto found = in_name_to_desc_.find(name);
    if (found == in_name_to_desc_.end()) {
      // skip columns not involved in the expression.
      continue;
    }

    /*std::cout << "LR PrepareEvalBatch 1a i=" << i << " record batch schema " << record_batch.schema()->ToString() 
    << "  num rows " << record_batch.num_rows()
    << " num columns " << record_batch.num_columns()
    << " data size " << record_batch.column_data().size()
    << " col 1 " << record_batch.column(0)->ToString()
    << std::endl;*/
    
    //std::cout << "LR PrepareEvalBatch 1a i=" << i << " record batch data " << record_batch.ToString() << std::endl;
    PrepareBuffersForField(*(found->second), *(record_batch.column_data(i)),
                           eval_batch.get(), false /*is_output*/);
  }

  // Fill in the entries for the output fields.
  //std::cout << "LR PrepareEvalBatch preparing output fields" << std::endl;
  int idx = 0;
  for (auto& arraydata : out_vector) {
    const FieldDescriptorPtr& desc = out_descs_.at(idx);
    PrepareBuffersForField(*desc, *arraydata, eval_batch.get(), true /*is_output*/);
    ++idx;
  }
  //std::cout << "LR PrepareEvalBatch 2" << std::endl;
  return eval_batch;
}

}  // namespace gandiva
