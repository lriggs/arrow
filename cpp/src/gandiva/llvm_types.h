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

#pragma once

#include <iostream>
#include <map>
#include <vector>

#include "arrow/util/logging.h"
#include "gandiva/arrow.h"
#include "gandiva/llvm_includes.h"
#include "gandiva/visibility.h"

namespace gandiva {

/// \brief Holder for llvm types, and mappings between arrow types and llvm types.
class GANDIVA_EXPORT LLVMTypes {
 public:
  explicit LLVMTypes(llvm::LLVMContext& context);

  llvm::Type* void_type() { return llvm::Type::getVoidTy(context_); }

  llvm::Type* i1_type() { return llvm::Type::getInt1Ty(context_); }

  llvm::Type* i8_type() { return llvm::Type::getInt8Ty(context_); }

  llvm::Type* i16_type() { return llvm::Type::getInt16Ty(context_); }

  llvm::Type* i32_type() { return llvm::Type::getInt32Ty(context_); }

  llvm::Type* i64_type() { return llvm::Type::getInt64Ty(context_); }

  llvm::Type* i128_type() { return llvm::Type::getInt128Ty(context_); }

  llvm::StructType* struct_type() {
    return llvm::StructType::get(context_, {double_type(), double_type()}, false);
  }

  llvm::VectorType* list_type() { return llvm::ScalableVectorType::get(i8_type(), (unsigned int)0); }

  llvm::StructType* i128_split_type() {
    // struct with high/low bits (see decimal_ops.cc:DecimalSplit)
    return llvm::StructType::get(context_, {i64_type(), i64_type()}, false);
  }

  llvm::Type* float_type() { return llvm::Type::getFloatTy(context_); }

  llvm::Type* double_type() { return llvm::Type::getDoubleTy(context_); }

  llvm::PointerType* ptr_type(llvm::Type* type) { return type->getPointerTo(); }

  llvm::PointerType* i1_ptr_type() { return ptr_type(i1_type()); }

  llvm::PointerType* i8_ptr_type() { return ptr_type(i8_type()); }

  llvm::PointerType* i32_ptr_type() { return ptr_type(i32_type()); }

  llvm::PointerType* i64_ptr_type() { return ptr_type(i64_type()); }

  llvm::PointerType* i128_ptr_type() { return ptr_type(i128_type()); }

  llvm::PointerType* float_ptr_type() { return ptr_type(float_type()); }

  llvm::PointerType* double_ptr_type() { return ptr_type(double_type()); }

  template <typename ctype, size_t N = (sizeof(ctype) * CHAR_BIT)>
  llvm::Constant* int_constant(ctype val) {
    return llvm::ConstantInt::get(context_, llvm::APInt(N, val));
  }

  llvm::Constant* i1_constant(bool val) { return int_constant<bool, 1>(val); }
  llvm::Constant* i8_constant(int8_t val) { return int_constant(val); }
  llvm::Constant* i16_constant(int16_t val) { return int_constant(val); }
  llvm::Constant* i32_constant(int32_t val) { return int_constant(val); }
  llvm::Constant* i64_constant(int64_t val) { return int_constant(val); }
  llvm::Constant* i128_constant(int64_t val) { return int_constant<int64_t, 128>(val); }

  llvm::Constant* true_constant() { return i1_constant(true); }
  llvm::Constant* false_constant() { return i1_constant(false); }

  llvm::Constant* i128_zero() { return i128_constant(0); }
  llvm::Constant* i128_one() { return i128_constant(1); }

  llvm::Constant* float_constant(float val) {
    return llvm::ConstantFP::get(float_type(), val);
  }

  llvm::LLVMContext* get_context() {
    return &context_;
  }

  llvm::Constant* double_constant(double val) {
    return llvm::ConstantFP::get(double_type(), val);
  }

  llvm::Constant* NullConstant(llvm::Type* type) {
    if (type->isIntegerTy()) {
      return llvm::ConstantInt::get(type, 0);
    } else if (type->isFloatingPointTy()) {
      return llvm::ConstantFP::get(type, 0);
    } else {
      DCHECK(type->isPointerTy());
      return llvm::ConstantPointerNull::getNullValue(type);
    }
  }

  /// For a given data type, find the ir type used for the data vector slot.
  llvm::Type* DataVecType(const DataTypePtr& data_type) {
    // support list type
    // list type data is formed by base type buffer, wrapped with offsets buffer
    // offsets buffer is to separate data into list
    // not support nested list
    if (data_type->id() == arrow::Type::LIST) {
      //LR TODO
      std::cout << "LR Returning list type as type " << data_type->field(0)->type()->id()<< " for IR " << std::endl;
      return IRType(data_type->field(0)->type()->id());
      //return IRType(data_type->id());
      //return i32_ptr_type();
    }
    return IRType(data_type->id());
  }

  /// For a given minor type, find the corresponding ir type.
  llvm::Type* IRType(arrow::Type::type arrow_type) {
    auto found = arrow_id_to_llvm_type_map_.find(arrow_type);
    return (found == arrow_id_to_llvm_type_map_.end()) ? NULL : found->second;
  }

  std::vector<arrow::Type::type> GetSupportedArrowTypes() {
    std::vector<arrow::Type::type> retval;
    for (auto const& element : arrow_id_to_llvm_type_map_) {
      retval.push_back(element.first);
    }
    return retval;
  }

 private:
  std::map<arrow::Type::type, llvm::Type*> arrow_id_to_llvm_type_map_;

  llvm::LLVMContext& context_;
};

}  // namespace gandiva
