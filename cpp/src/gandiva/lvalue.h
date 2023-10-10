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
#include <vector>

#include "arrow/util/macros.h"

#include "arrow/util/logging.h"
#include "gandiva/llvm_includes.h"

namespace gandiva {

/// \brief Tracks validity/value builders in LLVM.
class GANDIVA_EXPORT LValue {
 public:
  explicit LValue(llvm::Value* data, llvm::Value* length = NULLPTR,
                  llvm::Value* validity = NULLPTR)
      : data_(data), length_(length), validity_(validity) {
        std::cout << "LR created LValue " << to_string() << std::endl;
      }
  virtual ~LValue() = default;

  llvm::Value* data() { return data_; }
  llvm::Value* length() { return length_; }
  llvm::Value* validity() { return validity_; }

  void set_data(llvm::Value* data) { data_ = data; }

  // Append the params required when passing this as a function parameter.
  virtual void AppendFunctionParams(std::vector<llvm::Value*>* params) {
    std::cout << "LR LValue::AppendFunctionParams" << std::endl;
    params->push_back(data_);
    if (length_ != NULLPTR) {
      params->push_back(length_);
    }
    if (validity_ != NULLPTR) {
      params->push_back(validity_);
    }
  }

  virtual std::string to_string() {
    std::string s = "Base LValue";
    
    std::string str1 = "data:";
    if (data_) {
      llvm::raw_string_ostream output1(str1);
      data_->print(output1);
    }

    std::string str2 = "length:";
    if (length_) {
      llvm::raw_string_ostream output2(str2);
      length_->print(output2);
    }

    std::string str3 = "validity:";
    if (validity_) {
      llvm::raw_string_ostream output3(str3);
      validity_->print(output3);
    }

    return s + "\n" + str1 + "\n" + str2 + "\n" + str3;
  }

 protected:
  llvm::Value* data_;
  llvm::Value* length_;
  llvm::Value* validity_;
};

class GANDIVA_EXPORT DecimalLValue : public LValue {
 public:
  DecimalLValue(llvm::Value* data, llvm::Value* validity, llvm::Value* precision,
                llvm::Value* scale)
      : LValue(data, NULLPTR, validity), precision_(precision), scale_(scale) {}

  llvm::Value* precision() { return precision_; }
  llvm::Value* scale() { return scale_; }

  void AppendFunctionParams(std::vector<llvm::Value*>* params) override {
    LValue::AppendFunctionParams(params);
    params->push_back(precision_);
    params->push_back(scale_);
  }

 private:
  llvm::Value* precision_;
  llvm::Value* scale_;
};

class GANDIVA_EXPORT ListLValue : public LValue {
 public:
  ListLValue(llvm::Value* data, llvm::Value* child_offsets, llvm::Value* offsets_length,
             llvm::Value* validity = NULLPTR)
      : LValue(data, NULLPTR, validity),
        child_offsets_(child_offsets),
        offsets_length_(offsets_length) {
          //std::cout << "LR Creating ListLValue " << std::endl;
        }

  llvm::Value* child_offsets() { return child_offsets_; }

  llvm::Value* offsets_length() { return offsets_length_; }

  void AppendFunctionParams(std::vector<llvm::Value*>* params) override {
    std::cout << "LR ListLValue::AppendFunctionParams" << std::endl;
    LValue::AppendFunctionParams(params);
    params->push_back(child_offsets_);
    params->push_back(offsets_length_);
    params->push_back(validity_);
  }

  virtual std::string to_string() override {
    std::string s = "List LValue";
    
    s += " " + LValue::to_string();


    std::string str1 = "child_offsets_:";
    if (child_offsets_) {
      llvm::raw_string_ostream output1(str1);
      child_offsets_->print(output1);
    }

    std::string str2 = "offsets_length_:";
    if (offsets_length_) {
      llvm::raw_string_ostream output2(str2);
      offsets_length_->print(output2);
    }

    return s + "\n" + str1 + "\n" + str2;
  }

 private:
  llvm::Value* child_offsets_;
  llvm::Value* offsets_length_;
};

}  // namespace gandiva
