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

#include "gandiva/array_ops.h"

#include <bitset>
#include <iostream>
#include <string>

#include "arrow/util/value_parsing.h"

#include "gandiva/gdv_function_stubs.h"
#include "gandiva/engine.h"
#include "gandiva/exported_funcs.h"

/// Stub functions that can be accessed from LLVM or the pre-compiled library.

extern "C" {

bool array_utf8_contains_utf8(int64_t context_ptr, const char* entry_buf,
                              int32_t* entry_child_offsets, int32_t entry_offsets_len,
                              const char* contains_data, int32_t contains_data_length) {
  for (int i = 0; i < entry_offsets_len; i++) {
    int32_t entry_len = *(entry_child_offsets + i + 1) - *(entry_child_offsets + i);
    if (entry_len != contains_data_length) {
      entry_buf = entry_buf + entry_len;
      continue;
    }
    if (strncmp(entry_buf, contains_data, contains_data_length) == 0) {
      return true;
    }
    entry_buf = entry_buf + entry_len;
  }
  return false;
}

bool array_int32_contains_int32(int64_t context_ptr, const int32_t* entry_buf,
                              int32_t entry_offsets_len,
                              int32_t contains_data) {
  //std::cout << "LR array_int32_contains_int32 offset length=" << entry_offsets_len << std::endl;
  for (int i = 0; i < entry_offsets_len; i++) {
    //std::cout << "LR going to check " << entry_buf + i << std::endl;
    //LR TODO 
    int32_t entry_len = *(entry_buf + i);
    //coming as int64 for some reason. *2
    //int32_t entry_len = *(entry_buf + (i * 2));
    //std::cout << "LR checking value " << entry_len << " against target " << contains_data << std::endl;
    if (entry_len == contains_data) {
      return true;
    }
  }
  return false;
}

bool array_int64_contains_int64(int64_t context_ptr, const int64_t* entry_buf,
                              int32_t entry_offsets_len,
                              int64_t contains_data) {
  //std::cout << "LR array_int64_contains_int64 offset length=" << entry_offsets_len << std::endl;
  for (int i = 0; i < entry_offsets_len; i++) {
    //std::cout << "LR going to check " << entry_buf + i << std::endl;
    int64_t entry_len = *(entry_buf + (i*2));  //LR TODO sizeof int64?
    //std::cout << "LR checking value " << entry_len << " against target " << contains_data << std::endl;
    if (entry_len == contains_data) {
      return true;
    }
  }
  return false;
}


int32_t* array_int32_make_array(int64_t context_ptr, int32_t contains_data, int32_t* out_len) {
  //std::cout << "LR array_int32_make_array offset data=" << contains_data << std::endl;

  int integers[] = { contains_data, 21, 3, contains_data, 5 };
  *out_len = 5;// * 4;
  //length is number of items, but buffers must account for byte size.
  uint8_t* ret = gdv_fn_context_arena_malloc(context_ptr, *out_len * 4);
  memcpy(ret, integers, *out_len * 4);
  //std::cout << "LR made a buffer length" << *out_len * 4 << " item 3 is = " << int32_t(ret[3*4]) << std::endl; 

  
  //return reinterpret_cast<int32_t*>(ret);
  return reinterpret_cast<int32_t*>(ret);
}
/*
int32_t* array_int32_remove(int64_t context_ptr, const int32_t* entry_buf,
                              int32_t entry_offsets_len, int32_t remove_data, int32_t* out_len) {
  //std::cout << "LR array_int32_remove data=" << remove_data 
  //  << " entry_offsets_len " << entry_offsets_len << std::endl;

  //LR sizes are HACK
  int* integers = new int[5];
  int j = 0;
  for (int i = 0; i < entry_offsets_len; i++) {
    //std::cout << "LR going to check " << entry_buf + i << std::endl;
    int32_t entry_len = *(entry_buf + (i * 1));
    //std::cout << "LR checking value " << entry_len << " against target " << remove_data << std::endl;
    if (entry_len == remove_data) {
      continue;
    } else {
      integers[j++] = entry_len;
    }
  }

  *out_len = 5;// * 4;
  //length is number of items, but buffers must account for byte size.
  uint8_t* ret = gdv_fn_context_arena_malloc(context_ptr, *out_len * 4);
  memcpy(ret, integers, *out_len * 4);
  //std::cout << "LR made a buffer length" << *out_len * 4 << " item 3 is = " << int32_t(ret[3*4]) << std::endl; 

  delete [] integers;
  //return reinterpret_cast<int32_t*>(ret);
  return reinterpret_cast<int32_t*>(ret);
}
*/




int32_t* array_int32_remove(int64_t context_ptr, const int32_t* entry_buf,
                              int32_t entry_offsets_len, const int32_t* notSureWhatThisIs, bool combined_validity,
                              int32_t remove_data, bool entry_validWhat, 
                              /*const int32_t* array_valid_bits,*/ int64_t loop_var, int64_t validity_index_var, const int64_t* offsets,
                              bool* valid_buf, int32_t* out_len, int32_t** valid_ptr) {
  //std::cout << "LR array_int32_remove data=" << remove_data 
  //  << " entry_offsets_len " << entry_offsets_len << std::endl;

  std::cout << "LR array_int32_remove " << loop_var << std::endl;
  std::vector<int> newInts;
  
  
  /*std::bitset<8> validBits(*entry_valid);  //LR TODO handle size.
  std::bitset<8> outputValidBits;
  std::cout << "LR Entry bitset is " << validBits << std::endl;
  for (int i = 0; i < entry_offsets_len; i++) {
    //std::cout << "LR going to check " << entry_buf + i << std::endl;
    int32_t entry_item = *(entry_buf + (i * 1));
    //std::cout << "LR checking value " << entry_len << " against target " << remove_data << std::endl;
    if (entry_item == remove_data) {
      continue;
    } else if (!validBits[i]) {
      outputValidBits[i] = 0;
      newInts.push_back(0);  //This will be marked invalid, so data doesn't matter.
    } else {
      outputValidBits[i] = 1;
      //Note the vector can have n elements, while validbits might have n+1.
      newInts.push_back(entry_item);
    }
  }*/

  std::cout << "LR entry_buf=" << entry_buf << " *entry_buf=" << entry_buf << std::endl;
  std::cout << "LR notSureWhatThisIs=" << notSureWhatThisIs << " *notSureWhatThisIs=" << *notSureWhatThisIs << std::endl;
  std::cout << "LR combined_validity=" << combined_validity << " entry_validWhat=" << entry_validWhat << " validity_index_var=" << validity_index_var << std::endl;
  //<< " *notSureWhatThisIs=" << *notSureWhatThisIs << std::endl;
  const int32_t* notSureWhatThisIsAdjusted = notSureWhatThisIs - (loop_var );
  std::bitset<15> maybeInputBits (*notSureWhatThisIsAdjusted);
  std::cout << "LR maybeInputBits=" << maybeInputBits << std::endl;


  int64_t validityBitIndex = 0;
  //for (int i = 0; i < loop_var; i++) {
  //  validityBitIndex += *(offsets + i);
  //  std::cout << "LR i=" << i << " adding offset " << *(offsets + i) << " offset is " << offsets << std::endl;
  //}
validityBitIndex = validity_index_var - entry_offsets_len;
  //TODO temp until the buffer is worked out.
  //validityBitIndex -= (loop_var);
  

  std::cout << "Using validityBitIndex=" << validityBitIndex << std::endl;



  entry_validWhat = true;
  //std::bitset<10> outputValidBits;
  
  std::vector<bool> outValid;
  for (int i = 0; i < entry_offsets_len; i++) {
    //std::cout << "LR going to check " << entry_buf + i << std::endl;
    int32_t entry_item = *(entry_buf + (i * 1));
    //std::cout << "LR checking value " << entry_len << " against target " << remove_data << std::endl;
    if (entry_item == remove_data) {
      outValid.push_back(false);
      newInts.push_back(42);
      //outputValidBits[i] = 0;
       entry_validWhat = false;
    //TODO temp until buffer is worked out } else if (!arrow::bit_util::GetBit(reinterpret_cast<const uint8_t*>(array_valid_bits), validityBitIndex + i)) {
      } else if (!arrow::bit_util::GetBit(reinterpret_cast<const uint8_t*>(notSureWhatThisIsAdjusted), validityBitIndex + i)) {
      outValid.push_back(false);
      newInts.push_back(0);
      //outputValidBits[i] = 0; 
    } else {
      outValid.push_back(true);
      //Note the vector can have n elements, while validbits might have n+1.
      newInts.push_back(entry_item);
      //outputValidBits[i] = 1;
    }
  }

  *out_len = (int)newInts.size();

  //Since this function can remove values we don't know the length ahead of time.
  //LR TODO divide by 8 and ensure at least 1?
  uint8_t* validRet = gdv_fn_context_arena_malloc(context_ptr, *out_len);
  for (int i = 0; i < outValid.size(); i++) {
    arrow::bit_util::SetBitTo(validRet, i, outValid[i]);
  }

  int32_t outBufferLength = (int)*out_len * sizeof(int);
  //length is number of items, but buffers must account for byte size.
  uint8_t* ret = gdv_fn_context_arena_malloc(context_ptr, outBufferLength);
  memcpy(ret, newInts.data(), outBufferLength);
  //std::cout << "LR made a buffer length" << *out_len * 4 << " item 3 is = " << int32_t(ret[3*4]) << std::endl; 


  *valid_buf = true;


  //unsigned long ll = outputValidBits.to_ulong();
  if (!combined_validity) {
    //ll = 0;
    *out_len = 0;
    *valid_buf = false;  //this one is what works for the top level validity.
    entry_validWhat = false;
  }
  //LR no need, set along the way. memcpy(validRet, &ll, 1);
  //*valid_len = 1;
  std::cout << "LR valid_buf is " << valid_buf << std::endl;
  //std::cout << "LR outputValidBits is " << outputValidBits << std::endl;
  //valid_buf = reinterpret_cast<bool*>(validRet);

  *valid_ptr = reinterpret_cast<int32_t*>(validRet);
  std::cout << "LR setting valid_ptr=" << valid_ptr << " *valid_ptr=" << *valid_ptr << " **valid_ptr=" << **valid_ptr << " valid_ptr bitset data is " << std::bitset<8>(**valid_ptr) 
    << " return value is " << reinterpret_cast<int32_t*>(ret) << std::endl;


  //return reinterpret_cast<int32_t*>(ret);
  return reinterpret_cast<int32_t*>(ret);
}

int64_t array_utf8_length(int64_t context_ptr, const char* entry_buf,
                          int32_t* entry_child_offsets, int32_t entry_offsets_len) {
  int64_t res = entry_offsets_len;
  return res;
}
}

namespace gandiva {
void ExportedArrayFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  args = {types->i64_type(),      // int64_t execution_context
          types->i8_ptr_type(),   // int8_t* data ptr
          types->i32_ptr_type(),  // int32_t* child offsets ptr
          types->i32_type()};     // int32_t child offsets length

  engine->AddGlobalMappingForFunc("array_utf8_length", types->i64_type() /*return_type*/,
                                  args, reinterpret_cast<void*>(array_utf8_length));

  args = {types->i64_type(),      // int64_t execution_context
          types->i8_ptr_type(),   // int8_t* data ptr
          types->i32_ptr_type(),  // int32_t* child offsets ptr
          types->i32_type(),      // int32_t child offsets length
          types->i8_ptr_type(),   // const char* contains data buf
          types->i32_type()};     // int32_t contains data length

  engine->AddGlobalMappingForFunc("array_utf8_contains_utf8",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(array_utf8_contains_utf8));

  args = {types->i64_type(),      // int64_t execution_context
          types->i32_ptr_type(),   // int8_t* data ptr
          types->i32_type(),      // int32_t child offsets length
          types->i32_type()};     // int32_t contains data length

  engine->AddGlobalMappingForFunc("array_int32_contains_int32",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(array_int32_contains_int32));

  args = {types->i64_type(),      // int64_t execution_context
          types->i64_ptr_type(),   // int8_t* data ptr
          types->i32_type(),      // int32_t child offsets length
          types->i64_type()};     // int32_t contains data length

  engine->AddGlobalMappingForFunc("array_int64_contains_int64",
                                  types->i1_type() /*return_type*/, args,
                                  reinterpret_cast<void*>(array_int64_contains_int64));


  args = {types->i64_type(),      // int64_t execution_context
          types->i32_type(),   // array item input
          types->i32_ptr_type()};     // out array length

  engine->AddGlobalMappingForFunc("array_int32_make_array",
                                  types->i32_ptr_type(), args,
                                  reinterpret_cast<void*>(array_int32_make_array));

  args = {types->i64_type(),      // int64_t execution_context
          types->i32_ptr_type(),   // int8_t* data ptr
          types->i32_type(),      // int32_t child offsets length
          types->i32_ptr_type(),   // Not Sure???
          types->i1_type(),   // bool validity
          types->i32_type(),      //value to remove from input
          types->i1_type(),   // bool validity
          //types->i32_ptr_type(), //in validity bitmap
          types->i64_type(),      //in loop var
          types->i64_type(),      //in validity_index_var
          types->i64_ptr_type(), //in offsets
          types->i1_ptr_type(),   //valid buffer
          types->i32_ptr_type(),   // out array length
          types->i32_ptr_type()  //valid_ptr
          
        };

  engine->AddGlobalMappingForFunc("array_int32_remove",
                                  types->i32_ptr_type(), args,
                                  reinterpret_cast<void*>(array_int32_remove));
}
}  // namespace gandiva
