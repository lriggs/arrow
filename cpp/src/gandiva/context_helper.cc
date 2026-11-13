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

#include "gandiva/execution_context.h"
#include "gandiva/gdv_function_stubs.h"

// Slow path for the pre-compiled arena allocator (precompiled/arena_ops.cc); defined
// below and registered as an external mapping.
extern "C" uint8_t* gdv_fn_context_arena_malloc_slow(int64_t context_ptr, int32_t size);

// This file is also used in the pre-compiled unit tests, which do include
// llvm/engine/..
#ifndef GANDIVA_UNIT_TEST
#  include "gandiva/exported_funcs.h"

#  include "gandiva/engine.h"

namespace gandiva {

arrow::Status ExportedContextFunctions::AddMappings(Engine* engine) const {
  std::vector<llvm::Type*> args;
  auto types = engine->types();

  // gdv_fn_context_set_error_msg
  args = {types->i64_type(),      // int64_t context_ptr
          types->i8_ptr_type()};  // char const* err_msg

  engine->AddGlobalMappingForFunc("gdv_fn_context_set_error_msg", types->void_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_context_set_error_msg));

  // gdv_fn_context_arena_malloc's bump fast path is pre-compiled IR
  // (precompiled/arena_ops.cc) so it inlines into generated code; only the slow path
  // (chunk allocation, which needs arrow::Status / the memory pool) is an external stub.
  args = {types->i64_type(),   // int64_t context_ptr
          types->i32_type()};  // int32_t size
  engine->AddGlobalMappingForFunc(
      "gdv_fn_context_arena_malloc_slow", types->i8_ptr_type(), args,
      reinterpret_cast<void*>(gdv_fn_context_arena_malloc_slow));

  // gdv_fn_context_arena_used_ptr
  args = {types->i64_type()};  // int64_t context_ptr

  engine->AddGlobalMappingForFunc("gdv_fn_context_arena_used_ptr", types->i8_ptr_type(),
                                  args,
                                  reinterpret_cast<void*>(gdv_fn_context_arena_used_ptr));

  // gdv_fn_context_arena_reset
  args = {types->i64_type()};  // int64_t context_ptr
  engine->AddGlobalMappingForFunc("gdv_fn_context_arena_reset", types->void_type(), args,
                                  reinterpret_cast<void*>(gdv_fn_context_arena_reset));
  return arrow::Status::OK();
}

}  // namespace gandiva
#endif  // !GANDIVA_UNIT_TEST

extern "C" {

void gdv_fn_context_set_error_msg(int64_t context_ptr, const char* err_msg) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  context->set_error_msg(err_msg);
}

uint8_t* gdv_fn_context_arena_malloc(int64_t context_ptr, int32_t size) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  return context->arena()->Allocate(size);
}

uint8_t* gdv_fn_context_arena_used_ptr(int64_t context_ptr) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  return context->arena()->used_ptr();
}

// Slow path invoked by the pre-compiled fast-path allocator when the current chunk is
// full. Kept out of pre-compiled IR because it pulls in arrow::Status / the memory pool.
uint8_t* gdv_fn_context_arena_malloc_slow(int64_t context_ptr, int32_t size) {
  return gdv_fn_context_arena_malloc(context_ptr, size);
}

void gdv_fn_context_arena_reset(int64_t context_ptr) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  return context->arena()->Reset();
}
}
