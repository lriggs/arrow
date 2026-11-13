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

#include <cstddef>
#include <cstdint>

#include "gandiva/execution_context.h"

// Context arena allocation, provided as PRE-COMPILED IR rather than an external
// absolute-symbol stub, so the common-case bump allocation inlines into the generated
// expression loop and into the pre-compiled string functions that call it -- instead of
// a per-row opaque call that blocks optimization/vectorization.
//
// Only the fast path (SimpleArena::AllocateFast, pure pointer arithmetic) lives here.
// The slow path -- allocating a new chunk from the memory pool, which pulls in
// arrow::Status and virtual pool calls -- stays an external stub
// (gdv_fn_context_arena_malloc_slow, registered in context_helper.cc). Keeping it out of
// this IR is essential: symbols like arrow::Status::CopyFrom are not resolvable by the
// JIT in the arrow-java / Dremio dylib, and inlining them here breaks Projector::Make.
extern "C" {

// Defined + registered in context_helper.cc.
uint8_t* gdv_fn_context_arena_malloc_slow(int64_t context_ptr, int32_t size);

uint8_t* gdv_fn_context_arena_malloc(int64_t context_ptr, int32_t size) {
  auto context = reinterpret_cast<gandiva::ExecutionContext*>(context_ptr);
  uint8_t* fast = context->arena()->AllocateFast(size);
  if (fast != nullptr) {
    return fast;
  }
  return gdv_fn_context_arena_malloc_slow(context_ptr, size);
}
}
