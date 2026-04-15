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

#include "gandiva/jit_session.h"

#include <utility>

#include <llvm/ExecutionEngine/Orc/LLJIT.h>

#include "gandiva/engine.h"

namespace gandiva {

JITSession::~JITSession() {}

JITSession::JITSession(std::unique_ptr<llvm::orc::LLJIT> lljit,
                       std::shared_ptr<llvm::TargetMachine> target_machine)
    : lljit_(std::move(lljit)), target_machine_(std::move(target_machine)) {}

arrow::Result<std::shared_ptr<JITSession>> JITSession::Make(
    const std::shared_ptr<Configuration>& conf) {
  // Build a standalone Engine to compile the base module (precompiled bitcode +
  // DecimalIR + TimestampIR + external IR) into a fresh LLJIT instance.
  ARROW_ASSIGN_OR_RAISE(auto engine, Engine::Make(conf, /*cached=*/false));
  ARROW_RETURN_NOT_OK(engine->LoadFunctionIRs());
  // FinalizeModule compiles and adds the base module to the engine's LLJIT.
  // Because no expression functions were added (functions_to_compile_ is empty),
  // RemoveUnusedFunctions is skipped automatically so all base symbols are kept.
  ARROW_RETURN_NOT_OK(engine->FinalizeModule());

  // Transfer LLJIT and TargetMachine ownership to the session.
  auto lljit = engine->ExtractJIT();
  auto tm = engine->target_machine();

  return std::shared_ptr<JITSession>(new JITSession(std::move(lljit), std::move(tm)));
}

}  // namespace gandiva
