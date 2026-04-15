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

#include <memory>

#include "arrow/result.h"
#include "gandiva/configuration.h"
#include "gandiva/visibility.h"

namespace llvm {
class TargetMachine;
namespace orc {
class LLJIT;
}  // namespace orc
}  // namespace llvm

namespace gandiva {

/// \brief A long-lived JIT session that compiles the Gandiva base IR (precompiled
/// bitcode, DecimalIR, TimestampIR) exactly once and exposes a shared LLJIT instance
/// whose JITDylib can be reused across many Projector/Filter builds.
///
/// Typical usage:
///   auto session = JITSession::Make(config);
///   // ... thousands of queries:
///   Projector::Make(schema, exprs, config, session, &projector);
///
/// Each Projector/Filter built against a session compiles only the tiny per-query
/// expression function and resolves calls to base functions from the session's
/// already-compiled JITDylib, eliminating per-query LLJIT construction and base
/// IR loading overhead.
///
/// Limitation: compiled query modules are never unloaded from the LLJIT session.
/// For workloads with unbounded numbers of distinct expressions, periodically
/// create a fresh JITSession to reclaim memory.
class GANDIVA_EXPORT JITSession {
 public:
  // Inline dtor would attempt to resolve the destructor for llvm::orc::LLJIT
  // (an incomplete type here), so we compile it in the object code.
  ~JITSession();

  /// Build the session: compile all base IR once and return the session.
  static arrow::Result<std::shared_ptr<JITSession>> Make(
      const std::shared_ptr<Configuration>& conf);

  llvm::orc::LLJIT& lljit() const { return *lljit_; }
  std::shared_ptr<llvm::TargetMachine> target_machine() const { return target_machine_; }

 private:
  JITSession(std::unique_ptr<llvm::orc::LLJIT> lljit,
             std::shared_ptr<llvm::TargetMachine> target_machine);

  std::unique_ptr<llvm::orc::LLJIT> lljit_;
  std::shared_ptr<llvm::TargetMachine> target_machine_;
};

}  // namespace gandiva
