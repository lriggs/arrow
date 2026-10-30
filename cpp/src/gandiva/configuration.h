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

#include <cstdint>
#include <memory>
#include <string>

#include "arrow/status.h"
#include "gandiva/function_registry.h"
#include "gandiva/visibility.h"

namespace gandiva {

class ConfigurationBuilder;
/// \brief runtime config for gandiva
///
/// It contains elements to customize gandiva execution
/// at run time.
class GANDIVA_EXPORT Configuration {
 public:
  friend class ConfigurationBuilder;

  /// Selects the LLVM IR optimization pipeline used when optimize() is true.
  enum class OptimizeLevel {
    kDefault = 0,  // full -O3 pipeline (historical behavior)
    kTrimmed,      // -O3 without the redundant duplicate GVN / loop-vectorize passes (#3)
    kFast,         // lightweight -O1-style pipeline for low compile latency (#1)
  };

  /// Default row-count break-even for the compile-vs-run cost model (see
  /// ConfigurationBuilder::build_for_row_estimate). Below this many input rows, the
  /// time saved by skipping optimization outweighs the slower unoptimized runtime.
  /// Measured on a mid-complexity projection; callers with very simple or very heavy
  /// expressions may want to override it.
  static constexpr int64_t kDefaultOptBreakEvenRows = 345000;

  explicit Configuration(bool optimize,
                         std::shared_ptr<FunctionRegistry> function_registry =
                             gandiva::default_function_registry(),
                         bool dump_ir = false)
      : optimize_(optimize),
        target_host_cpu_(true),
        function_registry_(std::move(function_registry)),
        dump_ir_(dump_ir) {}

  Configuration() : Configuration(true) {}

  std::size_t Hash() const;
  bool operator==(const Configuration& other) const;
  bool operator!=(const Configuration& other) const;

  bool optimize() const { return optimize_; }
  bool target_host_cpu() const { return target_host_cpu_; }
  bool dump_ir() const { return dump_ir_; }
  OptimizeLevel opt_level() const { return opt_level_; }
  std::shared_ptr<FunctionRegistry> function_registry() const {
    return function_registry_;
  }

  void set_optimize(bool optimize) { optimize_ = optimize; }
  void set_dump_ir(bool dump_ir) { dump_ir_ = dump_ir; }
  void set_opt_level(OptimizeLevel opt_level) { opt_level_ = opt_level; }
  void target_host_cpu(bool target_host_cpu) { target_host_cpu_ = target_host_cpu; }
  void set_function_registry(std::shared_ptr<FunctionRegistry> function_registry) {
    function_registry_ = std::move(function_registry);
  }

 private:
  bool optimize_;        /* optimise the generated llvm IR */
  bool target_host_cpu_; /* set the mcpu flag to host cpu while compiling llvm ir */
  std::shared_ptr<FunctionRegistry>
      function_registry_; /* function registry that may contain external functions */
  // flag indicating if IR dumping is needed, defaults to false, and turning it on will
  // negatively affect performance
  bool dump_ir_ = false;
  // Selects the IR optimization pipeline when optimize_ is true. Defaults to full -O3.
  OptimizeLevel opt_level_ = OptimizeLevel::kDefault;
};

/// \brief configuration builder for gandiva
///
/// Provides a default configuration and convenience methods
/// to override specific values and build a custom instance
class GANDIVA_EXPORT ConfigurationBuilder {
 public:
  std::shared_ptr<Configuration> build() {
    std::shared_ptr<Configuration> configuration(new Configuration());
    return configuration;
  }

  std::shared_ptr<Configuration> build(bool optimize) {
    std::shared_ptr<Configuration> configuration(new Configuration(optimize));
    return configuration;
  }

  std::shared_ptr<Configuration> build_with_ir_dumping(bool dump_ir) {
    std::shared_ptr<Configuration> configuration(
        new Configuration(true, gandiva::default_function_registry(), dump_ir));
    return configuration;
  }

  std::shared_ptr<Configuration> build(
      std::shared_ptr<FunctionRegistry> function_registry) {
    std::shared_ptr<Configuration> configuration(
        new Configuration(true, std::move(function_registry)));
    return configuration;
  }

  /// \brief Cost-model hook (#1): choose the optimization tier from an estimated input
  /// row count.
  ///
  /// Below `break_even_rows` the compile time saved by skipping optimization outweighs
  /// the slower unoptimized runtime, so optimization is turned off for the lowest
  /// compile latency. At or above it (or when the estimate is unknown, i.e. negative)
  /// the trimmed -O3 pipeline is used -- same runtime as full -O3, lower compile cost.
  std::shared_ptr<Configuration> build_for_row_estimate(
      int64_t estimated_rows,
      int64_t break_even_rows = Configuration::kDefaultOptBreakEvenRows) {
    std::shared_ptr<Configuration> configuration(new Configuration());
    if (estimated_rows >= 0 && estimated_rows < break_even_rows) {
      configuration->set_optimize(false);
    } else {
      configuration->set_optimize(true);
      configuration->set_opt_level(Configuration::OptimizeLevel::kTrimmed);
    }
    return configuration;
  }

  static std::shared_ptr<Configuration> DefaultConfiguration() {
    return default_configuration_;
  }

 private:
  static std::shared_ptr<Configuration> InitDefaultConfig() {
    std::shared_ptr<Configuration> configuration(new Configuration());
    return configuration;
  }

  static const std::shared_ptr<Configuration> default_configuration_;
};

}  // namespace gandiva
