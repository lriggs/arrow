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

#include <gtest/gtest.h>
#include <llvm/IR/DataLayout.h>
#include <llvm/Support/Host.h>

#include "gandiva/llvm_generator.h"
#include "gandiva/tests/test_util.h"

namespace gandiva {

class TestTargetDataLayout : public ::testing::Test {
 protected:
  void SetUp() override {}
};

// Test that verifies the target data layout string representation
// is consistent with the CPU architecture. This test is portable
// across different architectures.
TEST_F(TestTargetDataLayout, VerifyDataLayoutForArchitecture) {
  // Create an LLVM generator with default configuration
  ASSERT_OK_AND_ASSIGN(auto generator, LLVMGenerator::Make(TestConfiguration(), false));

  // Get the module from the generator
  llvm::Module* module = generator->module();
  ASSERT_NE(module, nullptr);

  // Get the data layout from the module
  const llvm::DataLayout& data_layout = module->getDataLayout();
  std::string data_layout_str = data_layout.getStringRepresentation();

  // Verify that the data layout string is not empty
  EXPECT_FALSE(data_layout_str.empty());

  // Get the host CPU architecture information
  std::string host_cpu = llvm::sys::getHostCPUName().str();
  std::string triple = llvm::sys::getDefaultTargetTriple();

  // Log the information for debugging
  std::cout << "Host CPU: " << host_cpu << std::endl;
  std::cout << "Target Triple: " << triple << std::endl;
  std::cout << "Data Layout: " << data_layout_str << std::endl;

  // Verify basic data layout properties based on architecture
  // The data layout string should contain endianness information
  // 'e' for little-endian, 'E' for big-endian
  EXPECT_TRUE(data_layout_str[0] == 'e' || data_layout_str[0] == 'E')
      << "Data layout should start with endianness specifier";

  // Check for pointer size information in the data layout
  // Most modern architectures use 64-bit pointers
  if (triple.find("x86_64") != std::string::npos ||
      triple.find("aarch64") != std::string::npos ||
      triple.find("arm64") != std::string::npos) {
    // For 64-bit architectures, expect pointer size to be 64 bits
    EXPECT_NE(data_layout_str.find("p:64:64"), std::string::npos)
        << "Expected 64-bit pointer size for 64-bit architecture";
  } else if (triple.find("i386") != std::string::npos ||
             triple.find("i686") != std::string::npos ||
             triple.find("arm") != std::string::npos) {
    // For 32-bit architectures, expect pointer size to be 32 bits
    EXPECT_NE(data_layout_str.find("p:32:32"), std::string::npos)
        << "Expected 32-bit pointer size for 32-bit architecture";
  }

  // Verify that integer alignments are present
  EXPECT_NE(data_layout_str.find("i"), std::string::npos)
      << "Data layout should contain integer type information";

  // Verify endianness consistency
  if (data_layout_str[0] == 'e') {
    std::cout << "Architecture is little-endian" << std::endl;
  } else if (data_layout_str[0] == 'E') {
    std::cout << "Architecture is big-endian" << std::endl;
  }

  // Additional verification: Check that the data layout is valid
  // by querying specific properties
  EXPECT_GT(data_layout.getPointerSize(), 0u)
      << "Pointer size should be greater than 0";
  EXPECT_TRUE(data_layout.isLittleEndian() || data_layout.isBigEndian())
      << "Data layout should have a defined endianness";
}

// Test that verifies data layout consistency across multiple generator instances
TEST_F(TestTargetDataLayout, VerifyDataLayoutConsistency) {
  // Create two LLVM generators with the same configuration
  ASSERT_OK_AND_ASSIGN(auto generator1, LLVMGenerator::Make(TestConfiguration(), false));
  ASSERT_OK_AND_ASSIGN(auto generator2, LLVMGenerator::Make(TestConfiguration(), false));

  // Get the data layout from both modules
  const llvm::DataLayout& data_layout1 = generator1->module()->getDataLayout();
  const llvm::DataLayout& data_layout2 = generator2->module()->getDataLayout();

  std::string data_layout_str1 = data_layout1.getStringRepresentation();
  std::string data_layout_str2 = data_layout2.getStringRepresentation();

  // Verify that both generators produce the same data layout
  EXPECT_EQ(data_layout_str1, data_layout_str2)
      << "Data layout should be consistent across generator instances";

  // Verify that pointer sizes match
  EXPECT_EQ(data_layout1.getPointerSize(), data_layout2.getPointerSize())
      << "Pointer sizes should match";

  // Verify that endianness matches
  EXPECT_EQ(data_layout1.isLittleEndian(), data_layout2.isLittleEndian())
      << "Endianness should match";
}

// Test that verifies specific data layout properties for common types
TEST_F(TestTargetDataLayout, VerifyTypeAlignments) {
  ASSERT_OK_AND_ASSIGN(auto generator, LLVMGenerator::Make(TestConfiguration(), false));

  llvm::Module* module = generator->module();
  const llvm::DataLayout& data_layout = module->getDataLayout();

  // Get the LLVM context and types
  llvm::LLVMContext* context = generator->context();
  LLVMTypes* types = generator->types();

  // Verify alignment for common integer types
  EXPECT_GT(data_layout.getABITypeAlign(types->i8_type()).value(), 0u)
      << "i8 type should have non-zero alignment";
  EXPECT_GT(data_layout.getABITypeAlign(types->i32_type()).value(), 0u)
      << "i32 type should have non-zero alignment";
  EXPECT_GT(data_layout.getABITypeAlign(types->i64_type()).value(), 0u)
      << "i64 type should have non-zero alignment";

  // Verify alignment for pointer types
  EXPECT_GT(data_layout.getABITypeAlign(types->i8_ptr_type()).value(), 0u)
      << "i8* type should have non-zero alignment";

  // Verify type sizes
  EXPECT_EQ(data_layout.getTypeAllocSize(types->i8_type()), 1u)
      << "i8 type should be 1 byte";
  EXPECT_EQ(data_layout.getTypeAllocSize(types->i32_type()), 4u)
      << "i32 type should be 4 bytes";
  EXPECT_EQ(data_layout.getTypeAllocSize(types->i64_type()), 8u)
      << "i64 type should be 8 bytes";

  // Log the data layout string for reference
  std::cout << "Data Layout: " << data_layout.getStringRepresentation() << std::endl;
}

// Test that verifies data layout for different CPU configurations
TEST_F(TestTargetDataLayout, VerifyDataLayoutForHostCPU) {
  // Create a configuration that targets the host CPU
  auto config = TestConfiguration();

  ASSERT_OK_AND_ASSIGN(auto generator, LLVMGenerator::Make(config, false));

  llvm::Module* module = generator->module();
  const llvm::DataLayout& data_layout = module->getDataLayout();
  std::string data_layout_str = data_layout.getStringRepresentation();

  // Get host information
  std::string host_cpu = llvm::sys::getHostCPUName().str();
  std::string triple = llvm::sys::getDefaultTargetTriple();

  std::cout << "Testing with Host CPU: " << host_cpu << std::endl;
  std::cout << "Target Triple: " << triple << std::endl;
  std::cout << "Data Layout: " << data_layout_str << std::endl;

  // Verify that the data layout is appropriate for the host
  EXPECT_FALSE(data_layout_str.empty());

  // Check architecture-specific properties
  if (triple.find("x86_64") != std::string::npos) {
    // x86_64 should be little-endian
    EXPECT_EQ(data_layout_str[0], 'e') << "x86_64 should be little-endian";
    // x86_64 should have 64-bit pointers
    EXPECT_EQ(data_layout.getPointerSizeInBits(), 64u)
        << "x86_64 should have 64-bit pointers";
  } else if (triple.find("aarch64") != std::string::npos ||
             triple.find("arm64") != std::string::npos) {
    // ARM64 should be little-endian (most common configuration)
    EXPECT_EQ(data_layout_str[0], 'e') << "ARM64 should be little-endian";
    // ARM64 should have 64-bit pointers
    EXPECT_EQ(data_layout.getPointerSizeInBits(), 64u)
        << "ARM64 should have 64-bit pointers";
  }

  // Verify that the data layout is valid by checking basic properties
  EXPECT_TRUE(data_layout.isLittleEndian() || data_layout.isBigEndian());
  EXPECT_GT(data_layout.getPointerSize(), 0u);
}

}  // namespace gandiva

