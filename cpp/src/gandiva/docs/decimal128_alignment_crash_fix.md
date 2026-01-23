# Decimal128 Alignment Crash Fix

## Problem Summary

Gandiva JIT-compiled expressions involving decimal128 operations were crashing with `AddressSanitizer:DEADLYSIGNAL` when IR tracing was disabled. The crash was a classic "Heisenbug" - enabling debug tracing (`enable_ir_traces_(true)`) made the crash disappear.

## Symptoms

- Crash occurred during execution of JIT-compiled expressions with decimal128 columns
- ASAN reported a deadly signal (SIGSEGV/SIGBUS)
- The crash was non-deterministic - some decimal128 loads succeeded while others crashed
- Enabling IR traces "fixed" the crash (masking the real bug)

## Root Cause

**Arrow decimal128 data is 8-byte aligned, but LLVM's default `CreateLoad` for i128 type assumes 16-byte alignment.**

When loading decimal128 values from Arrow arrays, the code was using:

```cpp
slot_value = builder->CreateLoad(types->i128_type(), slot_offset, dex.FieldName());
```

This generates LLVM IR like:

```llvm
%SumCeilValue0 = load i128, ptr %25, align 16
```

The `align 16` tells LLVM the pointer is 16-byte aligned. When it's not, this is **undefined behavior** and can cause:
- Crashes on some CPU architectures
- Crashes when ASAN is enabled (stricter alignment checking)
- Intermittent crashes depending on memory layout

## Debugging Journey

### Initial Misdirections

Several hypotheses were investigated that turned out to be unrelated:
1. Heap-use-after-free in AddTrace() - Fixed but crash persisted
2. Hardcoded pointers in decimal IN expressions - Fixed but crash persisted
3. Wrong function signature for decimal IN - Fixed but crash persisted
4. Stack alignment attributes missing - Added but crash persisted
5. Alloca instructions in wrong basic block - Fixed but crash persisted

### The Heisenbug Clue

The key insight was that `enable_ir_traces_(true)` made the crash disappear. This suggested:
- The bug was sensitive to code generation changes
- Adding trace calls changed memory layout or optimizer decisions
- The underlying bug was likely memory-related (alignment, corruption, etc.)

### Systematic Debug Marker Approach

We added debug markers at strategic points in the generated code:

```cpp
void gdv_debug_marker(int64_t location_id, const char* location_name);
```

This allowed us to narrow down the crash location:
1. Location 20: Loop body start ✓
2. Location 30: Before decimal load ✓
3. Location 31: After GEP, before load ✓
4. Location 32: After decimal load ✗ (CRASH)

### Pointer/Index Debug Function

We added a function to print pointer addresses and alignment:

```cpp
void gdv_debug_ptr_index(const char* field_name, int64_t base_ptr, 
                         int64_t index, int64_t computed_ptr);
```

This revealed the smoking gun:

```
[JIT_DEBUG_PTR] SumFloorValue0: base_ptr=0x7f16900d49c8, index=0, computed_ptr=0x7f16900d49c8 (aligned=NO)
[JIT_DEBUG_PTR] SumCeilValue0: base_ptr=0x7f16900e49c8, index=0, computed_ptr=0x7f16900e49c8 (aligned=NO)
```

Both pointers end in `0x...9c8`:
- `0x9c8 % 16 = 8` → 8-byte aligned, NOT 16-byte aligned

## The Fix

Change from default alignment to explicit 8-byte alignment:

```cpp
// Before (BROKEN):
slot_value = builder->CreateLoad(types->i128_type(), slot_offset, dex.FieldName());

// After (FIXED):
slot_value = builder->CreateAlignedLoad(types->i128_type(), slot_offset, 
                                        llvm::MaybeAlign(8), false, dex.FieldName());
```

This generates correct IR:

```llvm
%SumCeilValue0 = load i128, ptr %25, align 8
```

## Why Traces "Fixed" It

When IR traces were enabled:
1. Printf calls were inserted throughout the code
2. LLVM's optimizer made different decisions
3. The memory layout changed
4. By chance, the misaligned loads happened to work (or were optimized differently)

This was NOT a real fix - just masking the underlying alignment bug.

## Lessons Learned

1. **Heisenbugs often indicate memory/alignment issues** - When adding debug code "fixes" a crash, suspect UB.

2. **Don't assume default alignment is correct** - LLVM's default alignment for types may not match your data's actual alignment.

3. **Systematic binary search with debug markers** - Adding markers at strategic points helps narrow down crash locations in JIT code.

4. **Print actual pointer values** - Logging addresses reveals alignment issues that are invisible in the source code.

5. **ASAN is your friend** - It catches alignment issues that might silently corrupt data on other systems.

## Files Modified

- `cpp/src/gandiva/llvm_generator.cc` - Fixed decimal128 load AND store alignment:
  - `VectorReadFixedLenValueDex` visitor: Use `CreateAlignedLoad` with 8-byte alignment
  - Output store code: Use `CreateAlignedStore` with 8-byte alignment for decimal128

## Unit Test

A unit test was added to verify the fix: `cpp/src/gandiva/tests/decimal_alignment_test.cc`

The test:
1. Creates decimal128 arrays with data at 8-byte aligned (but NOT 16-byte aligned) addresses
2. Runs a subtract operation on the misaligned data
3. Verifies the operation completes without crashing

Run the test with:
```bash
./debug/gandiva-projector-test --gtest_filter="*Alignment*"
```

## Testing

After the fix, all decimal128 operations should work regardless of:
- IR tracing enabled/disabled
- ASAN enabled/disabled
- Memory layout variations
- Data alignment (8-byte or 16-byte)

