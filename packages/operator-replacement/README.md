# DuckDB Operator Replacement

> **Status: reference implementation (superseded for production use).**
> This was the original proof-of-concept for transparently intercepting DuckDB
> queries from a Mojo/C++ extension. The two mechanisms it pioneered have since
> been realized more directly elsewhere in this repo, so it is kept as a
> documented reference rather than a recommended path:
>
> - **Goal — run Mojo kernels in place of built-in functions:** use
>   [`packages/mojo-kernel-overrides`](../mojo-kernel-overrides), which mutates
>   the built-in catalog entries directly (no optimizer pass, no name
>   indirection) and ships real Mojo SIMD kernels with stock fallback.
> - **Technique — intercept and rewrite the plan during optimization:** the same
>   `OptimizerExtension::Register` entry point is used (far more powerfully) by
>   [`packages/mojo-gpu-operator`](../mojo-gpu-operator), which matches whole plan
>   *shapes* and offloads subtrees to GPU kernels.
>
> What remains uniquely useful here is (1) a *generic* name→name function/operator
> swap registry, and (2) the **cast-bridging + re-bind** technique for swapping in
> a differently-typed replacement function — see
> [The reusable technique](#the-reusable-technique-cast-bridging--re-bind) below.
> The package still builds and is wired into the `full` pixi environment.

Replace any scalar function or operator in DuckDB queries with custom implementations using the OptimizerExtension API.

## What It Does

- **Registry-based replacement**: Map any function/operator name to a custom implementation
- **Runtime interception**: Replaces functions during query optimization (works on column expressions, not constants)
- **Uses conda libduckdb**: Build extension against conda-forge's libduckdb-devel (no source build required)

## Quick Start

### Prerequisites

Install pixi:
```bash
curl -fsSL https://pixi.sh/install.sh | bash
```

### Build and Test

**Default (fastest - uses conda libduckdb):**
```bash
pixi install          # Install libduckdb-devel from conda-forge
pixi run test         # Compile extension and run tests
```

**Alternative: Build from source:**
```bash
pixi run -e source-build test  # Build DuckDB from source and test
```

The default environment builds against conda-forge's `libduckdb-devel` v1.5.4 package (recommended). The `source-build` environment clones and builds DuckDB from source for testing custom builds or development versions.

## API Usage

### C API

```c
// 1. Register your custom functions in the catalog
register_custom_multiply(con);
register_custom_sqrt(con);

// 2. Map original function names to replacements
register_function_replacement("*", "custom_multiply");
register_function_replacement("sqrt", "custom_sqrt");

// 3. Activate the optimizer extension
register_operator_replacement(con);

// All queries now use your custom functions
```

### C++ API

```cpp
#include "duckdb_operator_replacement.hpp"

// Register replacements
OperatorReplacementExtension::RegisterReplacement("*", "custom_multiply");
OperatorReplacementExtension::RegisterReplacement("+", "custom_add");

// Clear all replacements
OperatorReplacementExtension::ClearReplacements();

// Get registered replacements
auto& replacements = OperatorReplacementExtension::GetReplacements();
```

## Creating Custom Functions

### 1. Implement Function

```cpp
static void custom_add_func(DataChunk &args, ExpressionState &state, Vector &result) {
    auto &left = args.data[0];
    auto &right = args.data[1];
    
    BinaryExecutor::Execute<int64_t, int64_t, int64_t>(
        left, right, result, args.size(),
        [](int64_t a, int64_t b) {
            return a + b;  // Your custom logic here
        });
}
```

### 2. Register in Catalog

```cpp
void register_custom_add(duckdb_connection con) {
    auto connection = reinterpret_cast<Connection*>(con);
    
    connection->context->RunFunctionInTransaction([&]() {
        auto &catalog = Catalog::GetSystemCatalog(*connection->context);
        
        ScalarFunctionSet func_set("custom_add");
        ScalarFunction func({LogicalType::BIGINT, LogicalType::BIGINT}, 
                           LogicalType::BIGINT, 
                           custom_add_func);
        func_set.AddFunction(func);
        
        CreateScalarFunctionInfo info(func_set);
        catalog.CreateFunction(*connection->context, info);
    });
}
```

### 3. Register Replacement

```c
register_function_replacement("+", "custom_add");
register_operator_replacement(con);
```

## The reusable technique: cast-bridging + re-bind

The non-obvious value of this package is *how* it swaps one bound function for
another inside an already-bound logical plan without corrupting it. When the
`OptimizerExtension` callback finds a `BoundFunctionExpression` whose name is in
the registry (see
[`duckdb_operator_replacement.cpp`](duckdb_operator_replacement.cpp), the
`ReplaceOperators` visitor), a naive `expr.function = replacement` is **not**
enough — the surrounding plan was bound expecting the original function's types
and bind data. Three things have to be bridged:

1. **Argument-type bridging.** The replacement overload is looked up with
   `functions.GetFunctionByArguments(context, arg_types)`; if no compatible
   overload exists the swap is skipped (the original is kept). Where a child's
   type doesn't match the replacement's declared parameter type (e.g. a
   `DECIMAL(15,2)` argument vs a `DECIMAL(18,4)` parameter), a
   `BoundCastExpression::AddCastToType` wrapper is inserted on that child.
   `AddCastToType` is a no-op when source == target, so it's safe to call
   unconditionally.

2. **Re-bind for the new `bind_info`.** The original `bind_info` was produced by
   the built-in's bind callback and has a different layout than the replacement's
   (here, the C-API wrapper expects a `CScalarFunctionInfo`). After assigning
   `expr.function = replacement`, the code re-runs `expr.function.bind(...)` to
   rebuild `bind_info`. **Skipping this is a silent memory-layout mismatch that
   crashes at execution**, not at plan time — the single most important detail.

3. **Return-type bridging.** If the replacement's return type differs from the
   original, the *whole* expression is wrapped in a final
   `AddCastToType(..., original_return_type)` so parent expressions (which were
   bound expecting the original type) stay valid. The execution engine also needs
   `expr.return_type` updated to the replacement's type so it allocates the
   correct output vector.

This cast-bridge + re-bind sequence is the reference to consult if any other
extension ever needs to remap a bound function to a differently-typed
replacement.

## Project Structure

```
test_pixi/
├── duckdb_operator_replacement.hpp        # OptimizerExtension class
├── duckdb_operator_replacement.cpp        # Replacement logic
├── duckdb_operator_replacement_wrapper.cpp # C API wrapper
├── duckdb_operator_replacement.h          # C API header
├── test_functions.cpp                     # Example implementations
├── test_functions.h                       # Example function headers
└── test_operator_replacement.c            # Test program
```

## Expected Test Output

```
=== Testing Operator Replacement ===

1. ✓ Registering custom functions
2. ✓ Direct function call: custom_multiply(3,4) = 6
3. ✓ Registering optimizer extension
4. Testing 3 * 4: Result = 12
   (Constants fold before optimizer - expected)
5. ✓ Complex query: l_quantity * l_extendedprice = 4
   (Operator replaced - custom_multiply doubles left arg: 2*2=4)
6. ✓ sqrt(25.0) = 125.0
   (Function replaced - custom_sqrt adds 100: 25+100=125)

=== Test Complete ===
```

## Pixi Commands

**Default environment (conda libduckdb):**
```bash
pixi install          # Install libduckdb-devel from conda
pixi run test         # Build and test extension
pixi run clean        # Remove build artifacts
```

**Source-build environment (build DuckDB from source):**
```bash
pixi run -e source-build test   # Clone, build DuckDB, compile extension, test
pixi run -e source-build clean  # Remove all build artifacts including duckdb/
```

## Environments

Two environments are available:

- **default**: Uses `libduckdb-devel==1.5.4` from conda-forge (fast, recommended)
- **source-build**: Builds DuckDB v1.5.4 from source (for testing custom builds/dev versions)

Use `-e source-build` flag to run commands in the source-build environment.

## Platform Support

Edit `pixi.toml` for other platforms:

```toml
[workspace]
platforms = ["linux-64"]  # or "win-64", "osx-64"

[dependencies]
clang_linux-64 = "*"  # Adjust compiler for platform
```

## Troubleshooting

**Memory issues during build**: Reduce parallelism in `pixi.toml`:
```toml
build-duckdb = { cmd = "cd duckdb && make -j2 release", depends-on = ["clone-duckdb"] }
```

**Tests fail**: Check DuckDB built successfully:
```bash
ls duckdb/build/release/src/libduckdb.dylib
```

**Extension won't compile**: Verify build dependencies:
```bash
pixi list
```
