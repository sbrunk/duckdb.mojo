# DuckDB Operator Replacement

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

```bash
pixi install          # Install libduckdb-devel from conda-forge
pixi run test         # Compile extension and run tests (~10 seconds)
```

The extension builds against conda-forge's `libduckdb-devel` v1.4.4 package.

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

```bash
pixi install          # Install libduckdb-devel from conda
pixi run test         # Build and test extension
pixi run clean        # Remove build artifacts
```

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
