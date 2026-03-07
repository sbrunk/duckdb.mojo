# CLAUDE.md

## Project Overview

duckdb.mojo provides Mojo bindings for DuckDB with two modes:
1. **Client API** - Query DuckDB from Mojo, register scalar/aggregate/table UDFs, process results with SIMD vectorization
2. **Extension development** (experimental) - Build DuckDB extensions as shared libraries in Mojo

## Tech Stack

- **Language:** Mojo (nightly builds, pinned in pixi.toml)
- **Database:** DuckDB 1.4 (C API via auto-generated FFI bindings)
- **Package manager:** Pixi (conda-based, config in pixi.toml)
- **CI:** GitHub Actions (test.yml - runs on linux-64, linux-aarch64, osx-arm64)

## Project Structure

- `src/` - Main library source (Mojo package named `duckdb`)
  - `_libduckdb.mojo` - Auto-generated low-level C API bindings (do not edit manually)
  - `connection.mojo`, `database.mojo`, `result.mojo` - Core client API
  - `scalar_function.mojo`, `aggregate_function.mojo`, `table_function.mojo` - UDF registration
  - `extension.mojo`, `api_level.mojo` - Extension development support
  - `chunk.mojo`, `vector.mojo`, `value.mojo`, `logical_type.mojo` - Data types
- `test/` - Test files (one per module, named `test_*.mojo`)
- `demo-extension/` - Working example DuckDB extension in Mojo
- `test-extension/` - Extension used for testing
- `benchmark/` - Performance benchmarks
- `scripts/` - Code generation and build helpers
- `packages/` - Sub-packages (duckdb-mojo-helpers, duckdb-from-source, operator-replacement)

## Development Commands

All commands run inside `pixi shell` or via `pixi run`:

```shell
pixi shell                    # Enter dev environment
pixi run test                 # Run all tests (library + extensions)
pixi run test-library         # Run library tests only
pixi run mojo run example.mojo  # Run example
pixi run generate-api         # Regenerate C API bindings from DuckDB source
pixi build                    # Build conda package
```

## Testing

- Tests are individual Mojo files in `test/`, one per module
- Run all: `pixi run test` (runs test-library + test-demo-extension + test-extension)
- Run single: `pixi run mojo run test/test_connection.mojo`
- Tests use assertions (no test framework) - a non-zero exit code indicates failure

## Key Patterns

- The `Connection` type is parameterized with `ApiLevel` (CLIENT, EXT_STABLE, EXT_UNSTABLE) to gate API access at compile time
- `_libduckdb.mojo` is auto-generated - regenerate with `pixi run generate-api` after bumping DuckDB version
- A small C shim library (`packages/duckdb-mojo-helpers`) is needed until Mojo supports variadic C function calls
- Extensions use the DuckDB Extension C API with stable/unstable split

## Updating Mojo Nightly

The Mojo compiler version is pinned in `pixi.toml` (lines 15 and 18: `package.host-dependencies` and `package.build-dependencies`). To update:

1. Check available versions: query `https://conda.modular.com/max-nightly/osx-arm64/repodata.json` (or `linux-64`/`linux-aarch64`) for `mojo-compiler` packages
2. Update the version pin in `pixi.toml` (both `host-dependencies` and `build-dependencies`)
3. Run `pixi install` to update the lockfile
4. Run `pixi run test-library` to verify compatibility
5. Nightly builds can have breaking changes — if the latest fails, try earlier nightlies

## Environments

- **default** - Standard dev environment with precompiled libduckdb from conda-forge
- **full** - Extended environment with operator-replacement feature (builds DuckDB from source)
