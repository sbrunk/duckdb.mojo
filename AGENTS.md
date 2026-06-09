# AGENTS.md

## Project Overview

duckdb.mojo provides Mojo bindings for DuckDB with two modes:
1. **Client API** - Query DuckDB from Mojo, register scalar/aggregate/table UDFs, process results with SIMD vectorization
2. **Extension development** (experimental) - Build DuckDB extensions as shared libraries in Mojo

## Tech Stack

- **Language:** Mojo (stable releases, pinned in pixi.toml)
- **Database:** DuckDB 1.5 (C API via auto-generated FFI bindings)
- **Package manager:** Pixi (conda-based, config in pixi.toml)
- **CI:** GitHub Actions (test.yml - runs on linux-64, linux-aarch64, osx-arm64)

## Project Structure

- `duckdb/` - Main library source (the `duckdb` Mojo package)
  - `_libduckdb.mojo` - Auto-generated low-level C API bindings (do not edit manually)
  - `connection.mojo`, `database.mojo`, `result.mojo` - Core client API
  - `scalar_function.mojo`, `aggregate_function.mojo`, `table_function.mojo` - UDF registration
  - `extension.mojo`, `api_level.mojo` - Extension development support
  - `chunk.mojo`, `vector.mojo`, `value.mojo`, `logical_type.mojo` - Data types
  - `kernels/` - reusable Mojo SIMD kernels (`simd.mojo`) + `register_simd_math` scalar UDF helpers
- `test/` - Test files (one per module, named `test_*.mojo`)
- `demo-extension/` - Working example DuckDB extension in Mojo
- `test-extension/` - Extension used for testing
- `benchmark/` - Performance benchmarks
- `scripts/` - Code generation and build helpers
- `packages/` - Sub-packages (duckdb-from-source, operator-replacement, mojo-kernel-overrides)

## Development Commands

All commands run inside `pixi shell` or via `pixi run`:

```shell
pixi shell                    # Enter dev environment
pixi run test                 # Run all tests (library + extensions)
pixi run test-library         # Run library tests only
pixi run mojo run example.mojo  # Run example
pixi run generate-api         # Regenerate C API bindings from DuckDB source
pixi run check-generated-api  # Fail if _libduckdb.mojo is out of sync with DuckDB
pixi build                    # Build conda package
pixi run overrides-build      # Build the mojo-kernel-overrides extension
pixi run overrides-bench      # Build + benchmark the override extension vs stock DuckDB
pixi run overrides-bench-runner-build     # Build ext + DuckDB benchmark_runner (w/ load-ext hook)
pixi run overrides-bench-runner '<regex>' # Run DuckDB's benchmark suite stock-vs-overrides
```

## Testing

- Tests are individual Mojo files in `test/`, one per module
- Run all: `pixi run test` (runs test-library + test-demo-extension + test-extension)
- Run single: `pixi run mojo run test/test_connection.mojo`
- Tests use assertions (no test framework) - a non-zero exit code indicates failure

## Key Patterns

- The `Connection` type is parameterized with `ApiLevel` (CLIENT, EXT_STABLE, EXT_UNSTABLE) to gate API access at compile time
- `_libduckdb.mojo` is auto-generated - regenerate with `pixi run generate-api` after bumping DuckDB version. CI runs `check-generated-api` (a dedicated job in `test.yml`) to fail the build if the committed bindings are stale, so a forgotten regeneration can't slip into main.
- Extensions use the DuckDB Extension C API with stable/unstable split

## SIMD kernels and the override extension

Mojo SIMD kernels live in `duckdb/kernels/simd.mojo` and are used two ways:

- **Named UDFs (part of the package):** `duckdb.kernels.register_simd_math(conn)` registers
  `mojo_sqrt`/`sin`/`cos`/`ln`/`exp`/`log10` as scalar functions. The kernels ship inside the
  precompiled `duckdb-mojo` package, so there is nothing extra to build or `LOAD`.
- **Built-in overrides (`packages/mojo-kernel-overrides`):** a self-contained CPP-ABI DuckDB
  extension that rewrites the built-in `sqrt`/`sin`/`cos`/`ln`/`exp`/`log10` and
  `sum`/`avg`/`min`/`max` in place via catalog mutation, with stock fallback for non-FLAT /
  null / grouped input. `sum`/`avg` cover `DOUBLE` plus the INT128-backed `HUGEINT` /
  `DECIMAL(19..38)` path — the one decimal aggregate with real headroom (stock uses an
  overflow-checked per-element `Hugeint::Add`; the kernel inlines it with multi-accumulators
  + overflow-fallback, ~7.5× single-threaded). The int64-backed `DECIMAL`/`BIGINT` sum is
  left untouched (already memory-bound). The kernels are emitted as an object (`src/capi_shim.mojo`) and linked
  straight into the one `.so`, so there is no separate kernel lib and no `dlopen`. Build with
  `pixi run overrides-build`; activate via `LOAD` (it is unsigned, so allow unsigned extensions)
  or the exported `register_mojo_overrides(duckdb_connection)`. It is **not** part of the conda
  package and is **version-locked** to the exact DuckDB it was built against (CPP ABI + internal
  headers). It can be driven through DuckDB's own benchmark suite
  (`pixi run overrides-bench-runner-build` then `overrides-bench-runner '<regex>'`): a stock
  `benchmark_runner` built from `.duckdb-src` with a ~13-line `interpreted_benchmark.cpp` hook
  (`packages/mojo-kernel-overrides/benchmark/runner_load_extension.patch`) that `LOAD`s the
  extension via the `DUCKDB_BENCH_EXTENSION` env-var toggle — no libduckdb fork.

## FFI Struct ABI Workaround

Mojo's `abi("C")` lowering on Linux x86_64 has a remaining miscompilation for >16-byte by-value struct arguments when the struct type carries no register-passable marker. As a workaround, the generator emits `duckdb_result` with `RegisterPassable` in its trait list — this routes it through the working ABI path. Both `RegisterPassable` and `TrivialRegisterPassable` select the working path (verified equivalent on nightly `1.0.0b2.dev2026060206`); we use the non-trivial `RegisterPassable`. Track upstream resolution at https://github.com/modular/modular/issues/6511 (the fix landed for register-passable-marked structs; a follow-up is still needed for plain/unmarked structs).

## Updating Mojo Nightly

The Mojo compiler version is pinned in `pixi.toml` (currently `1.0.0b2.dev2026060206` from the `https://conda.modular.com/max-nightly/` channel, set in `package.host-dependencies`, `package.build-dependencies`, the `[dependencies]` `mojo`, and the `operator-replacement` feature's `mojo`) **and** in `conda.recipe/recipe.yaml` (`requirements.build`/`host`/`run`). To update:

1. Check available versions: query `https://conda.modular.com/max-nightly/osx-arm64/repodata.json` (or `linux-64`/`linux-aarch64`) for `mojo-compiler` packages
2. Update the version pin in `pixi.toml` (both `host-dependencies` and `build-dependencies`)
3. Update the same pin in `conda.recipe/recipe.yaml` and `conda.recipe/recipe.local.yaml` (all three of `build`/`host`/`run`) — otherwise `pixi build` and the published conda package will disagree
4. Run `pixi install` to update the lockfile
5. Run `pixi run test-library` to verify compatibility
6. Nightly builds can have breaking changes — if the latest fails, try earlier nightlies

## Packaging / publishing

Two independent paths build a conda package of the bindings, and they must be kept in sync (see the pin checklist above):

- **`pixi build`** — the `[package]` block + `pixi-build-mojo` backend in `pixi.toml`. The backend infers the build steps (no recipe). Used for local builds and for consuming duckdb.mojo as a source dependency from other Pixi workspaces.
- **`conda.recipe/recipe.yaml`** (rattler-build) — an explicit recipe. This is what gets submitted to the [modular-community](https://github.com/modular/modular-community) channel, whose CI runs `rattler-build` on it. Key points: the `run` dependency pins `mojo-compiler` **exactly** (a precompiled `.mojoc` only loads under the exact compiler it was built with — `pin_compatible` would let a newer nightly fail at import); `libduckdb` is a `run` dependency (the bindings `dlopen` it). Verify locally with `conda.recipe/recipe.local.yaml`, which builds from the working tree instead of a pushed git SHA. Before submitting a release, set `source.rev` in `recipe.yaml` to the full release commit SHA.

The sub-packages in `packages/` use a third mechanism (the `pixi-build-rattler-build` backend, which runs rattler-build on their own `recipe.yaml` via `pixi build`) — unrelated to publishing the `duckdb-mojo` package.

## Environments

- **default** - Standard dev environment with precompiled libduckdb from conda-forge
- **full** - Extended environment with operator-replacement feature (builds DuckDB from source)
