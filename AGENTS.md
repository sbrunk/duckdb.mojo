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
- `packages/` - Sub-packages (duckdb-from-source, operator-replacement, mojo-kernel-overrides, mojo-gpu-operator)
- `third_party/duckdb/` - DuckDB source as a **git submodule**, pinned in `.gitmodules` to the release tag the FFI bindings were generated against (currently `v1.5.4`, shallow). Single source of truth for code generation (`generate-api`), the source build (`duckdb-from-source`), and DuckDB's `benchmark_runner`. The CPP-ABI C++ extensions build against conda's `libduckdb-devel` headers by default (it ships the full internal header tree), so the submodule only needs to be checked out for those three uses. Initialize with `git submodule update --init third_party/duckdb` or `pixi run clone-duckdb`.

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
# Consolidated benchmarks (see benchmark/README.md):
pixi run bench-build          # Build DuckDB's benchmark_runner (w/ load-ext hook), once
pixi run bench-sql <group> --engines=stock,cpu,gpu   # warm compare (mojo_simd|gpu_xover|gpu_knn|tpch/...)
pixi run bench-knn            # Mojo vector-search harness: stock/cpu-simd/vss-HNSW/GPU, latency+recall
```

## Testing

- Tests are individual Mojo files in `test/`, one per module
- Run all: `pixi run test` (runs test-library + test-demo-extension + test-extension)
- Run single: `pixi run mojo run test/test_connection.mojo`
- Tests use assertions (no test framework) - a non-zero exit code indicates failure

## Key Patterns

- The `Connection` type is parameterized with `ApiLevel` (CLIENT, EXT_STABLE, EXT_UNSTABLE) to gate API access at compile time
- `_libduckdb.mojo` is auto-generated from the `third_party/duckdb` submodule - regenerate with `pixi run generate-api` after bumping DuckDB version (which means **moving the submodule pin first**, see "Updating DuckDB" below). CI runs `check-generated-api` (a dedicated job in `test.yml`) to fail the build if the committed bindings are stale, so a forgotten regeneration can't slip into main. `pixi run clone-duckdb` initializes the submodule and warns if its tag doesn't match the installed `duckdb`.
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
  headers). It can be driven through DuckDB's own benchmark suite via the consolidated
  harness (`pixi run bench-build` then `pixi run bench-sql mojo_simd --engines=stock,cpu`;
  see `benchmark/README.md`): a stock `benchmark_runner` built from the `third_party/duckdb`
  submodule with a ~13-line `interpreted_benchmark.cpp` hook
  (`benchmark/drivers/runner_load_extension.patch`) that `LOAD`s the extension via the
  `DUCKDB_BENCH_EXTENSION` env-var toggle — no libduckdb fork.

## FFI Struct ABI Workaround

Mojo's `abi("C")` lowering on Linux x86_64 has a remaining miscompilation for >16-byte by-value struct arguments when the struct type carries no register-passable marker. As a workaround, the generator emits `duckdb_result` with `RegisterPassable` in its trait list — this routes it through the working ABI path. Both `RegisterPassable` and `TrivialRegisterPassable` select the working path (verified equivalent on Mojo `1.0.0b2` stable); we use the non-trivial `RegisterPassable`. Track upstream resolution at https://github.com/modular/modular/issues/6511 (the fix landed for register-passable-marked structs; a follow-up is still needed for plain/unmarked structs).

## Updating Mojo

The Mojo compiler version is pinned in `pixi.toml` (currently `1.0.0b2` from the `https://conda.modular.com/max/` stable channel, set in `package.host-dependencies`, `package.build-dependencies`, the `[dependencies]` `mojo`, and the `operator-replacement` feature's `mojo`) **and** in `conda.recipe/recipe.yaml` (`requirements.build`/`host`/`run`). To update:

1. Check available versions: query `https://conda.modular.com/max/osx-arm64/repodata.json` (stable releases) or `https://conda.modular.com/max-nightly/osx-arm64/repodata.json` (nightlies) — also `linux-64`/`linux-aarch64` — for `mojo-compiler` packages. Note `curl` must follow redirects (`-L`).
2. Update the version pin in `pixi.toml` (both `host-dependencies` and `build-dependencies`); when moving between stable and nightly also update the channel in `[workspace] channels` (stable = `.../max/`, nightly = `.../max-nightly/`)
3. Update the same pin in `conda.recipe/recipe.yaml` and `conda.recipe/recipe.local.yaml` (all three of `build`/`host`/`run`) — otherwise `pixi build` and the published conda package will disagree; the CI `conda-recipe` job's `-c https://conda.modular.com/max...` channel must match too
4. Run `pixi install` to update the lockfile
5. Run `pixi run test-library` to verify compatibility
6. Releases can have breaking changes — check the release notes; if a nightly fails, try earlier nightlies

## Updating DuckDB

DuckDB source lives in the `third_party/duckdb` git submodule, pinned (via the
gitlink in the index, with `branch`/`shallow` recorded in `.gitmodules`) to the
release tag the FFI bindings were generated against. The `libduckdb`/`duckdb-cli`
conda pins and the submodule pin must stay in lockstep; `pixi run clone-duckdb`
warns when they drift. To bump (e.g. `1.5.4` → `1.5.5`):

1. Move the submodule pin to the new tag and stage it:
   ```shell
   git -C third_party/duckdb fetch --depth 1 origin tag v1.5.5
   git -C third_party/duckdb checkout tags/v1.5.5
   git add third_party/duckdb
   ```
   Optionally bump `branch = v1.5.5` in `.gitmodules`.
2. Bump the conda pins to match: `libduckdb-devel`/`duckdb-cli` in `pixi.toml`,
   the `libduckdb >=…` ranges in both `conda.recipe/recipe*.yaml`, and the
   `version`/`tag` in `packages/duckdb-from-source/{pixi.toml,recipe.yaml}` +
   the `duckdb-from-source ==…` pins in `packages/operator-replacement/recipe.yaml`.
3. `pixi install` to refresh the lockfile.
4. `pixi run generate-api` to regenerate `duckdb/_libduckdb.mojo` from the new
   source, then commit it (CI `check-generated-api` fails otherwise).
5. `pixi run test` to verify. The CPP-ABI extensions
   (`mojo-kernel-overrides`, `mojo-gpu-operator`) are version-locked to the exact
   DuckDB and need a rebuild (`pixi run overrides-build` / `gpu-op-build`).

## Packaging / publishing

Two independent paths build a conda package of the bindings, and they must be kept in sync (see the pin checklist above):

- **`pixi build`** — the `[package]` block + `pixi-build-mojo` backend in `pixi.toml`. The backend infers the build steps (no recipe). Used for local builds and for consuming duckdb.mojo as a source dependency from other Pixi workspaces.
- **`conda.recipe/recipe.yaml`** (rattler-build) — an explicit recipe. This is what gets submitted to the [modular-community](https://github.com/modular/modular-community) channel, whose CI runs `rattler-build` on it. Key points: the `run` dependency pins `mojo-compiler` **exactly** (a precompiled `.mojoc` only loads under the exact compiler it was built with — `pin_compatible` would let a newer nightly fail at import); `libduckdb` is a `run` dependency (the bindings `dlopen` it). Verify locally with `conda.recipe/recipe.local.yaml`, which builds from the working tree instead of a pushed git SHA. Before submitting a release, set `source.rev` in `recipe.yaml` to the full release commit SHA.

The sub-packages in `packages/` use a third mechanism (the `pixi-build-rattler-build` backend, which runs rattler-build on their own `recipe.yaml` via `pixi build`) — unrelated to publishing the `duckdb-mojo` package.

## Environments

- **default** - Standard dev environment with precompiled libduckdb from conda-forge
- **full** - Extended environment with the operator-replacement feature. Builds DuckDB from the `third_party/duckdb` submodule via the `duckdb-from-source` package (initialize the submodule first). `operator-replacement` is now a **reference implementation** — superseded by `mojo-kernel-overrides` (Mojo kernels for built-ins) and `mojo-gpu-operator` (the same OptimizerExtension interception, for GPU offload) — but is kept wired here. See `packages/operator-replacement/README.md`.
