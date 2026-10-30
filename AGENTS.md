# AGENTS.md

## Project Overview

duckdb.mojo provides Mojo bindings for DuckDB with two modes:
1. **Client API** - Query DuckDB from Mojo, register scalar/aggregate/table UDFs, process results with SIMD vectorization
2. **Extension development** (experimental) - Build DuckDB extensions as shared libraries in Mojo

## Tech Stack

- **Language:** Mojo (stable releases, pinned in pixi.toml)
- **Database:** DuckDB 2.0 (V1 C API via auto-generated FFI bindings)
- **Package manager:** Pixi (conda-based, config in pixi.toml)
- **CI:** GitHub Actions (test.yml - runs on linux-64, linux-aarch64, osx-arm64)

## Project Structure

- `duckdb/` - Main library source (the `duckdb` Mojo package)
  - `_libduckdb.mojo` - Auto-generated low-level C API bindings (do not edit manually)
  - `_libduckdb_v2.mojo` - Auto-generated low-level bindings for DuckDB's V2 C API (do not edit manually, not used by the high-level API yet)
  - `connection.mojo`, `database.mojo`, `result.mojo` - Core client API
  - `scalar_function.mojo`, `aggregate_function.mojo`, `table_function.mojo` - UDF registration
  - `extension.mojo` - Extension development support (V1 C API)
  - `api_v2.mojo`, `extension_v2.mojo` - V2 C API runtime and extension entrypoint (experimental)
  - `chunk.mojo`, `vector.mojo`, `value.mojo`, `logical_type.mojo` - Data types
  - `kernels/` - reusable Mojo SIMD kernels (`simd.mojo`) + `register_simd_math` scalar UDF helpers
- `test/` - Test files (one per module, named `test_*.mojo`)
- `extensions/demo-extension/` - Working example DuckDB extension in Mojo
- `extensions/test-extension/` - Extensions used for testing (V1 and V2 C API, built by `build.sh`)
- `examples/` - Standalone example programs (`example.mojo` is run in CI as a smoke test; `gpu_knn.mojo` needs a GPU and the `gpu` environment: `pixi run -e gpu mojo run examples/gpu_knn.mojo`)
- `benchmark/` - Performance benchmarks
- `scripts/` - Code generation and build helpers
- `extensions/` - Sub-packages (duckdb-from-source, operator-replacement, mojo-kernel-overrides, mojo-gpu-operator)
- `third_party/duckdb/` - DuckDB source as a git submodule, pinned in `.gitmodules` to the release tag the FFI bindings were generated against (currently `v2.0.0`, shallow). It is the only DuckDB source used for code generation (`generate-api`), the source build (`duckdb-from-source`), and DuckDB's `benchmark_runner`. The CPP-ABI C++ extensions build against conda's `libduckdb-devel` headers by default (it ships the full internal header tree), so the submodule only needs to be checked out for those three uses. Initialize with `git submodule update --init third_party/duckdb` or `pixi run clone-duckdb`.

## Development Commands

All commands run inside `pixi shell` or via `pixi run`:

```shell
pixi shell                    # Enter dev environment
pixi run test                 # Run all tests (library + extensions)
pixi run test-library         # Run library tests only
pixi run mojo run examples/example.mojo  # Run example
pixi run generate-api         # Regenerate C API bindings from DuckDB source
pixi run check-generated-api  # Fail if the generated bindings are out of sync with DuckDB
pixi build                    # Build conda package
pixi run overrides-build      # Build the mojo-kernel-overrides extension
pixi run overrides-bench      # Build + benchmark the override extension vs stock DuckDB
pixi run -e gpu gpu-op-build  # Build the mojo-gpu-operator extension
pixi run -e gpu gpu-op-test   # Build it and run its GPU tests (needs a GPU)
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

- `_libduckdb.mojo` is auto-generated from DuckDB's YAML API spec in the `third_party/duckdb` submodule (`api_spec/v1/`, which replaced the JSON files under `header_generation/` in DuckDB 1.5.6). The generator also checks its extension API struct layout against the submodule's `duckdb_extension.h`. Regenerate with `pixi run generate-api` after bumping the DuckDB version. Move the submodule pin first (see "Updating DuckDB" below). CI runs `check-generated-api` (a dedicated job in `test.yml`) to fail the build if the committed bindings are stale, so a forgotten regeneration can't slip into main. `pixi run clone-duckdb` initializes the submodule and warns if its tag doesn't match the installed `duckdb`.
- Extensions use the DuckDB Extension C API (V1) and request API version v1.5.6 (`EXT_API_STABLE_VERSION` in `scripts/generate_mojo_api.py`, emitted as `DUCKDB_EXTENSION_API_VERSION`; keep the `--capi-version` default in `scripts/append_extension_metadata.py` in sync). DuckDB 1.5.6 stabilized all previously unstable functions, so `duckdb_ext_api_v1` holds the whole API and there is no separate unstable struct. Functions newer than that version would be left out of the struct. In extension mode they are bound to generated stubs that abort only when called. They are not looked up with dlsym, because `_find_dylib` aborts if the host has no loadable `libduckdb`. DuckDB 2.0 added two functions (`duckdb_create_timestamp_tz_ns`, `duckdb_get_timestamp_tz_ns`) to the v1.5.6 band after 1.5.6 shipped. They have no `offset` in the spec, and a 1.5.6 engine lacks their slots. The generator keeps them out of our struct and binds them to stubs in extension mode, so extensions still load on 1.5.6 (they work normally in client mode). DuckDB 2.0's new V2 C API (`api_spec/v2`, `duckdb_v2_*`) is not used by the high-level API or extensions yet: every V2 function is still marked unstable, so a V2 extension would only load on the exact DuckDB version it was built for.
- `_libduckdb_v2.mojo` is generated from `api_spec/v2/` by `scripts/generate_mojo_api_v2.py` (also part of `generate-api` and `check-generated-api`). Unlike the V1 generator it writes every type from the spec (handles, enums, structs including the `bytes` union, aliases, callbacks, constants) and wraps all functions in `LibDuckDBV2`. It checks the extension struct layout against `duckdb_extension_v2.h` (V2 functions have no `offset` while unstable; DuckDB orders the unstable tail by lifecycle date, then name), the parameter count of every function and every enum value against `duckdb_v2.h`. It emits `duckdb_ext_api_v2` (the stable part, empty in 2.0.0) and `duckdb_ext_api_v2_unstable` (everything). `LibDuckDBV2()` loads the functions with dlsym and raises if one is missing; the two other constructors take the extension structs, and with the stable one the unstable functions are bound to aborting stubs. `test/test_libduckdb_v2.mojo` checks struct sizes and runs a query through the raw V2 functions.
- V2 runtime and extensions: `duckdb/api_v2.mojo` holds the process-wide `LibDuckDBV2` (`DuckDBV2().libduckdb()`), plus helpers: `check(code, err)` turns a V2 error code and error info into an `Error`, `null[P]()` gives NULL for optional pointer/handle arguments (never pass `unsafe_dangling()` where DuckDB may read the value), `StrArg` owns a string argument and ties its `const duckdb_v2_str *` to itself. Do not build a `duckdb_v2_str` view of a local `String` and use it later: Mojo frees the String after its last use. In client mode the global aborts if `duckdb_v2_library_version` differs from `DUCKDB_V2_SOURCE_VERSION`. `duckdb/extension_v2.mojo` has `ExtensionV2.run[init, unstable=...]` for the `<name>_init_c_api_v2` entrypoint. `unstable=True` uses `duckdb_ext_api_v2_unstable` and needs the footer `--abi-type C_STRUCT_UNSTABLE` (version defaults to `DUCKDB_V2_SOURCE_VERSION`, read from the bindings by `append_extension_metadata.py`); `unstable=False` uses `duckdb_ext_api_v2` and needs `--capi-version v2.x.y`. DuckDB 2.0.0 refuses the stable target. The V2 test extension (`extensions/test-extension/src/test_v2_extension.mojo`, built by `extensions/test-extension/build.sh` into three copies with different footers) registers `mojo_v2_add` through the raw API. Lessons from it: view-based reads need `vector_flatten` for SEQUENCE/FSST/shredded vectors (`range()` produces SEQUENCE), and in a scalar exec callback `vector_set_null` fails on the result vector (its size is still 0), so write NULLs through `vector_flat_get_validity_mutable`.

## V2 high-level API (`duckdb/v2/`, in progress)

The new high-level API on the V2 C API is built in `duckdb/v2/` next to the V1-based one and replaces it at the end (design: `docs/design/v2-high-level-api.md`). Its "Open points" section is the one list of open work; keep it current. Rules learned so far:

- `__extension` conformances (e.g. `Int64` or `String` to `DuckDBMapped`) and `__extension` methods are only visible in the module that declares them. Never use such a trait as a bound in a public generic; take `T: Movable & Deinitable` and call the dispatch functions in `duckdb/v2/mapping.mojo` (`mapped_read[T]`, `mapped_accepts[T]`, `mapped_to_value[T]`, ...).
- Mojo frees a value right after its last use. When a raw handle or pointer from an owner (`value.handle()`, `list.unsafe_ptr()`, a chunk's data) is passed to DuckDB, keep the owner alive past the call (`_ = owner^`) or tie the borrower to the owner's origin.
- Generic return types need `Movable & Deinitable`. Default argument values must be compile-time constants.
- `Iterator.__next__` may only raise `StopIteration`, which `for` loops swallow. For iteration that can fail (streaming results), use the other loop protocol the compiler accepts: a raising `__has_next__()` plus a non-raising `__next__()` (see `ChunkIter`; not an `Iterator`, so no `enumerate`). Offer a standard `Iterator` of result-or-error elements next to it (see `TryChunkIter`). Loop variables are immutable references; `for var x in ...` gives an owned element.
- Mojo does not stop a second mutable borrow from being created and kept, but it rejects two mutable borrows of one value passed to the same call. Objects shared by many borrowers (`Connection`) take `self` immutably when their Mojo fields don't change.
- V2 query results are always flat vectors; constant, dictionary and sequence vectors only reach callbacks. `ColumnReader` still handles all of them.
- Nested types go through emberserde in `duckdb/v2/serde.mojo`: `_Cursor` (reading) and `_Writer` (writing) check `is_mapped[T]()` first and handle scalars in their storage layout; everything else goes to emberserde. A struct read from DuckDB must be `Defaultable` when it has non-trivial fields (`String`, `List`, nested structs). emberserde is a git dependency (tag `0.2.0`).
- Functions: scalar in `duckdb/v2/functions.mojo`, aggregate in `aggregate.mojo`, table in `table.mojo`, signatures (names, defaults, kinds) in `signature.mojo`, casts/replacement scans/macros/pragmas/options in `extend.mojo`, filter-pushdown expressions in `expression.mojo`. Every V2 aggregate needs bind data that tells it apart (`_identity_bind`): DuckDB merges aggregate calls whose C++ callbacks match, and all C API aggregates share them.
- Scalar functions: `duckdb/v2/functions.mojo`. The per-arity `from_function`/`from_state` overloads and their callbacks are generated: edit `scripts/generate_scalar_functions.py`, then run it (it rewrites the BEGIN/END GENERATED regions in `functions.mojo` and `aggregate.mojo`). Speed: `benchmark/v2_udf_bench.mojo` (V2 vs V1 vs built-ins). V2 extensions use `duckdb.v2.Extension.run[init]` with the `C_STRUCT_UNSTABLE` footer (see `extensions/demo-extension`).
- A bare list literal passed to a generic parameter (`Value([1, 2])`) becomes an `Array[T, n]`, not a `List`, so generic APIs must map `Array` too. A `List[T]` overload next to a generic `T` one is ambiguous.
- Adding fields to structs on the hot read path (`ColumnMeta`, tree nodes) can cost measurable speed even when reads never touch them (a `Dict` field cost 25%); keep rarely used data behind a pointer and rerun `benchmark/v2_serde_bench.mojo`.

## SIMD kernels and the override extension

Mojo SIMD kernels live in `duckdb/kernels/simd.mojo` and are used two ways:

- **Named UDFs (part of the package):** `duckdb.kernels.register_simd_math(conn)` registers
  `mojo_sqrt`/`sin`/`cos`/`ln`/`exp`/`log10` as scalar functions. The kernels ship inside the
  precompiled `duckdb-mojo` package, so there is nothing extra to build or `LOAD`.
- **Built-in overrides (`extensions/mojo-kernel-overrides`):** a self-contained CPP-ABI DuckDB
  extension that rewrites the built-in `sqrt`/`sin`/`cos`/`ln`/`exp`/`log10` and
  `sum`/`avg`/`min`/`max` in place via catalog mutation, with stock fallback for non-FLAT /
  null / grouped input. `sum`/`avg` cover `DOUBLE` plus the INT128-backed `HUGEINT` /
  `DECIMAL(19..38)` path. This is the one decimal aggregate with real room for speedup: stock
  DuckDB calls an overflow-checked `Hugeint::Add` per element, while the kernel inlines it with
  several accumulators and falls back on overflow (about 7.5× faster single-threaded). The int64-backed `DECIMAL`/`BIGINT` sum is
  left untouched (already memory-bound). The kernels are emitted as an object (`src/capi_shim.mojo`) and linked
  straight into the one `.so`, so there is no separate kernel lib and no `dlopen`. Build with
  `pixi run overrides-build`; activate via `LOAD` (it is unsigned, so allow unsigned extensions)
  or the exported `register_mojo_overrides(duckdb_connection)`. It is not part of the conda
  package and is version-locked to the exact DuckDB it was built against (CPP ABI + internal
  headers). It can be driven through DuckDB's own benchmark suite via the consolidated
  harness (`pixi run bench-build` then `pixi run bench-sql mojo_simd --engines=stock,cpu`;
  see `benchmark/README.md`): a stock `benchmark_runner` built from the `third_party/duckdb`
  submodule with a ~13-line `interpreted_benchmark.cpp` hook
  (`benchmark/drivers/runner_load_extension.patch`) that `LOAD`s the extension via the
  `DUCKDB_BENCH_EXTENSION` environment variable, so no libduckdb fork is needed.

## Updating Mojo

The Mojo compiler version is pinned in `pixi.toml` (currently `1.2.0` from the `https://conda.modular.com/max/` stable channel, set in `package.host-dependencies`, `package.build-dependencies`, the `[dependencies]` `mojo`, and the `operator-replacement` feature's `mojo`) and also in `conda.recipe/recipe.yaml` (`requirements.build`/`host`/`run`). To update:

1. Check available versions: query `https://conda.modular.com/max/osx-arm64/repodata.json` (stable releases) or `https://conda.modular.com/max-nightly/osx-arm64/repodata.json` (nightlies) for `mojo-compiler` packages. Check `linux-64` and `linux-aarch64` too. `curl` must follow redirects (`-L`).
2. Update the version pin in `pixi.toml` (both `host-dependencies` and `build-dependencies`); when moving between stable and nightly also update the channel in `[workspace] channels` (stable = `.../max/`, nightly = `.../max-nightly/`)
3. Update the same pin in `conda.recipe/recipe.yaml` and `conda.recipe/recipe.local.yaml` (all three of `build`/`host`/`run`). Otherwise `pixi build` and the published conda package will disagree. The CI `conda-recipe` job's `-c https://conda.modular.com/max...` channel must match too
4. Run `pixi install` to update the lockfile
5. Run `pixi run test-library` to verify compatibility
6. Releases can have breaking changes, so check the release notes. If a nightly fails, try earlier nightlies

## Updating DuckDB

DuckDB source lives in the `third_party/duckdb` git submodule, pinned (via the
gitlink in the index, with `branch`/`shallow` recorded in `.gitmodules`) to the
release tag the FFI bindings were generated against. The `libduckdb`/`duckdb-cli`
conda pins and the submodule pin must stay in lockstep; `pixi run clone-duckdb`
warns when they drift. To bump (for example from `2.0.0` to `2.0.1`):

1. Move the submodule pin to the new tag and stage it:
   ```shell
   git -C third_party/duckdb fetch --depth 1 origin tag v2.0.1
   git -C third_party/duckdb checkout tags/v2.0.1
   git add third_party/duckdb
   ```
   Optionally bump `branch = v2.0.1` in `.gitmodules`.
2. Bump the conda pins to match: `libduckdb-devel`/`duckdb-cli` in `pixi.toml`,
   the `libduckdb >=…` ranges in both `conda.recipe/recipe*.yaml`, and the
   `version`/`tag` in `duckdb-from-source/{pixi.toml,recipe.yaml}` +
   the `duckdb-from-source ==…` pins in `extensions/operator-replacement/recipe.yaml`.
3. `pixi install` to refresh the lockfile.
4. `pixi run generate-api` to regenerate `duckdb/_libduckdb.mojo` and
   `duckdb/_libduckdb_v2.mojo` from the new source, then commit them (CI
   `check-generated-api` fails otherwise). The unstable V2 API can change in
   any release, so expect V2 generator or test fixes.
5. `pixi run test` to verify. The CPP-ABI extensions
   (`mojo-kernel-overrides`, `mojo-gpu-operator`) are version-locked to the exact
   DuckDB and need a rebuild (`pixi run overrides-build` / `gpu-op-build`).
   Major releases usually change DuckDB's internal C++ API, so expect porting
   work, not only a rebuild (all three C++ extensions needed one for 2.0).
   Since 2.0, DuckDB's CMake no longer takes the version from `git describe`:
   a source build without `OVERRIDE_GIT_DESCRIBE=vX.Y.Z` reports its commit
   hash and rejects extensions whose footer says `vX.Y.Z`. `build_runner.sh`
   and the `duckdb-from-source` recipe pass it.

## Packaging / publishing

Two independent paths build a conda package of the bindings, and they must be kept in sync (see the pin checklist above):

- `pixi build`: the `[package]` block and the `pixi-build-mojo` backend in `pixi.toml`. The backend infers the build steps (no recipe). Used for local builds and for consuming duckdb.mojo as a source dependency from other Pixi workspaces.
- `conda.recipe/recipe.yaml` (rattler-build): an explicit recipe. This is what gets submitted to the [modular-community](https://github.com/modular/modular-community) channel, whose CI runs `rattler-build` on it. The `run` dependency pins `mojo-compiler` exactly, because a precompiled `.mojoc` only loads under the exact compiler it was built with (`pin_compatible` would let a newer nightly fail at import). `libduckdb` is a `run` dependency because the bindings `dlopen` it. Verify locally with `conda.recipe/recipe.local.yaml`, which builds from the working tree instead of a pushed git SHA. Build it with `rattler-build build --recipe conda.recipe/recipe.local.yaml -c conda-forge -c https://conda.modular.com/max -c https://repo.prefix.dev/modular-community`. A successful build runs the in-package smoke test and writes the `.conda` under `output/<platform>/`. Before submitting a release, set `source.rev` in `recipe.yaml` to the full release commit SHA.

The sub-packages in `extensions/` use a third mechanism (the `pixi-build-rattler-build` backend, which runs rattler-build on their own `recipe.yaml` via `pixi build`). It is unrelated to publishing the `duckdb-mojo` package.

## Environments

- **default** - Standard dev environment with precompiled libduckdb from conda-forge
- **gpu** - default plus `max-core`, which provides the accelerator APIs (`max.gpu.*`)
  and the `layout` package. Only `mojo-gpu-operator` needs it:
  `pixi run -e gpu gpu-op-build`. On macOS the GPU kernels compile to Metal, which
  needs Xcode's separately-downloaded Metal Toolchain component
  (`xcodebuild -downloadComponent MetalToolchain`).
- **full** - Extended environment with the operator-replacement feature. Builds DuckDB from the `third_party/duckdb` submodule via the `duckdb-from-source` package (initialize the submodule first). `operator-replacement` is now only a reference implementation. It has been replaced by `mojo-kernel-overrides` (Mojo kernels for built-ins) and `mojo-gpu-operator` (the same OptimizerExtension interception, for GPU offload), but it is still set up here. See `extensions/operator-replacement/README.md`. On macOS 27, conda's linker can't read the system SDK (`libSystem.tbd: unknown architecture arm64e.x1-macos`), so the DuckDB source build fails. A local workaround is a temporary `[workspace.build-variants]` entry `CONDA_BUILD_SYSROOT = ["/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk"]` in `pixi.toml` (don't commit it).
