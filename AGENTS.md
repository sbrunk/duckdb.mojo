# AGENTS.md

## Project Overview

duckdb.mojo provides Mojo bindings for DuckDB with two modes:

1. **Client API** - Query DuckDB from Mojo, register scalar/aggregate/table UDFs, process results with SIMD vectorization
2. **Extension development** (experimental) - Build DuckDB extensions as shared libraries in Mojo

## Tech Stack

- **Language:** Mojo (stable releases, pinned in pixi.toml)
- **Database:** DuckDB 2.0.0 (V2 C API via auto-generated FFI bindings; the raw V1 bindings are generated too)
- **Package manager:** Pixi (conda-based, config in pixi.toml)
- **CI:** GitHub Actions (test.yml - runs on linux-64, linux-aarch64, osx-arm64)

## Invariants

- DuckDB's V2 C API is still fully unstable, so `libduckdb` and `mojo-compiler`
  are exact pins in every file that names them. A precompiled `.mojoc` loads
  only under the compiler that built it. A V2 extension loads only on the
  DuckDB it was built for.
- The high-level API uses the V2 C API only. The raw V1 bindings stay for
  low-level users. The high-level API does not use them.

## Project Structure

- `duckdb/` - the `duckdb` Mojo package
  - `_libduckdb_v2.mojo`, `_libduckdb.mojo` - generated low-level V2/V1
    bindings (do not edit, see "Generated files")
  - `api_v2.mojo`, `extension_v2.mojo` - raw V2 runtime and extension
    entrypoint; their docstrings hold the API version and target rules
  - `mapping.mojo`, `serde.mojo` - scalar mapping, nested (de)serialization
  - `functions/aggregate/table/signature/extend/expression/copy/settings/files/`
    `observe/arrow.mojo` - one API area per module
  - `default.mojo` - process-wide in-memory connection; the package root
    re-exports its API (`duckdb.sql`, `duckdb.execute`, `duckdb.connect`, ...)
  - `kernels/` - SIMD kernels, `register_simd_math`
- `test/` - one Mojo file per module (`test_*.mojo`)
- `examples/`, `benchmark/` - examples and benchmarks (`benchmark/README.md`)
- `extensions/` - sub-packages (mostly with their own README): `demo-extension`,
  `test-extension` (built by `build.sh`), `duckdb-from-source`,
  `operator-replacement`, `mojo-kernel-overrides`, `mojo-gpu-operator`
- `scripts/` - code generation and build helpers
- `third_party/duckdb/` - DuckDB source, shallow submodule pinned to the tag
  the bindings were generated against. Only needed for `generate-api`, the
  source build, and `benchmark_runner`; CPP-ABI extensions use conda headers.
- `docs/design/high-level-api.md` - design and decisions. Its "Open points"
  section lists open work. Keep it current.

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

- Tests are Mojo files with plain assertions (no framework), one per module in
  `test/`. A non-zero exit code indicates failure.
- Run all: `pixi run test`. Run a single file:
  `pixi run mojo run test/test_results.mojo` (a few minutes, the first test
  includes compile time).

## Generated files (do not edit)

- `duckdb/_libduckdb.mojo`, `duckdb/_libduckdb_v2.mojo`: generated from the
  `third_party/duckdb` api_spec by `pixi run generate-api`. Commit them, CI
  (`check-generated-api`) fails on drift. The generators also check the
  extension struct layout against the submodule's headers.
- Generated regions in `functions.mojo` and `aggregate.mojo` come from
  `scripts/generate_scalar_functions.py`: edit the script, re-run it.

## High-level API rules

- `__extension` conformances (e.g. `Int64` to `DuckDBMapped`) and
  `__extension` methods are only visible in the module that declares them.
  Never use such a trait as a bound in a public generic; take
  `T: Movable & Deinitable` and call the dispatch functions in
  `duckdb/mapping.mojo` (`mapped_read[T]`, ...).
- Mojo frees a value right after its last use. Keep an owner alive past every
  call that receives its raw handle or pointer (`_ = owner^`), or tie the
  borrower to the owner's origin. Wrappers around borrowed handles (`Settings`,
  `FileSystem`, `MetricsNode`) must carry the owner's origin, or
  `con.settings().get(...)` on the connection's last line destroys the
  connection before `get` runs.
- Mojo does not stop a second mutable borrow from being created and kept, but
  it rejects two mutable borrows of one value passed to the same call. Objects
  shared by many borrowers (`Connection`) take `self` immutably when their Mojo
  fields don't change.
- `Iterator.__next__` may only raise `StopIteration`, which `for` loops
  swallow. Iteration that can fail (streaming results) uses a raising
  `__has_next__()` plus a non-raising `__next__()` (`ChunkIter`) and a standard
  `Iterator` of result-or-error elements next to it (`TryChunkIter`). Loop
  variables are immutable references; `for var x in ...` gives an owned element.
- Generic return types need `Movable & Deinitable`. Default argument values
  must be compile-time constants.
- A bare list literal passed to a generic parameter (`Value([1, 2])`) becomes
  an `Array[T, n]`, not a `List`, so generic APIs must map `Array` too. A
  `List[T]` overload next to a generic `T` one is ambiguous.
- V2 query results are always flat vectors; constant, dictionary and sequence
  vectors only reach callbacks. `ColumnReader` still handles all of them.
- Raw chunk kernels: `ColumnReader.unsafe_data()` is indexed through the
  selection; `is_flat()` means row i is at index i, `is_constant()` means every
  row is at index 0 (literals passed to functions). `Chunk.array_span[T](col)`
  gives the elements of a flat ARRAY column without copying.
- Nested types go through emberserde in `duckdb/serde.mojo`: `_Cursor` and
  `_Writer` check `is_mapped[T]()` first and handle scalars in their storage
  layout, everything else goes to emberserde. A struct read from DuckDB must be
  `Defaultable` when it has non-trivial fields. emberserde is a git dependency
  (tag `0.2.0`).
- Every V2 aggregate needs bind data that tells it apart (`_identity_bind`):
  DuckDB merges aggregate calls whose C++ callbacks match, and all C API
  aggregates share them.
- Extensions use `duckdb.Extension.run[init]` with the `C_STRUCT_UNSTABLE`
  footer (see `extensions/demo-extension`).
- Adding fields to structs on the hot read path (`ColumnMeta`, tree nodes)
  costs measurable speed even when reads never touch them (a `Dict` field cost
  25%). Keep rarely used data behind a pointer and rerun
  `benchmark/serde_bench.mojo`.

## SIMD kernels

- `duckdb/kernels/simd.mojo` holds the kernels;
  `duckdb.register_simd_math(con)` registers them as `mojo_sqrt`/`sin`/`cos`/
  `ln`/`exp`/`log10`. They ship in the package, nothing to build or `LOAD`.
- `extensions/mojo-kernel-overrides` overrides the built-in math functions and
  `sum`/`avg`/`min`/`max` via catalog mutation. CPP-ABI, version-locked to its
  DuckDB, not in the conda package. Build: `pixi run overrides-build`, details
  in its README and `benchmark/README.md`.

## Bumping versions

Bump Mojo and DuckDB with the checklists in `docs/updating-deps.md`.

## Packaging and publishing

`pixi build` and `conda.recipe/recipe.yaml` both build a conda package of the
bindings. They are separate and their pins must agree. Check command and
release steps: `docs/packaging.md`.

## Environments

- **default** - standard dev environment, precompiled libduckdb from conda-forge
- **gpu** - default plus `max-core` (accelerator APIs `max.gpu.*`, `layout`).
  Only `mojo-gpu-operator` needs it. Metal kernels on macOS need Xcode's
  separately downloaded Metal Toolchain component.
- **full** - the operator-replacement feature: source-builds DuckDB via
  `duckdb-from-source` (initialize the submodule first). operator-replacement
  is only a reference implementation. mojo-kernel-overrides and
  mojo-gpu-operator replaced it. On macOS 27 the conda linker cannot read the
  system SDK
  and the source build fails; a local workaround is a temporary
  `[workspace.build-variants]` entry
  `CONDA_BUILD_SYSROOT = ["/Library/Developer/CommandLineTools/SDKs/MacOSX26.5.sdk"]`
  in `pixi.toml` (do not commit it).
