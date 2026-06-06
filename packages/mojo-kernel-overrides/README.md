# mojo-kernel-overrides

A DuckDB extension that transparently dispatches selected built-in scalar and
aggregate functions to Mojo SIMD kernels without patching to DuckDB. You can load
it into a normal libduckdb and the rewrites apply for the rest of the session.

## What it overrides

| function(s) | type | mechanism |
|---|---|---|
| `sqrt sin cos ln exp log10` | scalar `DOUBLE→DOUBLE` | swap `function` |
| `sum` `avg` | aggregate `DOUBLE` | swap `simple_update` (ungrouped) |
| `min` `max` | aggregate `DOUBLE` + `FLOAT` | wrap the `bind` callback, swap resolved `simple_update` |

Everything else, and any non-FLAT / null-containing / grouped input, falls back to the
original built-in (the original pointer is captured at load), so results are unchanged.

## How it works

At `LOAD`, the extension's init mutates the built-in **catalog function entries in place**:

- `ScalarFunctionCatalogEntry::functions` / `AggregateFunctionCatalogEntry::functions`
  are public & mutable, and the binder copies the function **by value** at bind time so a
  mutation done at load is picked up by every later query.
- It captures each original function pointer for the slow-path fallback (so no
  DuckDB-internal operator types are needed) and, for FLAT/all-valid input, calls a Mojo
  SIMD kernel (linked into the extension) over the raw column buffer.
- `sum`/`avg` have concrete per-type overloads → override `simple_update` directly.
  `min`/`max` are registered `ANY→ANY` with a **bind callback** (the concrete per-type
  function is produced at bind time) → the extension **wraps the bind**, runs the original,
  then swaps the resolved `f64`/`f32` `simple_update`.
- Aggregate state structs (`SumState`/`AvgState`/`MinMaxState`) are mirrored and guarded by a
  runtime `state_size` check before swapping.

## Build & run (pixi)

```bash
pixi run overrides-build                 # build the self-contained mojo_overrides.duckdb_extension
pixi run overrides-bench                 # build, load, print stock-vs-Mojo table (50M rows, 1 thread)
pixi run overrides-bench -- --threads=8 --rows=20000000
pixi run overrides-clean
```

Artifacts land in `build/`:
- `mojo_overrides.duckdb_extension` — the loadable extension (CPP ABI), self-contained:
  the Mojo SIMD kernels are emitted as an object (`capi.o`) and linked straight in, so
  there is no separate kernel lib and no runtime `dlopen` of one.
- `libmojo_simd.dylib` (`.so` on Linux) — the same kernels as a standalone C-ABI lib,
  built for direct kernel use / the source-patch path; not needed by the extension.

Builds against the default pixi env's libduckdb (`$CONDA_PREFIX/include` + `/lib`). Override
with `DUCKDB_INCLUDE` / `DUCKDB_LIB` / `DUCKDB_VERSION` to target another DuckDB tree.

## Use from the DuckDB CLI / any libduckdb

The extension is unsigned and uses the CPP ABI, so it must be loaded with unsigned
extensions allowed, into a libduckdb of the **exact same version** it was built against.
It is self-contained (kernels linked in), so nothing else needs to be on the path:

```bash
duckdb -unsigned -c "
  LOAD '$PWD/packages/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension';
  SELECT min(i::DOUBLE), sum(i::DOUBLE) FROM range(50000000) t(i);"
```

## Use from the Mojo client

Build the extension first (`pixi run overrides-build`), then load it like any other
extension by allowing unsigned extensions at connect and issuing `LOAD`:

```mojo
from duckdb import DuckDB
from duckdb.config import Config

var config = Config()
config.set("allow_unsigned_extensions", "true")
var conn = DuckDB.connect(":memory:", config)
_ = conn.execute("LOAD 'packages/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension'")
```

`LOAD` validates the CPP metadata footer, so a `.so` built for a different DuckDB
version is rejected with a clear error rather than crashing.

### Advanced: install without `LOAD`

The `.so` also exports a plain C entry point
`register_mojo_overrides(duckdb_connection)`. A host that holds a connection but cannot
issue `LOAD` (e.g. an embedder) can `dlopen` the `.so` and call it directly. This skips
the loader, so there is no signature or version-footer check; the caller is responsible
for matching the DuckDB version. The library must stay loaded for the process, since the
catalog holds function pointers into it.

## Caveats

- **Version/ABI locked**: a CPP-ABI extension only loads into the exact DuckDB version it was
  built against (footer is `--duckdb-version`, default `v1.5.3`). Rebuild per version.
- Needs the C++ **internal** headers + an ABI-matched libduckdb — not the stable C extension API.
- **State-layout coupling**: the mirrored aggregate state structs must match DuckDB's; the
  `state_size` runtime check guards gross mismatches but not silent field-order changes.
- Requires `allow_unsigned_extensions` (set in the benchmark driver; use `-unsigned` in the CLI).
- Mutates shared system-catalog functions for the whole DB instance; applied once at load.
- **Correctness**: SIMD reductions sum/compare in a different order, so floating-point results
  can differ from stock in the last ~1 ULP (verified within 1e-6 relative). Domain checks on
  the fast path are skipped (e.g. `sqrt`/`ln` of negatives return NaN instead of throwing).

## Files

The SIMD kernels themselves live in [`duckdb/kernels`](../../duckdb/kernels) and are
shared with the scalar UDF helpers; this package only adds the C++ override glue.

- `src/capi_shim.mojo` — `@export` C-ABI wrappers over `duckdb.kernels.simd`, emitted as an object and linked into the extension
- `src/mojo_overrides.cpp` — the C++ extension (catalog mutation + wrappers, two entry points)
- `bench/benchmark.cpp` — standalone stock-vs-Mojo timing driver
- `build.sh` / `run-benchmark.sh` — invoked by the pixi tasks
