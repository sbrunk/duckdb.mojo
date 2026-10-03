# Benchmarks

## End-to-end through DuckDB SQL: compare engines

DuckDB's own `benchmark_runner` runs `.benchmark` files warm (median of N iterations).
One driver compares stock DuckDB, cpu (mojo-kernel-overrides) and gpu (mojo-gpu-operator)
on the same workload.

```bash
pixi run bench-build                                   # build the runner (one-time)
pixi run bench-sql mojo_simd --engines=stock,cpu       # SIMD scalar/agg overrides
pixi run bench-sql gpu_xover --engines=stock,cpu,gpu   # GPU_COSINE vs CPU vs stock by N
pixi run bench-sql gpu_knn   --by-suffix               # top-k: per-file engine (stock/cpu/gpu)
pixi run bench-sql tpch/sf1/q0[16] --engines=stock,cpu,gpu   # TPC-H (built-in)
```

- `sql/<group>/*.benchmark`: the benchmark definitions. The driver copies them into the runner tree.
- `drivers/bench_runner.py`: the comparison driver. Use `--engines` to run the same files with
  each engine, or `--by-suffix` when each `<regime>_<engine>.benchmark` file is for one engine.
- `drivers/build_runner.sh`: builds `benchmark_runner` and applies the load-extension
  hook (`runner_load_extension.patch`). On NixOS, run the cmake step under
  `nix-shell -p cmake ninja gcc gnumake` (the script adds `-rdynamic`).

## Vector search (latency and recall): Mojo client harness

```bash
pixi run bench-knn        # single + batch cosine top-k: stock / cpu-simd / vss-HNSW / GPU
```

`mojo/knn_compare.mojo` uses the duckdb.mojo client. It loads each engine's extension
in its own connection and reports warm median latency and recall@k compared to exact
search. Tune `N/M/K/k` with its comptime constants. The GPU and vss rows are skipped
if they are not available.
`mojo/bench_util.mojo` holds the shared `warm_median` / `recall` helpers.

## Raw-kernel microbenchmarks (no DuckDB)

Mojo `perf_counter_ns` microbenchmarks that call kernels directly:
- `extensions/mojo-gpu-operator/bench/*.mojo`: mostly GPU correctness tests, run with
  `pixi run -e gpu gpu-op-test`; a few print timings.
- top-level `benchmark/*.mojo`: early SIMD/GPU math and reduction prototypes.
- `extensions/mojo-kernel-overrides/bench/benchmark.cpp`: standalone C++ timer comparing
  stock and Mojo (`pixi run overrides-bench`).
