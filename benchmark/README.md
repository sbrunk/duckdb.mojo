# Benchmarks

Two modalities; pick by what you're measuring.

## End-to-end (through DuckDB SQL) — compare engines

DuckDB's own `benchmark_runner` runs `.benchmark` files warm (median of N iters).
One driver compares **stock / cpu (mojo-kernel-overrides) / gpu (mojo-gpu-operator)**
on the same workload.

```bash
pixi run bench-build                                   # build the runner (one-time)
pixi run bench-sql mojo_simd --engines=stock,cpu       # SIMD scalar/agg overrides
pixi run bench-sql gpu_xover --engines=stock,cpu,gpu   # GPU_COSINE vs CPU vs stock by N
pixi run bench-sql gpu_knn   --by-suffix               # top-k: per-file engine (stock/cpu/gpu)
pixi run bench-sql tpch/sf1/q0[16] --engines=stock,cpu,gpu   # TPC-H (built-in)
```

- `sql/<group>/*.benchmark` — source of truth (staged into the runner tree by the driver).
- `drivers/bench_runner.py` — the unified compare driver (`--engines` toggle mode, or
  `--by-suffix` when each `<regime>_<engine>.benchmark` is engine-specific).
- `drivers/build_runner.sh` — builds `benchmark_runner` + applies the load-extension
  hook (`runner_load_extension.patch`). NixOS: run the cmake step under
  `nix-shell -p cmake ninja gcc gnumake` (the script adds `-rdynamic`).

## Vector search (latency + recall) — Mojo client harness

```bash
pixi run bench-knn        # single + batch cosine top-k: stock / cpu-simd / vss-HNSW / GPU
```

`mojo/knn_compare.mojo` drives the duckdb.mojo client, loads each engine's extension
in its own connection, reports warm median latency and recall@k vs exact. Tune
`N/M/K/k` via its comptime constants. GPU/vss rows self-skip if unavailable.
`mojo/bench_util.mojo` holds the shared `warm_median` / `recall` helpers.

## Raw-kernel microbenchmarks (no DuckDB)

Mojo `perf_counter_ns` microbenches that drive kernels directly:
- `packages/mojo-gpu-operator/bench/*.mojo` — GPU C-ABI latency/oracle tests/probes.
- top-level `benchmark/*.mojo` — SIMD/GPU math + reduction POCs.
- `packages/mojo-kernel-overrides/bench/benchmark.cpp` — standalone stock-vs-Mojo C++ timer
  (`pixi run overrides-bench`).
</content>
