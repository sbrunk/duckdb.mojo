#!/usr/bin/env bash
# Warm A/B (stock vs the mojo_gpu_operator extension) over the GPU-operator win classes
# that DuckDB does NOT accelerate: f64 transcendental aggregates (sum/avg of
# sqrt/exp/ln/log10/log2/power/...), statistical aggregates (stddev/var/covar/corr/regr_*),
# both ungrouped and dense-grouped. The INT128 TPC-H classes (Q1/Q5/Q6/Q14) are covered by
# DuckDB's own benchmark/tpch/sf1/* — run those with `overrides-bench-runner 'benchmark/tpch/sf1/.*'`
# and DUCKDB_BENCH_EXTENSION pointed here.
#
# Mirrors mojo-kernel-overrides/benchmark: reuses the SAME stock benchmark_runner (built by
# `pixi run overrides-bench-runner-build`, which applies the load-extension hook) and the SAME
# DUCKDB_BENCH_EXTENSION toggle — only the extension path + the GPU_OP_* flags differ.
#
# The benchmark files self-generate a DECIMAL dataset via a `cache` directive (no tpch / external
# db dependency) — DECIMAL because the f64 aggregate paths decline on raw DOUBLE columns.
#
#   pixi run gpu-op-bench               # all gpuop win-class benchmarks
#   pixi run gpu-op-bench 'gpuop/pow.*' # a subset (regex matches the benchmark/ name)
#
# These f64 paths are NVIDIA-only: on Apple/AMD they decline cleanly -> ~1.0x (no win, no error).
# Run on an NVIDIA box (the warm wins are 2.5-8.6x at this scale on an RTX 4090).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DUCKDB_SRC="${DUCKDB_SRC:-$ROOT/.duckdb-src}"
EXT="${DUCKDB_BENCH_EXTENSION:-$ROOT/packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension}"
SUB="${1:-gpuop/.*}"   # benchmark-name regex (relative to benchmark/)

[[ -f "$EXT" ]] || { echo "ERROR: gpu-op extension missing at $EXT — run 'pixi run gpu-op-build'" >&2; exit 1; }
[[ -d "$DUCKDB_SRC" ]] || { echo "ERROR: $DUCKDB_SRC missing — run 'pixi run clone-duckdb'" >&2; exit 1; }

echo "==> copying gpuop benchmarks into the source tree"
mkdir -p "$DUCKDB_SRC/benchmark/gpuop"
cp "$HERE/gpuop/"*.benchmark "$DUCKDB_SRC/benchmark/gpuop/"

# Default-on for the f64 classes is harmless here even after they ship on-by-default (=1 is the
# accelerated posture; the paths fail-closed + decline on non-NVIDIA). COLPOOL=2 = the shipped
# warm-residency config.
export DUCKDB_BENCH_EXTENSION="$EXT"
export GPU_OP_TRANSCENDENTAL="${GPU_OP_TRANSCENDENTAL:-1}"
export GPU_OP_STATS="${GPU_OP_STATS:-1}"
export GPU_OP_COLPOOL="${GPU_OP_COLPOOL:-2}"

exec bash "$ROOT/packages/mojo-kernel-overrides/benchmark/run_runner_compare.sh" "benchmark/$SUB"
