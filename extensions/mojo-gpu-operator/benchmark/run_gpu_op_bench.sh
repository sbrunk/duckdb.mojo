#!/usr/bin/env bash
# Warm comparison (stock vs the mojo_gpu_operator extension) over the query classes where
# the GPU operator wins and DuckDB has no acceleration: f64 transcendental aggregates (sum/avg
# of sqrt/exp/ln/log10/log2/power/...) and statistical aggregates
# (stddev/var/covar/corr/regr_*), both ungrouped and dense-grouped. The INT128 TPC-H classes
# (Q1/Q5/Q6/Q14) are covered by DuckDB's own benchmark/tpch/sf1/*. Run those with
# `pixi run bench-sql tpch/sf1/q0[13456] --engines=stock,gpu`.
#
# Reuses the consolidated harness (benchmark/README.md): the same stock benchmark_runner
# (built by `pixi run bench-build`) and the unified driver. This wrapper stages its own
# self-generating `gpuop/` benchmark group and exports the GPU_OP_* flags, then hands the
# stock vs gpu comparison to benchmark/drivers/bench_runner.py.
#
# The benchmark files generate their own DECIMAL dataset via a `cache` directive (no tpch or
# external db dependency). They use DECIMAL because the f64 aggregate paths decline on raw
# DOUBLE columns.
#
#   bash extensions/mojo-gpu-operator/benchmark/run_gpu_op_bench.sh                # all win-class benchmarks
#   bash extensions/mojo-gpu-operator/benchmark/run_gpu_op_bench.sh 'gpuop/pow.*'  # a subset (regex matches the benchmark/ name)
#
# These f64 paths are NVIDIA-only. On Apple/AMD they decline cleanly, giving ~1.0x (no win, no error).
# Run on an NVIDIA box (the warm wins are 2.5-8.6x at this scale on an RTX 4090).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DUCKDB_SRC="${DUCKDB_SRC:-$ROOT/third_party/duckdb}"
EXT="${DUCKDB_BENCH_EXTENSION:-$ROOT/extensions/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension}"
SUB="${1:-gpuop/.*}"   # benchmark-name regex (relative to benchmark/)

[[ -f "$EXT" ]] || { echo "ERROR: gpu-op extension missing at $EXT, run 'pixi run gpu-op-build'" >&2; exit 1; }
[[ -d "$DUCKDB_SRC" ]] || { echo "ERROR: $DUCKDB_SRC missing, run 'pixi run clone-duckdb'" >&2; exit 1; }

echo "==> copying gpuop benchmarks into the source tree"
mkdir -p "$DUCKDB_SRC/benchmark/gpuop"
cp "$HERE/gpuop/"*.benchmark "$DUCKDB_SRC/benchmark/gpuop/"

# Turning the f64 classes on here is harmless even once they ship enabled by default (=1 turns
# acceleration on, and on non-NVIDIA GPUs the paths decline safely). COLPOOL=2 is the shipped
# config for keeping columns resident on the GPU between queries.
export DUCKDB_BENCH_EXTENSION="$EXT"
export GPU_OP_TRANSCENDENTAL="${GPU_OP_TRANSCENDENTAL:-1}"
export GPU_OP_STATS="${GPU_OP_STATS:-1}"
export GPU_OP_COLPOOL="${GPU_OP_COLPOOL:-2}"

# Delegate to the unified driver (toggle mode, stock vs gpu). SUB carries a '/', so
# bench_runner treats it as a runner-tree path (benchmark/$SUB.*); the exported GPU_OP_*
# flags are inherited by the runner subprocess.
exec python3 "$ROOT/benchmark/drivers/bench_runner.py" "$SUB" --engines=stock,gpu
