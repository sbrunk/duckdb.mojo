#!/usr/bin/env bash
# Build DuckDB's own benchmark_runner with the one-line "LOAD an extension at init" hook,
# so the mojo_overrides extension can be loaded into every benchmark via DUCKDB_BENCH_EXTENSION.
#
# Reuses the existing .duckdb-src checkout + build/release dir (no second DuckDB clone or build):
#   1. apply runner_load_extension.patch if the hook isn't already present (idempotent),
#   2. copy our committed micro/mojo_simd/*.benchmark files into the source tree,
#   3. (re)configure + build the benchmark_runner target (tpch extension included, for TPC-H).
#
# Run via `pixi run overrides-bench-runner-build` (so cmake + the conda toolchain are on PATH).
#
# Overridable env:
#   DUCKDB_SRC   DuckDB source checkout         (default: <repo>/.duckdb-src)
#   BUILD_DIR    cmake build dir                (default: $DUCKDB_SRC/build/release)
#   JOBS         parallel build jobs            (default: number of CPUs)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DUCKDB_SRC="${DUCKDB_SRC:-$ROOT/.duckdb-src}"
BUILD_DIR="${BUILD_DIR:-$DUCKDB_SRC/build/release}"
JOBS="${JOBS:-$( (nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4) )}"

[[ -d "$DUCKDB_SRC" ]] || { echo "ERROR: $DUCKDB_SRC missing — run 'pixi run clone-duckdb' first" >&2; exit 1; }

IB="$DUCKDB_SRC/benchmark/interpreted_benchmark.cpp"
if grep -q DUCKDB_BENCH_EXTENSION "$IB"; then
	echo "==> runner load-extension hook already present"
else
	echo "==> applying runner_load_extension.patch to $DUCKDB_SRC"
	git -C "$DUCKDB_SRC" apply "$HERE/runner_load_extension.patch"
fi

echo "==> copying micro/mojo_simd benchmarks into the source tree"
mkdir -p "$DUCKDB_SRC/benchmark/micro/mojo_simd"
cp "$HERE/micro/mojo_simd/"*.benchmark "$DUCKDB_SRC/benchmark/micro/mojo_simd/"

echo "==> configuring + building benchmark_runner ($JOBS jobs)"
cmake -S "$DUCKDB_SRC" -B "$BUILD_DIR" -DCMAKE_BUILD_TYPE=Release \
	-DBUILD_BENCHMARKS=1 -DBUILD_EXTENSIONS=tpch >/dev/null
cmake --build "$BUILD_DIR" --target benchmark_runner -j "$JOBS"

echo "==> done: $BUILD_DIR/benchmark/benchmark_runner"
