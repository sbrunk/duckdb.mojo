#!/usr/bin/env bash
# Build everything, then run the stock-vs-Mojo benchmark with the extension loaded.
# Run via `pixi run overrides-bench [-- --threads=N --rows=N]`.
#
# Overridable env:
#   DUCKDB_INCLUDE  (default: $CONDA_PREFIX/include)
#   DUCKDB_LIB      (default: $CONDA_PREFIX/lib)   — libduckdb to link the driver against
#   CXX             (default: clang++)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD="$HERE/build"

DUCKDB_INCLUDE="${DUCKDB_INCLUDE:-${CONDA_PREFIX:?set CONDA_PREFIX or DUCKDB_INCLUDE}/include}"
DUCKDB_LIB="${DUCKDB_LIB:-${CONDA_PREFIX}/lib}"
CXX="${CXX:-clang++}"

bash "$HERE/build.sh"

echo "==> benchmark driver"
"$CXX" -std=c++17 -O2 "$HERE/bench/benchmark.cpp" -I "$DUCKDB_INCLUDE" \
	-L "$DUCKDB_LIB" -lduckdb -Wl,-rpath,"$DUCKDB_LIB" -o "$BUILD/benchmark"

# The extension is self-contained (kernels linked in); no DUCKDB_MOJO_LIB needed.
echo "==> running"
"$BUILD/benchmark" "$BUILD/mojo_overrides.duckdb_extension" "$@"
