#!/usr/bin/env bash
# Build the Mojo SIMD kernels + the mojo_overrides DuckDB extension.
# Run via `pixi run overrides-build` (so mojo + the conda libduckdb headers are on PATH).
#
# Overridable env:
#   DUCKDB_INCLUDE  duckdb headers dir   (default: $CONDA_PREFIX/include)
#   DUCKDB_VERSION  for the CPP footer   (default: v1.5.3)
#   CXX             C++ compiler         (default: clang++)
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BUILD="$HERE/build"
mkdir -p "$BUILD"

DUCKDB_INCLUDE="${DUCKDB_INCLUDE:-${CONDA_PREFIX:?set CONDA_PREFIX or DUCKDB_INCLUDE}/include}"
DUCKDB_VERSION="${DUCKDB_VERSION:-v1.5.3}"
CXX="${CXX:-clang++}"

case "$(uname -s)" in
Darwin) SO=dylib; SOFLAGS=(-undefined dynamic_lookup) ;;
*) SO=so; SOFLAGS=(-Wl,--allow-shlib-undefined) ;;
esac

echo "==> Mojo SIMD kernels -> build/capi.o (object, no Mojo runtime deps)"
mojo build --emit object "$HERE/src/capi_shim.mojo" -o "$BUILD/capi.o"

echo "==> mojo_overrides extension (single self-contained .so, kernels linked in)"
# DuckDB symbols are resolved from the host libduckdb at load time (dynamic lookup);
# the Mojo kernel object is linked straight in, so there is no separate kernel lib
# and no dlopen. The kernels' only external dep is libm (cos/sin).
"$CXX" -std=c++17 -O2 -fPIC -shared "${SOFLAGS[@]}" \
	"$HERE/src/mojo_overrides.cpp" "$BUILD/capi.o" -I "$DUCKDB_INCLUDE" -lm \
	-o "$BUILD/mojo_overrides.duckdb_extension"

# Standalone kernel lib (same object) for the source-patch path / direct kernel use.
"$CXX" -shared "${SOFLAGS[@]}" "$BUILD/capi.o" -lm -o "$BUILD/libmojo_simd.$SO"

echo "==> append CPP metadata footer ($DUCKDB_VERSION)"
python3 "$ROOT/scripts/append_extension_metadata.py" "$BUILD/mojo_overrides.duckdb_extension" \
	--abi-type CPP --duckdb-version "$DUCKDB_VERSION"

echo "==> done:"
echo "    $BUILD/mojo_overrides.duckdb_extension"
echo "    $BUILD/libmojo_simd.$SO"
