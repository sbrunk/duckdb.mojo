#!/usr/bin/env bash
# Build the Mojo GPU kernels + the mojo_gpu_operator DuckDB extension.
# Run via `pixi run gpu-op-build` (so mojo + the conda libduckdb headers are on PATH).
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

MOJO_LIB="${MOJO_LIB:-${CONDA_PREFIX:?}/lib}"

# Platform-conditional link flags + shared-lib extension + self-relative rpath.
# macOS: .dylib + @loader_path; Linux (incl. the NVIDIA CI targets): .so + $ORIGIN.
case "$(uname -s)" in
Darwin) SOFLAGS=(-undefined dynamic_lookup); SO=dylib; ORIGIN='@loader_path' ;;
*) SOFLAGS=(-Wl,--allow-shlib-undefined); SO=so; ORIGIN='$ORIGIN' ;;
esac

# The GPU kernels need the Mojo GPU/AsyncRT runtime, so they are built as a
# self-linking shared library (not a bare object): `--emit shared-lib` pulls in
# the runtime (libAsyncRTMojoBindings / libKGENCompilerRTShared via @rpath). The
# extension links this companion dylib; rpaths point at it (@loader_path) and at
# the env lib dir (the Mojo runtime). This is why GPU can't be a single .so the
# way the SIMD-only kernel-overrides extension is.
echo "==> Mojo GPU kernels -> build/libmojo_gpu_kernels.$SO (shared-lib, runtime linked)"
mojo build --emit shared-lib "$HERE/src/gpu_kernels.mojo" -o "$BUILD/libmojo_gpu_kernels.$SO"

# NOTE: descriptor.mojo is no longer built/linked separately. Its pure
# RawPlan->descriptor logic is `import`ed by gpu_kernels.mojo, and ALL the
# mojo_gpu_build_descriptor / mojo_gpu_desc_* C-ABI @export wrappers now live in
# gpu_kernels.mojo (the root build file), so they ship in the kernel dylib in the
# SAME compilation unit as the GPU kernels -- required for the Stage-2 shuttle,
# whose pin_finalize must read the descriptor AND run kernels.

echo "==> mojo_gpu_operator extension (links the kernel companion dylib)"
"$CXX" -std=c++17 -O2 -fPIC -shared "${SOFLAGS[@]}" \
	"$HERE/src/gpu_operator.cpp" -I "$DUCKDB_INCLUDE" -lm \
	-L "$BUILD" -lmojo_gpu_kernels \
	-Wl,-rpath,"$ORIGIN" -Wl,-rpath,"$MOJO_LIB" \
	-o "$BUILD/mojo_gpu_operator.duckdb_extension"

echo "==> append CPP metadata footer ($DUCKDB_VERSION)"
python3 "$ROOT/scripts/append_extension_metadata.py" "$BUILD/mojo_gpu_operator.duckdb_extension" \
	--abi-type CPP --duckdb-version "$DUCKDB_VERSION"

echo "==> done: $BUILD/mojo_gpu_operator.duckdb_extension"
