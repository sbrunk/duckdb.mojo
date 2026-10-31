#!/usr/bin/env bash
# Build the test extensions. Run via `pixi run build-test-extension`.
#
# V2 C API (one source, compiled once, copied under three names; DuckDB calls
# <file name>_init_c_api_v2, so each copy runs a different entrypoint):
#   mojo_v2.duckdb_extension         unstable target (C_STRUCT_UNSTABLE)
#   mojo_v2_fail.duckdb_extension    unstable target, init raises
#   mojo_v2_stable.duckdb_extension  stable target (C_STRUCT v2.0.0)
# V2 high-level API (duckdb.Extension):
#   mojo_v2_api.duckdb_extension     unstable target (C_STRUCT_UNSTABLE)
set -euo pipefail

read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
BUILD="$HERE/build"
META="python3 $ROOT/scripts/append_extension_metadata.py"
mkdir -p "$BUILD"

mojo build "${MOJO_TARGET[@]}" "$HERE/src/test_v2_extension.mojo" --emit shared-lib -o "$BUILD/mojo_v2.so"
cp "$BUILD/mojo_v2.so" "$BUILD/mojo_v2.duckdb_extension"
cp "$BUILD/mojo_v2.so" "$BUILD/mojo_v2_fail.duckdb_extension"
cp "$BUILD/mojo_v2.so" "$BUILD/mojo_v2_stable.duckdb_extension"
rm "$BUILD/mojo_v2.so"
$META "$BUILD/mojo_v2.duckdb_extension" --abi-type C_STRUCT_UNSTABLE
$META "$BUILD/mojo_v2_fail.duckdb_extension" --abi-type C_STRUCT_UNSTABLE
$META "$BUILD/mojo_v2_stable.duckdb_extension" --capi-version v2.0.0

mojo build "${MOJO_TARGET[@]}" "$HERE/src/test_v2_api_extension.mojo" --emit shared-lib -o "$BUILD/mojo_v2_api.duckdb_extension"
$META "$BUILD/mojo_v2_api.duckdb_extension" --abi-type C_STRUCT_UNSTABLE
