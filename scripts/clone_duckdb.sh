#!/bin/sh
# Ensure the DuckDB source tree is available for code generation and for building
# DuckDB's benchmark_runner / a source build.
#
# The source lives in the `third_party/duckdb` git submodule, pinned to the
# release tag the FFI bindings were generated against (see .gitmodules). This
# script just initializes/updates that submodule (shallow), then sanity-checks
# that the pinned version matches the installed `duckdb` so a forgotten bump
# can't silently desync the bindings from the runtime library.
set -e

DIR="third_party/duckdb"

if [ ! -e "$DIR/CMakeLists.txt" ]; then
    echo "Initializing DuckDB source submodule at $DIR (shallow)..."
    git submodule update --init --depth 1 "$DIR"
else
    # Make sure the working tree matches the pinned gitlink (e.g. after a bump).
    git submodule update --depth 1 "$DIR" 2>/dev/null || true
fi

# Sanity check: the submodule tag should match the installed DuckDB. This is a
# warning, not a hard error, so the bindings can still be regenerated for a
# version that isn't installed locally.
SRC_TAG=$(git -C "$DIR" describe --tags --always 2>/dev/null || echo "unknown")
if command -v duckdb >/dev/null 2>&1; then
    LIB_TAG="v$(duckdb --version | head -1 | awk '{print $1}' | sed 's/^v//')"
    if [ "$SRC_TAG" != "$LIB_TAG" ]; then
        echo "WARNING: $DIR is at $SRC_TAG but the installed duckdb is $LIB_TAG."
        echo "         Update the submodule pin (and the FFI bindings) to match:"
        echo "             git -C $DIR fetch --depth 1 origin tag $LIB_TAG"
        echo "             git -C $DIR checkout tags/$LIB_TAG && git add $DIR"
    fi
fi

echo "DuckDB source ready at $DIR ($SRC_TAG)"
