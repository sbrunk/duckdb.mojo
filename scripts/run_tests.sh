#!/bin/bash
# Run tests against the pre-built duckdb.mojopkg (created by the build task).
# The mojopkg must exist in CWD before this script runs.
# We hide src/ to prevent the Mojo compiler from recompiling from source,
# which uses 9+GB peak memory due to __extension block monomorphization.
set -e

if [ ! -f "./duckdb.mojopkg" ]; then
    echo "ERROR: duckdb.mojopkg not found in CWD. Run 'pixi run build' first."
    exit 1
fi

# Hide src/ so mojo uses the pre-built mojopkg
mv src src.bak
trap "mv src.bak src" EXIT

for f in test/test_*.mojo; do
    echo "--- Running: $f ---"
    mojo run "$f"
done
