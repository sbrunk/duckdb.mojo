#!/bin/bash
# Compile each test against the pre-built mojopkg instead of recompiling
# from source. The Mojo compiler recompiles the entire duckdb package from
# the src/ directory for each test file, using 9+GB peak memory.
# By temporarily hiding src/, we force it to use the pre-built mojopkg.
set -e

# Temporarily hide src/ so mojo uses the pre-built mojopkg
mv src src.bak
trap "mv src.bak src" EXIT

for f in test/test_*.mojo; do
    mojo run "$f"
done
