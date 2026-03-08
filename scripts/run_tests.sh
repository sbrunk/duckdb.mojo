#!/bin/bash
# Compile each test against the pre-built mojopkg instead of recompiling
# from source. The Mojo compiler recompiles the entire duckdb package from
# the src/ directory for each test file, using 9+GB peak memory.
# By temporarily hiding src/, we force it to use the pre-built mojopkg.
set -e

# Debug: show what mojo sees
echo "=== Debug: import path ==="
cat "$MODULAR_HOME/modular.cfg" 2>/dev/null | grep import_path || echo "no import_path found"
echo "=== Debug: looking for duckdb packages ==="
find . -maxdepth 2 -name "duckdb*" -o -name "src" | head -20
echo "=== Debug: MODULAR_HOME=$MODULAR_HOME ==="
echo "=== Debug: checking mojopkg in import path ==="
IMPORT_PATH=$(cat "$MODULAR_HOME/modular.cfg" 2>/dev/null | grep import_path | cut -d= -f2 | tr -d ' ')
ls -la "$IMPORT_PATH/duckdb.mojopkg" 2>/dev/null || echo "no duckdb.mojopkg in import_path"
echo "=== End debug ==="

# Temporarily hide src/ so mojo uses the pre-built mojopkg
mv src src.bak
trap "mv src.bak src" EXIT

echo "=== After mv: looking for duckdb packages ==="
find . -maxdepth 2 -name "duckdb*" -o -name "src" | head -20

for f in test/test_*.mojo; do
    echo "--- Running: $f ---"
    mojo run "$f"
done
