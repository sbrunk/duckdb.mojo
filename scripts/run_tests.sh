#!/bin/bash
# Compile each test against the pre-built mojopkg instead of recompiling
# from source. The Mojo compiler recompiles the entire duckdb package from
# the src/ directory for each test file, using 9+GB peak memory.
# By hiding src/ and placing the mojopkg in CWD, we force mojo to use it.
set -e

# Get the mojopkg path from pixi env
IMPORT_PATH=$(cat "$MODULAR_HOME/modular.cfg" 2>/dev/null | grep import_path | cut -d= -f2 | tr -d ' ')
MOJOPKG="$IMPORT_PATH/duckdb.mojopkg"

if [ ! -f "$MOJOPKG" ]; then
    echo "ERROR: duckdb.mojopkg not found at $MOJOPKG"
    exit 1
fi

# Copy mojopkg to CWD so mojo finds it immediately
cp "$MOJOPKG" ./duckdb.mojopkg
trap "rm -f ./duckdb.mojopkg; [ -d src.bak ] && mv src.bak src" EXIT

# Hide src/ so mojo can't find it
mv src src.bak

echo "Using mojopkg: $(ls -la ./duckdb.mojopkg)"

# Dump all mojo-related env vars for debugging
echo "=== Environment ==="
env | grep -iE "mojo|modular|import|path" | sort
echo "==="

# Try a quick compile to test mojopkg works
echo "=== Quick compile test ==="
time mojo run -I . test/test_config.mojo 2>&1 | tail -3
echo "=== End quick test ==="

for f in test/test_*.mojo; do
    echo "--- Running: $f ($(date)) ---"
    time mojo run -I . "$f"
    echo "--- Completed: $f ($(date)) ---"
done
