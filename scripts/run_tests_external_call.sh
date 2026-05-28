#!/bin/bash
# Run tests using the external_call FFI path (-D USE_DLHANDLE=false).
#
# This builds each test file with `mojo build` and links against libduckdb
# directly, verifying that struct-by-value functions work correctly via
# external_call without the C shim library at runtime.
#
# Usage: pixi run test-external-call
set -e

LIBDIR=".pixi/envs/default/lib"

if [ ! -d "$LIBDIR" ]; then
    echo "ERROR: $LIBDIR not found. Run inside pixi environment."
    exit 1
fi

# Tests that exercise the struct-by-value workaround functions
TESTS=(
    test/test_duckdb_type.mojo
    test/test_value.mojo
    test/test_connection.mojo
    test/test_result.mojo
    test/test_appender.mojo
)

PASSED=0
FAILED=0
for f in "${TESTS[@]}"; do
    echo "--- Building (external_call): $f ---"
    out="/tmp/$(basename "$f" .mojo)_ext"
    if mojo build "$f" -o "$out" \
        -D USE_DLHANDLE=false \
        -Xlinker "-L$LIBDIR" -Xlinker -lduckdb -Xlinker -lduckdb_mojo_helpers; then
        echo "--- Running: $f ---"
        if DYLD_LIBRARY_PATH="$LIBDIR" LD_LIBRARY_PATH="$LIBDIR" "$out"; then
            PASSED=$((PASSED + 1))
        else
            echo "FAIL: $f"
            FAILED=$((FAILED + 1))
        fi
        rm -f "$out"
    else
        echo "BUILD FAIL: $f"
        FAILED=$((FAILED + 1))
    fi
done

echo ""
echo "=== external_call path: $PASSED passed, $FAILED failed (of ${#TESTS[@]} test files) ==="
if [ "$FAILED" -gt 0 ]; then
    exit 1
fi
