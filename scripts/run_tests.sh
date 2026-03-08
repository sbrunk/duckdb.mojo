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
BUILD_DIR=$(mktemp -d)
trap "mv src.bak src; rm -rf $BUILD_DIR" EXIT

# Order tests: simple tests first to warm the compilation cache,
# then heavy appender tests (which use expensive generic monomorphization).
# This reduces peak memory for the heavy tests on memory-constrained CI.
SIMPLE_TESTS=(
    test/test_config.mojo
    test/test_database.mojo
    test/test_connection.mojo
    test/test_duckdb_type.mojo
    test/test_logical_type.mojo
    test/test_value.mojo
    test/test_vector.mojo
    test/test_chunk.mojo
    test/test_result.mojo
    test/test_query_results.mojo
    test/test_typed_api.mojo
    test/test_scalar_function.mojo
    test/test_aggregate_function.mojo
    test/test_table_function.mojo
)
HEAVY_TESTS=(
    test/test_appender.mojo
    test/test_appender_numeric.mojo
    test/test_appender_temporal.mojo
    test/test_appender_list.mojo
    test/test_appender_map.mojo
    test/test_appender_variant.mojo
)

ALL_TESTS=("${SIMPLE_TESTS[@]}" "${HEAVY_TESTS[@]}")

# Build all test binaries first, then run them.
# Building sequentially populates the Mojo compilation cache, reducing
# peak memory for subsequent compilations.
for f in "${ALL_TESTS[@]}"; do
    name=$(basename "$f" .mojo)
    echo "--- Building: $f ---"
    mojo build "$f" -o "$BUILD_DIR/$name"
done

for f in "${ALL_TESTS[@]}"; do
    name=$(basename "$f" .mojo)
    echo "--- Running: $name ---"
    "$BUILD_DIR/$name"
done
