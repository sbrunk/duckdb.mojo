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

# Tests that compile quickly (no heavy generic monomorphization)
TESTS=(
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
    test/test_scalar_function.mojo
    test/test_aggregate_function.mojo
    test/test_table_function.mojo
    test/test_appender.mojo
)

# Medium tests use generics but fit in ~17 GB with -j 2.
MEDIUM_TESTS=(
    test/test_typed_api_scalars.mojo
    test/test_typed_api_tuples.mojo
    test/test_appender_list.mojo
    test/test_appender_map_variant.mojo
)

# Heavy tests use struct/collection generics that require expensive
# monomorphization. We compile them with -j 1 (single thread) to
# minimize peak memory for CI runners with only 7 GB RAM.
HEAVY_TESTS=(
    test/test_typed_api_mojo_type.mojo
    test/test_typed_api_table_structs.mojo
    test/test_typed_api_collections.mojo
)

for f in "${TESTS[@]}"; do
    echo "--- Running: $f ---"
    mojo run "$f"
done

for f in "${MEDIUM_TESTS[@]}"; do
    echo "--- Running (-j 2): $f ---"
    mojo run -j 2 "$f"
done

for f in "${HEAVY_TESTS[@]}"; do
    echo "--- Running (-j 1): $f ---"
    mojo run -j 1 "$f"
done
