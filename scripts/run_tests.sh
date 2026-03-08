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

# Heavy tests use Dict/List/Variant generics that require expensive
# monomorphization. We compile them with -j 2 (limit parallelism) to
# reduce peak memory usage so they can run on CI runners with ~17 GB
# (7 GB RAM + 10 GB swap).
HEAVY_TESTS=(
    test/test_typed_api_scalars.mojo
    test/test_typed_api_mojo_type.mojo
    test/test_typed_api_table_structs.mojo
    test/test_typed_api_tuples.mojo
    test/test_typed_api_collections.mojo
    test/test_appender_list.mojo
    test/test_appender_map_variant.mojo
)

for f in "${TESTS[@]}"; do
    echo "--- Running: $f ---"
    mojo run "$f"
done

for f in "${HEAVY_TESTS[@]}"; do
    echo "--- Running (low-memory): $f ---"
    mojo run -j 2 "$f"
done
