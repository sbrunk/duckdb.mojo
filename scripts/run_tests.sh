#!/bin/bash
# Run tests, compiling the duckdb package from source for each test file.
#
set -e

# Optional extra flags for every mojo build/run, for example
# MOJO_TARGET_FLAGS=--target-cpu=x86-64-v3 to pin the codegen target. Unset by
# default, so builds target the native CPU.
read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"

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
    test/test_module_api.mojo
    test/test_relation.mojo
    test/test_lifecycle.mojo
    test/test_readers.mojo
    test/test_libduckdb_v2.mojo
    test/v2/test_v2_foundations.mojo
    test/v2/test_v2_results.mojo
    test/v2/test_v2_relation.mojo
    test/v2/test_v2_nested.mojo
    test/v2/test_v2_functions.mojo
    test/v2/test_v2_aggregates.mojo
    test/v2/test_v2_tables.mojo
    test/test_typed_access.mojo
    test/test_errors.mojo
)

# Generic tests compile more slowly (heavy monomorphization), so they run with
# -j 2. The struct/list/dict/variant ones used to need >21 GB peak memory and
# were run only locally; on Mojo 1.2 each compiles in under 1 GB.
GENERIC_TESTS=(
    test/test_typed_api_scalars.mojo
    test/test_typed_api_tuples.mojo
    test/test_prepared_statement.mojo
    test/test_result_fetch.mojo
    test/test_typed_api_mojo_type.mojo
    test/test_typed_api_table_structs.mojo
    test/test_typed_api_collections.mojo
    test/test_appender_list.mojo
    test/test_appender_map_variant.mojo
)

for f in "${TESTS[@]}"; do
    echo "--- Running: $f ---"
    mojo run "${MOJO_TARGET[@]}" "$f"
done

if [[ "${CI:-}" == "true" && -z "${DUCKDB_MOJO_FULL_TESTS:-}" && "$(uname)" != "Linux" ]]; then
    echo "--- Skipping generic tests on macOS CI (use full test run to include) ---"
else
    for f in "${GENERIC_TESTS[@]}"; do
        echo "--- Running (-j 2): $f ---"
        mojo run -j 2 "${MOJO_TARGET[@]}" "$f"
    done
fi
