#!/bin/bash
# Run tests, compiling the duckdb package from source for each test file.
#
set -e

# Optional Mojo codegen target override. Set ONLY in CI (see
# .github/workflows/test.yml) to work around a Mojo bug on GitHub's AMD EPYC
# 9V74 runners, where host-CPU codegen emits AVX-512 that Azure has masked,
# causing SIGILL. Unset locally → no override, so local builds stay native.
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
)

# Generic tests require expensive monomorphization. The lightest ones
# (scalars, tuples) can run on Linux CI with -j 2. The heavier ones
# (struct/list/dict/variant generics) need >21 GB peak memory during
# compilation and must be run locally.
GENERIC_TESTS=(
    test/test_typed_api_scalars.mojo
    test/test_typed_api_tuples.mojo
    test/test_prepared_statement.mojo
    test/test_result_fetch.mojo
)
# These tests need too much memory for CI runners (>21 GB peak).
# Run locally with: pixi run mojo run test/test_<name>.mojo
# HEAVY_TESTS=(
#     test/test_typed_api_mojo_type.mojo
#     test/test_typed_api_table_structs.mojo
#     test/test_typed_api_collections.mojo
#     test/test_appender_list.mojo
#     test/test_appender_map_variant.mojo
# )

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
