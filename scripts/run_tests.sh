#!/bin/bash
# Run tests, compiling the duckdb package from source for each test file.
#
set -e

# Optional extra flags for every mojo build/run, for example
# MOJO_TARGET_FLAGS=--target-cpu=x86-64-v3 to pin the codegen target. Unset by
# default, so builds target the native CPU.
read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"

TESTS=(
    test/test_scalar_types.mojo
    test/test_libduckdb_v2.mojo
    test/test_foundations.mojo
    test/test_results.mojo
    test/test_relation.mojo
    test/test_nested.mojo
    test/test_functions.mojo
    test/test_aggregates.mojo
    test/test_tables.mojo
    test/test_module_api.mojo
    test/test_signatures.mojo
    test/test_extend.mojo
    test/test_observe.mojo
    test/test_copy.mojo
)

for f in "${TESTS[@]}"; do
    echo "--- Running: $f ---"
    mojo run "${MOJO_TARGET[@]}" "$f"
done
