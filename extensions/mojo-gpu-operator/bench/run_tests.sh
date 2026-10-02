#!/usr/bin/env bash
# Run the mojo-gpu-operator GPU tests. Needs a GPU and the `gpu` environment:
#
#   pixi run -e gpu gpu-op-test            # all tests
#   pixi run -e gpu gpu-op-test q6_shuttle # only tests whose name contains "q6_shuttle"
#
# The SQL tests load the built extension, so gpu-op-test builds it first.
# A test fails if it exits nonzero or prints FAIL or MISMATCH (a few tests
# report failures only by printing).
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
SRC="$ROOT/extensions/mojo-gpu-operator/src"
FILTER="${1:-}"
cd "$ROOT"

read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"
LOGS="$(mktemp -d)"
trap 'rm -rf "$LOGS"' EXIT

# seg_f64_kernel_test reads scaled l_extendedprice values from TPC-H sf1.
export LEXT_BIN="${LEXT_BIN:-/tmp/lext_scaled.bin}"
make_lext_bin() {
    [[ -s "$LEXT_BIN" ]] && return 0
    echo "    (writing $LEXT_BIN from TPC-H sf1)"
    duckdb -c "INSTALL tpch; LOAD tpch; CALL dbgen(sf=1);
        COPY (SELECT (l_extendedprice * 100)::BIGINT FROM lineitem)
        TO '$LEXT_BIN' (FORMAT csv, HEADER false);" > /dev/null
}

# name|environment variables to set (space separated, may be empty)
TESTS=(
    # Descriptor and expression VM unit tests
    "raw_plan_roundtrip_test|"
    "expr_vm_f64_test|"
    "generic_kernel_test|"
    "native_decode_test|"
    "colpool_costaware_test|"
    # Full C-ABI execution path for each query class, without DuckDB
    "q1_shuttle_test|"
    "q3_shuttle_test|"
    "q5_shuttle_test|"
    "q6_shuttle_test|"
    "q14_shuttle_test|"
    "or_filter_shuttle_test|"
    "skipmat_allresident_test|GPU_OP_COLPOOL=2 GPU_OP_NATIVE_DECODE=1"
    # SQL through the loaded extension, compared with stock DuckDB
    "nullable_sql_test|"
    "nullable_f64_sql_test|"
    "nullable_grouped_sql_test|"
    "nullable_multiagg_sql_test|"
    "nullable_adversarial_sweep|"
    "q1_dense_filter_phantom_probe|"
    # Vector search
    "cosine_topk_test|"
    "cosine_batch_test|"
    "cosine_f16_test|"
    "tc_knn_operator_recall|"
    "apple_tc_knn_test|"
)
# NVIDIA-only paths: the float64 kernels (Metal has no kernel float64 and no
# 64-bit atomics) and the non-cosine metrics of the tensor-core kNN.
if [[ "$(uname)" != "Darwin" ]]; then
    TESTS+=("seg_f64_kernel_test|" "tc_knn_metric_test|GPU_OP_TENSORCORE=1")
fi

passed=0
failed=()
for entry in "${TESTS[@]}"; do
    name="${entry%%|*}"
    vars="${entry#*|}"
    [[ -n "$FILTER" && "$name" != *"$FILTER"* ]] && continue
    echo "--- $name ${vars:+($vars)}"
    [[ "$name" == seg_f64_kernel_test ]] && make_lext_bin
    # Only stdout is checked for FAIL: compiler diagnostics go to stderr and
    # quote source lines, which can contain the same words.
    # shellcheck disable=SC2086
    out="$(env $vars mojo run "${MOJO_TARGET[@]}" -I "$SRC" "$HERE/$name.mojo" 2> "$LOGS/$name.err")"
    rc=$?
    if [[ $rc -ne 0 ]] || grep -qE '\bFAIL\b|MISMATCH' <<< "$out"; then
        echo "$out" | tail -15
        grep -E 'error|Error' "$LOGS/$name.err" | tail -5
        echo "FAILED: $name (exit $rc)"
        failed+=("$name")
    else
        echo "$out" | grep -v '^\s*$' | tail -1
        passed=$((passed + 1))
    fi
done

if [[ -z "$FILTER" || "pin_evict_test" == *"$FILTER"* ]]; then
    echo "--- pin_evict_test"
    if bash "$HERE/pin_evict_test.sh" > /tmp/pin_evict_test.log 2>&1; then
        tail -1 /tmp/pin_evict_test.log
        passed=$((passed + 1))
    else
        tail -20 /tmp/pin_evict_test.log
        echo "FAILED: pin_evict_test"
        failed+=("pin_evict_test")
    fi
fi

echo
echo "$passed passed, ${#failed[@]} failed${failed:+: ${failed[*]}}"
[[ ${#failed[@]} -eq 0 ]]
