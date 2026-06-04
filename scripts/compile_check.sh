#!/bin/bash
# Compile-check every Mojo file in a directory without running it.
#
# Benchmarks and examples aren't part of the test suite, so they silently rot
# when the Mojo stdlib or the duckdb API changes (renamed methods, new ABI
# requirements on callbacks, stdlib import-path churn). A plain `mojo build`
# catches all of those in seconds — no data generation, no runtime variance —
# which is all we want from a guard against API/ABI drift.
#
# Usage: scripts/compile_check.sh <dir> [<dir> ...]
# Runs via `pixi run compile-benchmarks` (see pixi.toml). Compiles the duckdb
# package from source per file (see the EXPERIMENT note in run_tests.sh).
set -e

# Optional Mojo codegen target override, set ONLY in CI — see run_tests.sh and
# .github/workflows/test.yml for the rationale. Unset locally → native build.
read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"

# Files that can't be compiled in the default environment. Keep this list short
# and explain every entry — anything excluded here is NOT guarded against drift.
EXCLUDE=(
    # Requires the `full` environment (duckdb-from-source + the
    # operator_replacement package). Covered separately; see the full-env CI
    # job rather than this default-env check.
    "benchmark/tpch_benchmark_op_replacement.mojo"
)

is_excluded() {
    local f="$1"
    for e in "${EXCLUDE[@]}"; do
        [[ "$f" == "$e" ]] && return 0
    done
    return 1
}

OUT_DIR="$(mktemp -d)"
trap 'rm -rf "$OUT_DIR"' EXIT

shopt -s nullglob
failed=0
checked=0
for dir in "$@"; do
    for f in "$dir"/*.mojo; do
        if is_excluded "$f"; then
            echo "--- Skipping (needs full env): $f ---"
            continue
        fi
        echo "--- Compiling: $f ---"
        if mojo build "${MOJO_TARGET[@]}" "$f" -o "$OUT_DIR/$(basename "$f").bin"; then
            checked=$((checked + 1))
        else
            echo "ERROR: failed to compile $f"
            failed=$((failed + 1))
        fi
    done
done

echo
if [[ "$failed" -gt 0 ]]; then
    echo "Compile-check FAILED: $failed file(s) did not compile ($checked passed)."
    exit 1
fi
echo "Compile-check passed: $checked file(s) compiled."
