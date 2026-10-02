#!/bin/bash
# Compile-check every Mojo file in a directory without running it.
#
# Benchmarks and examples aren't part of the test suite, so they break without
# anyone noticing when the Mojo stdlib or the duckdb API changes (renamed
# methods, new ABI requirements on callbacks, moved stdlib imports). A plain
# `mojo build` catches all of those in seconds, without generating data or
# depending on runtime behavior, which is all we need to catch API/ABI drift.
#
# Usage: scripts/compile_check.sh <dir> [<dir> ...]
# Runs via `pixi run compile-check` (see pixi.toml). Like run_tests.sh, it
# compiles the duckdb package from source for each file.
set -e

# Optional Mojo codegen target override, set only in CI. See run_tests.sh and
# .github/workflows/test.yml for the reason. When unset (locally), the build is
# native.
read -ra MOJO_TARGET <<< "${MOJO_TARGET_FLAGS:-}"

# Files that can't be compiled in the default environment. Keep this list short
# and explain every entry. Anything excluded here is not checked for drift.
EXCLUDE=(
    # Requires the `full` environment (duckdb-from-source + the
    # operator_replacement package). Covered separately; see the full-env CI
    # job rather than this default-env check.
    "benchmark/tpch_benchmark_op_replacement.mojo"
    # Requires the `gpu` environment (max.gpu).
    "examples/gpu_knn.mojo"
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
            echo "--- Skipping (needs another environment): $f ---"
            continue
        fi
        echo "--- Compiling: $f ---"
        # --emit object: compile through codegen but skip the executable link.
        # All the drift we guard against (imports, renamed APIs, ABI signatures)
        # is caught at compile time, so linking adds nothing and a full link
        # spuriously fails on linux for math-using files (mojo doesn't put -lm on
        # the link line: "libm.so.6: DSO missing from command line"). Skipping
        # the link sidesteps that and is faster.
        if mojo build --emit object "${MOJO_TARGET[@]}" "$f" -o "$OUT_DIR/$(basename "$f").o"; then
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
