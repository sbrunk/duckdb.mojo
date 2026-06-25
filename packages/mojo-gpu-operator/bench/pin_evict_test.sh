#!/usr/bin/env bash
# Driver for the bounded-LRU pin-cache eviction test (feature #4).
#
# Substitutes the extension path + a deterministic 128-element FLOAT query-vector
# literal into pin_evict_test.sql, runs it with a SMALL pin budget against the
# unsigned-extension DuckDB CLI, and FAILs (nonzero exit) if any assertion prints
# FAIL or the run errors. Run directly: `bash packages/mojo-gpu-operator/bench/pin_evict_test.sh`.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PKG="$(cd "$HERE/.." && pwd)"
EXT="$PKG/build/mojo_gpu_operator.duckdb_extension"

if [[ ! -f "$EXT" ]]; then
  echo "extension not built: $EXT (run: pixi run gpu-op-build)" >&2
  exit 1
fi

# Deterministic query vector: q[j] = (j*3+5) % 11, length 128 (matches ref0).
QV="$(python3 -c "print('['+','.join(f'{float((j*3+5)%11)}' for j in range(128))+']::FLOAT[128]')")"

# Build the runnable SQL with substitutions (use a temp file to avoid -c quoting).
TMPSQL="$(mktemp -t pin_evict.XXXXXX.sql)"
trap 'rm -f "$TMPSQL"' EXIT
sed -e "s#__EXT__#$EXT#g" -e "s#__QV__#$QV#g" "$HERE/pin_evict_test.sql" > "$TMPSQL"

echo "==> pin-evict test (budget=${GPU_OP_PIN_BUDGET_MB:-256} MB)"
OUT="$(GPU_OP_PIN_BUDGET_MB="${GPU_OP_PIN_BUDGET_MB:-256}" duckdb -unsigned < "$TMPSQL" 2>&1)"
echo "$OUT"

if echo "$OUT" | grep -qiE "FAIL|Error"; then
  echo "RESULT: FAIL" >&2
  exit 1
fi
echo "RESULT: ALL PASS"
