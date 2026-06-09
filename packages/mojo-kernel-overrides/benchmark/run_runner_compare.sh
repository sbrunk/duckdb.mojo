#!/usr/bin/env bash
# Run DuckDB's own benchmark_runner twice — stock vs. with the mojo_overrides extension
# loaded — and tabulate the median timing + speedup per benchmark.
#
#   run_runner_compare.sh '<benchmark-regex>' [extra runner args...]
#   run_runner_compare.sh 'benchmark/micro/mojo_simd/.*'        # the mojo micro group (default)
#   run_runner_compare.sh 'benchmark/micro/mojo_simd/.*' --threads=1
#   run_runner_compare.sh 'benchmark/tpch/sf1/.*'              # official TPC-H Q1-Q22 (self-gen data)
#
# Pick a SPECIFIC group, not 'benchmark/tpch/.*': that broad regex sweeps in CSV/delete/ingestion
# benchmarks that abort the runner even on stock DuckDB (missing data / pre-existing bugs).
#
# The toggle is a single env var: DUCKDB_BENCH_EXTENSION set -> overrides on (the runner LOADs
# the extension at init via the runner_load_extension.patch hook); unset -> stock. Same binary,
# two runs, so the comparison is apples-to-apples against an otherwise stock DuckDB.
#
# NOTE: bandwidth-bound ops (plain +-*, parallel SUM/AVG, scans) only show headroom
# single-threaded — pass --threads=1 to see the kernel wins isolated from memory bandwidth.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)"
DUCKDB_SRC="${DUCKDB_SRC:-$ROOT/.duckdb-src}"
RUNNER="${RUNNER:-$DUCKDB_SRC/build/release/benchmark/benchmark_runner}"
EXT="${DUCKDB_BENCH_EXTENSION:-$ROOT/packages/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension}"

REGEX="${1:-benchmark/micro/mojo_simd/.*}"
shift || true   # remaining args ($@) are passed through to the runner (e.g. --threads=1)

[[ -x "$RUNNER" ]] || { echo "ERROR: benchmark_runner not built at $RUNNER — run 'pixi run overrides-bench-runner-build'" >&2; exit 1; }
[[ -f "$EXT" ]] || { echo "ERROR: extension missing at $EXT — run 'pixi run overrides-build'" >&2; exit 1; }
grep -q DUCKDB_BENCH_EXTENSION "$DUCKDB_SRC/benchmark/interpreted_benchmark.cpp" \
	|| { echo "ERROR: runner lacks the load-extension hook — run 'pixi run overrides-bench-runner-build'" >&2; exit 1; }

stock_out="$(mktemp)"; mojo_out="$(mktemp)"
trap 'rm -f "$stock_out" "$mojo_out"' EXIT

# median timing (seconds -> ms) per benchmark name, from the runner's CSV.
median_ms() {  # reads CSV on stdin, writes "name<TAB>median_ms"
  awk -F'\t' 'NR>1 && $3 ~ /^[0-9.]+$/ { v[$1]=v[$1] $3 " " }
    END { for (n in v) { c=split(v[n],a," "); for(i=1;i<=c;i++)for(j=i+1;j<=c;j++)if(a[j]<a[i]){t=a[i];a[i]=a[j];a[j]=t}
          m=(c%2)?a[int(c/2)+1]:(a[c/2]+a[c/2+1])/2; printf "%s\t%.4f\n", n, m*1000 } }'
}

# benchmark_runner emits the CSV timings on stderr, so merge it into stdout. Run from the source
# tree so the .benchmark files resolve by their relative paths.
echo ">>> STOCK ..."
( unset DUCKDB_BENCH_EXTENSION DUCKDB_MOJO_SIMD; cd "$DUCKDB_SRC" && "$RUNNER" "$REGEX" "$@" 2>&1 ) | median_ms | sort > "$stock_out"
echo ">>> MOJO-OVERRIDES ..."
( unset DUCKDB_MOJO_SIMD; export DUCKDB_BENCH_EXTENSION="$EXT"; cd "$DUCKDB_SRC" && "$RUNNER" "$REGEX" "$@" 2>&1 ) | median_ms | sort > "$mojo_out"

echo
printf "%-48s %10s %10s %9s\n" "benchmark" "stock(ms)" "mojo(ms)" "speedup"
printf '%s\n' "--------------------------------------------------------------------------------"
join -t$'\t' "$stock_out" "$mojo_out" |
while IFS=$'\t' read -r name sms mms; do
  sp=$(awk -v s="$sms" -v m="$mms" 'BEGIN{ printf (m>0)?"%.2fx":"n/a", s/m }')
  short="${name#benchmark/}"
  printf "%-48s %10s %10s %9s\n" "$short" "$sms" "$mms" "$sp"
done
