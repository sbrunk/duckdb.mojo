#!/usr/bin/env bash
# Unified DuckDB benchmark_runner compare driver. Replaces the old
# run_runner_compare / gpu_warm_sweep / knn_warm_sweep scripts.
#
#   bench_runner.sh <group> [--engines=stock,cpu,gpu] [--by-suffix] [--threads=N] [runner-args...]
#
#   <group>      a dir under benchmark/sql/ (mojo_simd | gpu_xover | gpu_knn), or a
#                path under the runner tree for built-ins (e.g. tpch/sf1/q0[16]).
#   --engines    toggle mode (default): run the SAME benchmarks under each engine
#                (env toggles which extension loads), one column per engine.
#   --by-suffix  per-file mode: each <regime>_<engine>.benchmark runs under the
#                extension named by its suffix (stock|cpu|gpu); rows = regimes.
#   --threads=N  pass through to the runner (single-thread isolates kernel wins).
#
# Engines: stock = no extension; cpu = mojo-kernel-overrides; gpu = mojo-gpu-operator
# (+GPU_OP_MIN_ROWS=0). Warm: benchmark_runner reports the median of its iterations.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
SRC="${DUCKDB_SRC:-$ROOT/third_party/duckdb}"
RUNNER="${RUNNER:-$SRC/build/release/benchmark/benchmark_runner}"
OVR="${OVR_EXT:-$ROOT/packages/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension}"
GPU="${GPU_EXT:-$ROOT/packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension}"

GROUP="${1:?usage: bench_runner.sh <group> [--engines=..|--by-suffix] [--threads=N]}"; shift || true
ENGINES="stock,cpu,gpu"; BYSUFFIX=0; PASS=()
for a in "$@"; do
  case "$a" in
    --engines=*) ENGINES="${a#--engines=}" ;;
    --by-suffix) BYSUFFIX=1 ;;
    --) ;;
    *) PASS+=("$a") ;;
  esac
done
[[ -x "$RUNNER" ]] || { echo "no benchmark_runner at $RUNNER — run 'pixi run bench-build'"; exit 1; }

# Stage committed benchmark/sql/* into the runner tree (idempotent).
for g in "$ROOT"/benchmark/sql/*/; do
  n="$(basename "$g")"; mkdir -p "$SRC/benchmark/micro/$n"; cp "$g"*.benchmark "$SRC/benchmark/micro/$n/" 2>/dev/null || true
done

# regex: a path with '/' is a runner-tree path (built-ins like tpch/sf1); else a micro group.
case "$GROUP" in */*) REGEX="benchmark/$GROUP.*" ;; *) REGEX="benchmark/micro/$GROUP/.*" ;; esac

env_for() { case "$1" in
  stock) echo "" ;; cpu) echo "DUCKDB_BENCH_EXTENSION=$OVR" ;;
  gpu) echo "DUCKDB_BENCH_EXTENSION=$GPU GPU_OP_MIN_ROWS=0" ;; *) echo "" ;; esac; }
med() { awk -F'\t' 'NR>1 && $3 ~ /^[0-9.]+$/ {a[$1]=a[$1] $3 " "}
  END{for(n in a){c=split(a[n],x," ");for(i=1;i<=c;i++)for(j=i+1;j<=c;j++)if(x[j]<x[i]){t=x[i];x[i]=x[j];x[j]=t}
      m=(c%2)?x[int(c/2)+1]:(x[c/2]+x[c/2+1])/2; printf "%s\t%.2f\n",n,m*1000}}'; }
runner() { ( cd "$SRC" && env $1 "$RUNNER" "$2" "${PASS[@]}" 2>&1 ) | med; }

TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
if [[ "$BYSUFFIX" == 1 ]]; then
  # per-file: ext from filename suffix; rows = regime (prefix), cols = engine.
  for f in "$SRC/benchmark/micro/$GROUP/"*.benchmark; do
    b="$(basename "$f" .benchmark)"; eng="${b##*_}"
    runner "$(env_for "$eng")" "benchmark/micro/$GROUP/$b.benchmark" >> "$TMP/all"
  done
  python3 - "$TMP/all" <<'PY'
import sys,collections
rows=collections.defaultdict(dict); engs=[]
for line in open(sys.argv[1]):
    name,ms=line.rstrip("\n").split("\t"); b=name.split("/")[-1].replace(".benchmark","")
    regime,_,eng=b.rpartition("_"); rows[regime][eng]=ms
    if eng not in engs: engs.append(eng)
engs=[e for e in ("stock","cpu","gpu","vss") if e in engs]
print("%-12s"%"regime"+"".join("%12s"%e for e in engs))
for r in rows: print("%-12s"%r+"".join("%12s"%rows[r].get(e,"-") for e in engs))
PY
else
  IFS=',' read -ra ENG <<< "$ENGINES"
  for e in "${ENG[@]}"; do runner "$(env_for "$e")" "$REGEX" | sed "s|^|$e\t|" >> "$TMP/all"; done
  python3 - "$TMP/all" "$ENGINES" <<'PY'
import sys,collections
order=sys.argv[2].split(","); rows=collections.defaultdict(dict)
for line in open(sys.argv[1]):
    eng,name,ms=line.rstrip("\n").split("\t"); rows[name.replace("benchmark/micro/","")][eng]=float(ms)
print("%-40s"%"benchmark"+"".join("%10s"%e for e in order)+"%9s"%"winner")
for n in sorted(rows):
    r=rows[n]; w=min((e for e in r),key=lambda e:r[e]) if r else "-"
    print("%-40s"%n+"".join(("%10.2f"%r[e] if e in r else "%10s"%"-") for e in order)+"%9s"%w)
PY
fi
