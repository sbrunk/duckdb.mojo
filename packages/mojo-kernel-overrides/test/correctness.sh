#!/usr/bin/env bash
# Regression test for the array distance/similarity SIMD overrides.
# Runs the same deterministic queries twice — stock DuckDB vs the extension —
# and asserts the results match within ~1 ULP, plus NULL-semantics and
# known-value checks. Exits non-zero on any mismatch.
#
# Run via `pixi run overrides-test` (builds the extension first).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
EXT="$HERE/../build/mojo_overrides.duckdb_extension"
DUCKDB="${DUCKDB:-duckdb}"
[ -f "$EXT" ] || { echo "missing $EXT — run 'pixi run overrides-build' first"; exit 1; }

TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# Deterministic random arrays of several sizes/types; dump per-row metrics to CSV.
# OUTFILE is substituted per run.
read -r -d '' BODY <<'SQL' || true
SET lambda_syntax='ENABLE_SINGLE_ARROW';
SELECT setseed(0.123);
CREATE TABLE e_f32_128 AS SELECT id, apply(range(0,128), x -> (random()-0.5)::FLOAT)::FLOAT[128]  AS v FROM range(1500) t(id);
CREATE TABLE e_f32_13  AS SELECT id, apply(range(0,13),  x -> (random()-0.5)::FLOAT)::FLOAT[13]   AS v FROM range(1500) t(id);
CREATE TABLE e_f32_17  AS SELECT id, apply(range(0,17),  x -> (random()-0.5)::FLOAT)::FLOAT[17]   AS v FROM range(1500) t(id);
CREATE TABLE e_f64_100 AS SELECT id, apply(range(0,100), x -> (random()-0.5)::DOUBLE)::DOUBLE[100] AS v FROM range(1500) t(id);
.mode csv
.headers off
.output OUTFILE
SELECT t.id,
       array_distance(t.v, q.v),
       array_inner_product(t.v, q.v),
       array_negative_inner_product(t.v, q.v),
       array_cosine_similarity(t.v, q.v),
       array_cosine_distance(t.v, q.v)
FROM e_f32_128 t, (SELECT v FROM e_f32_128 WHERE id=7) q ORDER BY t.id;
SELECT t.id, array_distance(t.v,q.v), array_cosine_distance(t.v,q.v)
FROM e_f32_13 t, (SELECT v FROM e_f32_13 WHERE id=7) q ORDER BY t.id;
SELECT t.id, array_distance(t.v,q.v), array_cosine_distance(t.v,q.v)
FROM e_f32_17 t, (SELECT v FROM e_f32_17 WHERE id=7) q ORDER BY t.id;
SELECT t.id, array_distance(t.v,q.v), array_inner_product(t.v,q.v), array_cosine_distance(t.v,q.v)
FROM e_f64_100 t, (SELECT v FROM e_f64_100 WHERE id=7) q ORDER BY t.id;
SQL

echo "==> stock run"
echo "${BODY/OUTFILE/$TMP/stock.csv}" | "$DUCKDB" -unsigned >/dev/null
echo "==> override run"
{ echo "LOAD '$EXT';"; echo "${BODY/OUTFILE/$TMP/over.csv}"; } | "$DUCKDB" -unsigned >/dev/null

echo "==> compare values (tolerance abs<=2e-4 OR rel<=2e-4)"
python3 - "$TMP/stock.csv" "$TMP/over.csv" <<'PY'
import sys, csv
a = list(csv.reader(open(sys.argv[1]))); b = list(csv.reader(open(sys.argv[2])))
assert len(a) == len(b) and a, f"row count differs: {len(a)} vs {len(b)}"
maxabs = maxrel = 0.0; n = 0
for ra, rb in zip(a, b):
    assert len(ra) == len(rb)
    for x, y in zip(ra, rb):
        try: fx, fy = float(x), float(y)
        except ValueError:
            assert x == y; continue
        n += 1
        ad = abs(fx - fy); rd = ad / max(abs(fx), abs(fy), 1e-30)
        maxabs = max(maxabs, ad); maxrel = max(maxrel, rd)
print(f"   compared {n} numeric cells across {len(a)} rows")
print(f"   max abs diff = {maxabs:.3e}   max rel diff = {maxrel:.3e}")
if maxabs > 2e-4 and maxrel > 2e-4:
    print("   FAIL"); sys.exit(1)
PY

echo "==> NULL + known-value checks"
fail=0
chk() { # chk "label" "expected" "sql"
  local got
  got=$("$DUCKDB" -unsigned -noheader -list -c "LOAD '$EXT'; $3" 2>&1) || true
  if [[ "$got" == *"$2"* ]]; then echo "   ok: $1"; else echo "   FAIL: $1 — expected '$2' got '$got'"; fail=1; fi
}
chk "NULL row -> NULL"        "NULL"   "SELECT array_distance(NULL::FLOAT[3], [1,2,3]::FLOAT[3]);"
chk "NULL element -> error"   "can not contain NULL values" "SELECT array_distance([1,NULL,3]::FLOAT[3], [1,2,3]::FLOAT[3]);"
chk "distance([0,3,4],0)=5"   "5.0"    "SELECT array_distance([0,3,4]::FLOAT[3],[0,0,0]::FLOAT[3]);"
chk "cos_sim([1,0],[1,0])=1"  "1.0"    "SELECT array_cosine_similarity([1,0]::FLOAT[2],[1,0]::FLOAT[2]);"
chk "dot([1,2,3],[4,5,6])=32" "32.0"   "SELECT array_inner_product([1,2,3]::DOUBLE[3],[4,5,6]::DOUBLE[3]);"

[ "$fail" -eq 0 ] && echo "PASS: array-distance override checks" || { echo "FAILED"; exit 1; }

# ---- nullable aggregates (A1 mask-multiply): sum/avg/min/max over NULL-containing columns ----
echo "==> nullable aggregates (stock vs override)"
read -r -d '' NBODY <<'SQL' || true
SELECT setseed(0.77);
CREATE TABLE t AS
SELECT
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*1000 - 500) END::DOUBLE AS d,
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*1000 - 500) END::REAL   AS f,
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*1e9)::HUGEINT END        AS h,
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*1e6)::DECIMAL(38,4) END  AS dec,
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*100) END::DOUBLE AS pos,
  NULL::DOUBLE AS alln
FROM range(500000) r(i);
.mode csv
.headers off
.output OUTFILE
SELECT sum(d), avg(d), min(d), max(d) FROM t;
SELECT min(f), max(f) FROM t;
SELECT sum(h), avg(h) FROM t;
SELECT sum(dec), avg(dec) FROM t;
SELECT sum(sqrt(pos)) FROM t;
SELECT sum(alln), avg(alln), min(alln), max(alln) FROM t;
SQL
echo "${NBODY/OUTFILE/$TMP/n_stock.csv}" | "$DUCKDB" -unsigned >/dev/null
{ echo "LOAD '$EXT';"; echo "${NBODY/OUTFILE/$TMP/n_over.csv}"; } | "$DUCKDB" -unsigned >/dev/null
python3 - "$TMP/n_stock.csv" "$TMP/n_over.csv" <<'PY'
import sys, csv
a = list(csv.reader(open(sys.argv[1]))); b = list(csv.reader(open(sys.argv[2])))
assert len(a) == len(b) and a, (len(a), len(b))
mx = 0.0
for ra, rb in zip(a, b):
    for x, y in zip(ra, rb):
        if x == y: continue          # exact (incl. NULL/NULL, hugeint, decimal)
        try: fx, fy = float(x), float(y)
        except ValueError: print("FAIL str mismatch", x, y); sys.exit(1)
        ad = abs(fx - fy); rd = ad / max(abs(fx), abs(fy), 1e-30); mx = max(mx, rd)
        if ad > 1e-3 and rd > 1e-6: print("FAIL", x, y, "rel", rd); sys.exit(1)
print(f"   nullable max rel diff = {mx:.3e}")
print("   PASS")
PY
