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

# ---- mojo_knn table function (item 4): recall@k + self-match vs exact stock ----
echo "==> mojo_knn batch kNN (recall vs exact stock)"
KDB="$TMP/knn.db"; K=10; KN=3000; KM=24; KD=128
"$DUCKDB" "$KDB" -c "SET lambda_syntax='ENABLE_SINGLE_ARROW'; SELECT setseed(0.33);
CREATE TABLE emb AS SELECT i AS id, apply(range(0,$KD), x->(random()-0.5)::FLOAT)::FLOAT[$KD] AS v FROM range($KN) t(i);
CREATE TABLE queries AS SELECT id AS qid, v AS qv FROM emb WHERE id<$KM;" >/dev/null 2>&1
for METRIC in cosine l2 ip; do
  case $METRIC in
    cosine) DF="array_cosine_distance(e.v,q.qv)";; l2) DF="array_distance(e.v,q.qv)";;
    ip) DF="array_negative_inner_product(e.v,q.qv)";;
  esac
  "$DUCKDB" "$KDB" -readonly -csv -noheader -c "SELECT q.qid, e.id FROM queries q, emb e
    QUALIFY row_number() OVER (PARTITION BY q.qid ORDER BY $DF, e.id) <= $K ORDER BY q.qid;" > "$TMP/kref_$METRIC.csv" 2>/dev/null
  "$DUCKDB" "$KDB" -unsigned -readonly -csv -noheader -c "LOAD '$EXT';
    SELECT query_rowid, rowid FROM mojo_knn('emb','v','queries','qv',$K, metric:='$METRIC');" 2>/dev/null \
    | grep -v installed > "$TMP/kmojo_$METRIC.csv"
done
python3 - "$TMP" "$KM" <<'PY'
import sys, csv
tmp, M = sys.argv[1], int(sys.argv[2]); ok = True
for metric in ("cosine","l2","ip"):
    ref={}; mojo={}
    for q,i in csv.reader(open(f"{tmp}/kref_{metric}.csv")): ref.setdefault(int(q),set()).add(int(i))
    for q,i in csv.reader(open(f"{tmp}/kmojo_{metric}.csv")): mojo.setdefault(int(q),set()).add(int(i))
    rec=sum(len(ref[q]&mojo.get(q,set()))/len(ref[q]) for q in ref)/len(ref)
    self_ok = 0 in mojo.get(0,set())
    print(f"   {metric:7s} recall@10={rec:.4f}  self-match={self_ok}")
    if rec < 0.99 or not self_ok: ok=False
print("   PASS" if ok else "   FAIL"); sys.exit(0 if ok else 1)
PY

# ---- fused sum/avg(transcendental) (item 2): optimizer rewrite vs stock ----
echo "==> fused sum/avg(transcendental) (stock vs override)"
read -r -d '' FBODY <<'SQL' || true
SELECT setseed(0.9);
CREATE TABLE ft AS SELECT (random()*100+0.01)::DOUBLE AS x,
  CASE WHEN random()<0.3 THEN NULL ELSE (random()*100+0.01) END::DOUBLE AS xn FROM range(500000) r(i);
.mode csv
.headers off
.output OUTFILE
SELECT sum(sqrt(x)),avg(sqrt(x)),sum(sin(x)),avg(cos(x)),sum(ln(x)),avg(exp(x*0.01)),sum(log10(x)) FROM ft;
SELECT sum(sqrt(xn)),avg(sqrt(xn)),sum(ln(xn)),avg(exp(xn*0.01)),sum(log10(xn)),sum(sin(xn)) FROM ft;
SELECT sum(sqrt(x)) FROM ft WHERE x < 0;  -- empty -> NULL
SQL
echo "${FBODY/OUTFILE/$TMP/f_stock.csv}" | "$DUCKDB" -unsigned >/dev/null
{ echo "LOAD '$EXT';"; echo "${FBODY/OUTFILE/$TMP/f_over.csv}"; } | "$DUCKDB" -unsigned >/dev/null
# confirm the rewrite actually fires
fired=$("$DUCKDB" -unsigned -noheader -list -cmd "LOAD '$EXT'" -c "EXPLAIN SELECT sum(sqrt(i::DOUBLE)) FROM range(10) t(i);" 2>/dev/null | grep -oE '__mojo_fsum_sqrt' | head -1)
[ "$fired" = "__mojo_fsum_sqrt" ] && echo "   ok: optimizer rewrite fires" || { echo "   FAIL: rewrite did not fire"; exit 1; }
python3 - "$TMP/f_stock.csv" "$TMP/f_over.csv" <<'PY'
import sys, csv
a=list(csv.reader(open(sys.argv[1]))); b=list(csv.reader(open(sys.argv[2])))
assert len(a)==len(b) and a; mx=0.0
for ra,rb in zip(a,b):
  for x,y in zip(ra,rb):
    if x==y: continue
    fx,fy=float(x),float(y); ad=abs(fx-fy); rd=ad/max(abs(fx),abs(fy),1e-30); mx=max(mx,rd)
    if ad>1e-2 and rd>1e-7: print("   FAIL",x,y,"rel",rd); sys.exit(1)
print(f"   fused max rel diff = {mx:.3e}"); print("   PASS")
PY
