-- Bounded-LRU pin-cache eviction test for the mojo-gpu-operator kNN pin cache.
--
-- Invoked by `pixi run gpu-op-pin-evict-test`, which builds the extension, sets a
-- SMALL budget (GPU_OP_PIN_BUDGET_MB=256), substitutes __EXT__ (extension path)
-- and __QV__ (a 128-element FLOAT query-vector literal), then runs this script.
-- It greps for `FAIL` to determine pass/fail.
--
-- The test:
--   1. Builds 4 synthetic FLOAT[128] embedding tables, each ~100 MB resident.
--   2. Pins them in order emb0..emb3 (each cold-materializes + uploads). With a
--      256 MB budget only ~2 fit, so the LRU (emb0/emb1) must be evicted.
--   3. Asserts the resident set is BOUNDED (resident_mb <= budget_mb) and that
--      EVICTION HAPPENED (fewer than 4 resident; the LRU emb0 is gone).
--   4. Re-runs a kNN query against the EVICTED emb0 (forces a COLD rebuild) and
--      asserts every distance matches stock array_cosine_distance bit-closely
--      (the cold rebuild must never change the answer).
--   5. Confirms no leak: resident_mb stays <= budget_mb after the rebuild, and
--      gpu_unpin_all() drains the cache to empty.

LOAD '__EXT__';

-- 4 embedding tables, 200k rows x 128 dims fp32 ~= 102 MB each. Deterministic
-- values (no random()), distinct per table.
CREATE TABLE emb0 AS SELECT i AS id,
  (SELECT array_agg(CAST(((i*7+j*13+0) % 97) AS FLOAT) ORDER BY j) FROM range(128) g(j))::FLOAT[128] AS v
  FROM range(200000) t(i);
CREATE TABLE emb1 AS SELECT i AS id,
  (SELECT array_agg(CAST(((i*7+j*13+1) % 97) AS FLOAT) ORDER BY j) FROM range(128) g(j))::FLOAT[128] AS v
  FROM range(200000) t(i);
CREATE TABLE emb2 AS SELECT i AS id,
  (SELECT array_agg(CAST(((i*7+j*13+2) % 97) AS FLOAT) ORDER BY j) FROM range(128) g(j))::FLOAT[128] AS v
  FROM range(200000) t(i);
CREATE TABLE emb3 AS SELECT i AS id,
  (SELECT array_agg(CAST(((i*7+j*13+3) % 97) AS FLOAT) ORDER BY j) FROM range(128) g(j))::FLOAT[128] AS v
  FROM range(200000) t(i);

-- Reference: stock-DuckDB cosine distance for EVERY emb0 row vs the query vector,
-- computed with NO GPU pin. This is the ground truth the cold rebuild must match.
CREATE TABLE ref0 AS
  SELECT id AS rowid, array_cosine_distance(v, __QV__) AS dist FROM emb0;

-- 1-2. Pin all 4 (fp32) in order. emb0 first => first LRU victim.
SELECT 'pin emb0' AS step, key, rows, K, (bytes/1024/1024) AS mb FROM gpu_pin_table('emb0','v','fp32');
SELECT 'pin emb1' AS step, key FROM gpu_pin_table('emb1','v','fp32');
SELECT 'pin emb2' AS step, key FROM gpu_pin_table('emb2','v','fp32');
SELECT 'pin emb3' AS step, key FROM gpu_pin_table('emb3','v','fp32');

-- Observability snapshot.
SELECT 'status' AS step, key, kind, (bytes/1024/1024) AS mb, in_use, resident_mb, budget_mb
FROM gpu_pin_status() ORDER BY key;

-- 3a. ASSERT bounded: total resident_mb <= budget_mb.
SELECT 'ASSERT bounded' AS check,
       CASE WHEN COALESCE(MAX(resident_mb),0) <= COALESCE(MAX(budget_mb),256) THEN 'PASS' ELSE 'FAIL' END AS result,
       COALESCE(MAX(resident_mb),0) AS resident_mb, COALESCE(MAX(budget_mb),0) AS budget_mb
FROM gpu_pin_status();

-- 3b. ASSERT eviction happened: < 4 resident AND emb0 (LRU) evicted.
SELECT 'ASSERT eviction' AS check,
       CASE WHEN (SELECT count(*) FROM gpu_pin_status()) < 4
             AND (SELECT count(*) FROM gpu_pin_status() WHERE key = 'emb0.v') = 0
            THEN 'PASS' ELSE 'FAIL' END AS result,
       (SELECT count(*) FROM gpu_pin_status()) AS resident_entries;

-- 4. Re-run kNN on the EVICTED emb0 (COLD rebuild) via the fp32-exact gpu_cosine
-- (returns all rows). Compare to the stock reference by the SORTED distance
-- distribution -- order-independent, because gpu_cosine's rowid is the (possibly
-- parallel) scan position, which need not equal the table id. Both must produce
-- the same multiset of distances; count k-th-smallest pairs that differ by more
-- than 1e-5 -- must be 0 (the cold rebuild must not change any distance).
WITH g AS (SELECT row_number() OVER (ORDER BY dist) AS rn, dist FROM gpu_cosine('emb0','v',__QV__)),
     r AS (SELECT row_number() OVER (ORDER BY dist) AS rn, dist FROM ref0)
SELECT 'ASSERT cold-rebuild correct' AS check,
       CASE WHEN (SELECT count(*) FROM g JOIN r USING (rn) WHERE abs(g.dist - r.dist) > 1e-5) = 0
            THEN 'PASS' ELSE 'FAIL' END AS result,
       (SELECT count(*) FROM g JOIN r USING (rn) WHERE abs(g.dist - r.dist) > 1e-5) AS mismatched_dists;

-- 5. Still bounded after rebuild (rebuild re-pins emb0, may evict another LRU).
SELECT 'ASSERT still bounded' AS check,
       CASE WHEN COALESCE(MAX(resident_mb),0) <= COALESCE(MAX(budget_mb),256) THEN 'PASS' ELSE 'FAIL' END AS result,
       COALESCE(MAX(resident_mb),0) AS resident_mb
FROM gpu_pin_status();

-- Drain + assert empty.
SELECT 'unpin_all' AS step, freed, skipped_in_use FROM gpu_unpin_all();
SELECT 'ASSERT empty after unpin_all' AS check,
       CASE WHEN count(*) = 0 THEN 'PASS' ELSE 'FAIL' END AS result, count(*) AS resident_entries
FROM gpu_pin_status();
