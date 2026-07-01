"""Vector-search (cosine top-k) benchmark harness — Mojo, via the duckdb.mojo client.

Compares, warm, on one dataset:
  single-query top-k : stock-brute | cpu-simd (array override) | vss-HNSW | gpu
  batch top-k (M qs) : cpu (mojo_knn) | gpu (gpu_cosine_topk_batch)
reporting median latency (ms) and recall@k vs the exact brute-force result.

Loads the CPP-ABI extensions (version-locked, unsigned) per engine in its own
connection. Skips an engine cleanly if its extension/feature is unavailable.

  pixi run bench-knn      # = mojo run -I . -I benchmark/mojo benchmark/mojo/knn_compare.mojo
Tune N/M/K/k via the comptime constants. Extensions must be built first:
  pixi run overrides-build && pixi run gpu-op-build
"""
from duckdb import DuckDB, Config, Connection, ApiLevel
from std.time import perf_counter_ns
from std.sys import has_accelerator
from std.collections import List
from bench_util import warm_median, recall

comptime N = 200_000      # embeddings
comptime M = 500          # batch queries
comptime K = 128          # dim
comptime KK = 10          # top-k
comptime WARMUP = 3
comptime ITERS = 7
comptime DB_PATH = "/tmp/knn_compare.db"
comptime OVR_EXT = "extensions/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension"
comptime GPU_EXT = "extensions/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension"


def vec_literal() raises -> String:
    var s = String("[")
    for i in range(K):
        if i > 0:
            s += ","
        s += "0.1"
    s += "]::FLOAT[" + String(K) + "]"
    return s




def new_con(unsigned: Bool) raises -> Connection[ApiLevel.CLIENT]:
    var cfg = Config()
    if unsigned:
        cfg.set("allow_unsigned_extensions", "true")
    return DuckDB.connect(DB_PATH, cfg)


def consume(mut con: Connection[ApiLevel.CLIENT], sql: String) raises:
    for _ in con.execute(sql):
        pass


def time_query(mut con: Connection[ApiLevel.CLIENT], sql: String) raises -> Float64:
    for _ in range(WARMUP):
        consume(con, sql)
    var times = List[Int]()
    for _ in range(ITERS):
        var t0 = perf_counter_ns()
        consume(con, sql)
        times.append(Int(perf_counter_ns() - t0))
    return warm_median(times)


def topk_ids(mut con: Connection[ApiLevel.CLIENT], sql: String) raises -> List[Int64]:
    var ids = List[Int64]()
    for row in con.execute(sql):
        ids.append(row.get[Int64](col=0))
    return ids^




def report(label: String, ms: Float64, rec: Float64):
    print("  ", label, "  ", ms, "ms   recall@", KK, "=", rec)


def main() raises:
    var lit = vec_literal()

    # ---- build dataset once ----
    print("building dataset N=", N, " M=", M, " K=", K, " ...")
    var b = new_con(False)
    _ = b.execute("SET lambda_syntax='ENABLE_SINGLE_ARROW';")
    _ = b.execute("DROP TABLE IF EXISTS emb; DROP TABLE IF EXISTS queries;")
    _ = b.execute(
        "CREATE TABLE emb AS SELECT i id, apply(range(0,"
        + String(K)
        + "),x->random()::FLOAT)::FLOAT["
        + String(K)
        + "] v FROM range("
        + String(N)
        + ") u(i);"
    )
    _ = b.execute(
        "CREATE TABLE queries AS SELECT i qid, apply(range(0,"
        + String(K)
        + "),x->random()::FLOAT)::FLOAT["
        + String(K)
        + "] qv FROM range("
        + String(M)
        + ") u(i);"
    )

    # exact reference (stock brute), for recall
    var inner = "SELECT id FROM emb ORDER BY array_cosine_distance(v," + lit + ") LIMIT " + String(KK)
    var topk_sql = inner + ";"
    var cnt_sql = "SELECT count(*) FROM (" + inner + ");"
    var exact = topk_ids(b, topk_sql)

    print("\n== single-query top-k ==")
    # stock
    var cs = new_con(False)
    report("stock-brute", time_query(cs, cnt_sql), recall(topk_ids(cs, topk_sql), exact))

    # cpu-simd (array_cosine_distance override)
    try:
        var cc = new_con(True)
        _ = cc.execute("LOAD '" + OVR_EXT + "';")
        report("cpu-simd", time_query(cc, cnt_sql), recall(topk_ids(cc, topk_sql), exact))
    except e:
        print("   cpu-simd: skipped (", e, ")")

    # vss HNSW
    try:
        var cv = new_con(False)
        _ = cv.execute("INSTALL vss; LOAD vss;")
        _ = cv.execute("SET hnsw_enable_experimental_persistence=true;")
        _ = cv.execute("DROP INDEX IF EXISTS h;")
        var t0 = perf_counter_ns()
        _ = cv.execute("CREATE INDEX h ON emb USING HNSW(v) WITH (metric='cosine');")
        var build_ms = Float64(Int(perf_counter_ns() - t0)) / 1e6
        report("vss-hnsw", time_query(cv, cnt_sql), recall(topk_ids(cv, topk_sql), exact))
        print("     (hnsw build:", build_ms, "ms, one-time)")
        _ = cv.execute("DROP INDEX IF EXISTS h;")
    except e:
        print("   vss-hnsw: skipped (", e, ")")

    # gpu
    if has_accelerator():
        try:
            var cg = new_con(True)
            _ = cg.execute("LOAD '" + GPU_EXT + "';")
            var gsql = "SELECT count(*) FROM gpu_cosine_topk('emb','v'," + lit + "," + String(KK) + ");"
            var gids = "SELECT rowid FROM gpu_cosine_topk('emb','v'," + lit + "," + String(KK) + ");"
            report("gpu", time_query(cg, gsql), recall(topk_ids(cg, gids), exact))
        except e:
            print("   gpu: skipped (", e, ")")
    else:
        print("   gpu: skipped (no accelerator)")

    print("\n== batch top-k (M=", M, ") ==")
    var knn_sql = "SELECT count(*) FROM mojo_knn('emb','v','queries','qv'," + String(KK) + ",metric:='cosine');"
    try:
        var cc2 = new_con(True)
        _ = cc2.execute("LOAD '" + OVR_EXT + "';")
        report("cpu mojo_knn", time_query(cc2, knn_sql), 1.0)
    except e:
        print("   cpu mojo_knn: skipped (", e, ")")
    if has_accelerator():
        try:
            var cg2 = new_con(True)
            _ = cg2.execute("LOAD '" + GPU_EXT + "';")
            var gb = "SELECT count(*) FROM gpu_cosine_topk_batch('emb','v','queries','qv'," + String(KK) + ");"
            report("gpu batch", time_query(cg2, gb), 1.0)
        except e:
            print("   gpu batch: skipped (", e, ")")
