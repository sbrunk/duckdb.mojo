"""Exact vector search (k nearest neighbors) on the GPU.

The embeddings are stored in a DuckDB table. They are uploaded to the GPU
before the timed section and reused by every query. `cosine_kernel` scores each
row against all queries while the row sits in registers, so all queries share
the same scan of the table. The distances are copied back to the host, where
`top_k` keeps the nearest `TOP_K`.

Needs the `gpu` environment (it provides `max.gpu`):

    pixi run -e gpu mojo run examples/gpu_knn.mojo
"""

import duckdb
from duckdb import Connection
from max.gpu import block_idx, thread_idx, WARP_SIZE
from max.gpu.primitives import warp
from max.gpu.host import DeviceContext
from std.math import sqrt
from std.memory import unsafe_memcpy
from std.sys import has_accelerator
from std.time import perf_counter_ns

comptime N = 200_000  # rows in the table
comptime K = 256  # embedding dimension
comptime QUERIES = 20
comptime TOP_K = 10
comptime ROWS_PER_BLOCK = 8  # one warp per row
comptime ROW_F32 = K // 32  # values per lane, the 32 lanes of a warp cover one row
comptime SCAN_BLOCK = 16  # host top-k: distances per step


def cosine_kernel(
    emb: Pointer[Float32, MutUntrackedOrigin],
    qs: Pointer[Float32, MutUntrackedOrigin],
    qnorms: Pointer[Float32, MutUntrackedOrigin],
    dist: Pointer[Float32, MutUntrackedOrigin],
    M: Int64,  # query count, Int64 because kernel arguments need a fixed width
):
    """One warp per row. The warp loads its row into registers once and scores
    it against all M query vectors, so the table is read once for the whole
    batch. The row norm comes from those registers, the query norms from
    `qnorms`, computed on the host. Lane 0 writes each distance into the
    QUERIES x N matrix `dist`."""
    var row = Int(block_idx.x) * ROWS_PER_BLOCK + Int(thread_idx.x) // WARP_SIZE
    var lane = Int(thread_idx.x) % WARP_SIZE
    var slice = SIMD[DType.float32, ROW_F32]()
    for i in range(ROW_F32):
        slice[i] = emb[unsafe_offset=row * K + lane * ROW_F32 + i]
    var rnorm = sqrt(warp.sum(slice * slice).reduce_add())
    for qid in range(Int(M)):
        var q = SIMD[DType.float32, ROW_F32]()
        for i in range(ROW_F32):
            q[i] = qs[unsafe_offset=qid * K + lane * ROW_F32 + i]
        var dot = warp.sum(slice * q).reduce_add()
        if lane == 0:
            var denom = rnorm * qnorms[unsafe_offset=qid]
            dist[unsafe_offset=qid * N + row] = Float32(1) - dot / denom if denom != 0 else Float32(1)


def top_k(dist: Pointer[Float32, _], qid: Int) -> List[Int64]:
    """Indices of the TOP_K smallest distances of query `qid`, nearest first."""
    var best = List[Int64]()
    var best_d = List[Float32]()
    var worst = Float32(3.4028234663852886e38)  # +inf until the list is full
    for row in range(0, N, SCAN_BLOCK):
        var block = dist.unsafe_load[dtype=DType.float32, width=SCAN_BLOCK](Int(qid * N + row))
        # `worst` is the TOP_K-th best so far. A group whose minimum is not
        # smaller than `worst` cannot contain one of the TOP_K best.
        if block.reduce_min() >= worst:
            continue
        for j in range(SCAN_BLOCK):
            var d = block[j]
            if len(best) == TOP_K and d >= worst:
                continue
            var pos = len(best)
            while pos > 0 and best_d[pos - 1] > d:
                pos -= 1
            best.insert(pos, Int64(row + j))
            best_d.insert(pos, d)
            if len(best) > TOP_K:
                _ = best.pop()
                _ = best_d.pop()
                worst = best_d[TOP_K - 1]
    return best^


def copy_vectors(
    con: Connection, sql: String, dest: Pointer[Float32, MutUntrackedOrigin]
) raises:
    """Copy a FLOAT[K] column into a contiguous row-major buffer."""
    var offset = 0
    for chunk in con.execute(sql).chunks():
        var values = chunk.array_span[Float32](0)
        if not values:
            raise Error("expected a flat FLOAT[K] column without NULLs")
        var span = values.value()
        unsafe_memcpy(dest=dest.unsafe_offset(offset), src=span.unsafe_ptr(), count=len(span))
        offset += len(span)


def ms(start: Int, end: Int) -> Float64:
    return Float64(end - start) / 1e6


def fmt(x: Float64) -> String:
    """Round to one decimal place for printing."""
    return String(Float64(Int(x * 10 + 0.5)) / 10)


def main() raises:
    comptime assert N % ROWS_PER_BLOCK == 0, "N must be a multiple of ROWS_PER_BLOCK"
    comptime assert N % SCAN_BLOCK == 0, "N must be a multiple of SCAN_BLOCK"
    comptime if not has_accelerator():
        print("No GPU found, nothing to compare.")
        return
    if WARP_SIZE != 32:
        # ROW_F32 and the row slices below assume 32-lane warps.
        raise Error("this example assumes 32-lane warps")

    var con = duckdb.connect()
    var random_vector = String(
        "list_transform(range(", K, "), lambda x: random()::FLOAT)::FLOAT[", K, "]"
    )
    _ = con.execute(
        String(
            "SELECT setseed(0.42);",
            "CREATE TABLE emb AS SELECT i AS id, ", random_vector,
            " AS v FROM range(", N, ") t(i);",
            "CREATE TABLE queries AS SELECT i AS qid, ", random_vector,
            " AS v FROM range(", QUERIES, ") t(i);",
        )
    )
    print("Table: ", N, " rows of FLOAT[", K, "], ", QUERIES, " queries", sep="")

    # ---- DuckDB (CPU) ----
    # qid -1 is an untimed warm-up run of query 0.
    var cpu_results = List[List[Int64]]()
    var start = perf_counter_ns()
    for qid in range(-1, QUERIES):
        if qid == 0:
            start = perf_counter_ns()
        var sql = String(
            "SELECT id FROM emb ORDER BY array_cosine_distance(v, ",
            "(SELECT v FROM queries WHERE qid = ", max(qid, 0), ")) LIMIT ", TOP_K,
        )
        var ids = con.execute(sql).get[Int64]()
        if qid >= 0:
            cpu_results.append(ids^)
    var cpu_ms = ms(start, perf_counter_ns())

    # ---- GPU ----
    var ctx = DeviceContext()
    var emb_host = ctx.enqueue_create_host_buffer[DType.float32](N * K)
    var q_host = ctx.enqueue_create_host_buffer[DType.float32](QUERIES * K)
    var qn_host = ctx.enqueue_create_host_buffer[DType.float32](QUERIES)
    var dist_host = ctx.enqueue_create_host_buffer[DType.float32](N * QUERIES)
    var emb_dev = ctx.enqueue_create_buffer[DType.float32](N * K)
    var q_dev = ctx.enqueue_create_buffer[DType.float32](QUERIES * K)
    var qn_dev = ctx.enqueue_create_buffer[DType.float32](QUERIES)
    var dist_dev = ctx.enqueue_create_buffer[DType.float32](N * QUERIES)
    ctx.synchronize()

    # One-time cost: read the table out of DuckDB and upload it.
    start = perf_counter_ns()
    copy_vectors(con, "SELECT v FROM emb ORDER BY id", emb_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_copy(dst_buf=emb_dev, src_buf=emb_host)
    ctx.synchronize()
    var upload_ms = ms(start, perf_counter_ns())
    copy_vectors(con, "SELECT v FROM queries ORDER BY qid", q_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())

    # Query norms depend only on the query vectors, so they are computed here
    # once for the whole batch.
    for qid in range(QUERIES):
        var q = q_host.unsafe_ptr().unsafe_offset(qid * K)
        var qnorm = Float32(0)
        for i in range(K):
            qnorm += q[unsafe_offset=i] * q[unsafe_offset=i]
        qn_host[qid] = sqrt(qnorm)

    # Warm-up launch: the first launch compiles the kernel, outside the timed
    # window below.
    ctx.enqueue_copy(q_dev, q_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_copy(qn_dev, qn_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_function[cosine_kernel](
        emb_dev, q_dev, qn_dev, dist_dev, Int64(QUERIES),
        grid_dim=N // ROWS_PER_BLOCK, block_dim=ROWS_PER_BLOCK * WARP_SIZE,
    )
    ctx.synchronize()

    # Timed window: upload the queries, score all rows against all queries in
    # one launch, copy the QUERIES x N distances back, then pick the top-k on
    # the CPU.
    var mismatches = 0
    start = perf_counter_ns()
    ctx.enqueue_copy(q_dev, q_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_copy(qn_dev, qn_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_function[cosine_kernel](
        emb_dev, q_dev, qn_dev, dist_dev, Int64(QUERIES),
        grid_dim=N // ROWS_PER_BLOCK, block_dim=ROWS_PER_BLOCK * WARP_SIZE,
    )
    ctx.enqueue_copy(dst_buf=dist_host, src_buf=dist_dev)
    ctx.synchronize()
    for qid in range(QUERIES):
        if top_k(dist_host.unsafe_ptr(), qid) != cpu_results[qid]:
            mismatches += 1
    var gpu_ms = ms(start, perf_counter_ns())

    print("DuckDB array_cosine_distance: ", fmt(cpu_ms / QUERIES), " ms per query", sep="")
    print("GPU, table resident:          ", fmt(gpu_ms / QUERIES), " ms per query", sep="")
    print("  speedup:                    ", fmt(cpu_ms / gpu_ms), "x", sep="")
    print("  one-time upload:            ", fmt(upload_ms), " ms", sep="")
    print("Queries with a different top", TOP_K, ": ", mismatches, " of ", QUERIES, sep="")
