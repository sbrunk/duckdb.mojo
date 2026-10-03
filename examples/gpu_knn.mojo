"""Exact vector search (k nearest neighbors) on the GPU, compared with DuckDB.

The embeddings live in a DuckDB table. We copy them to the GPU once and keep
them there. Each query then only uploads the query vector, runs one kernel that
scores every row, and copies the distances back. DuckDB has to scan and score
the whole table for every query, so the GPU is much faster once the data is on
the GPU.

Needs the `gpu` environment (it provides `max.gpu`):

    pixi run -e gpu mojo run examples/gpu_knn.mojo
"""

from duckdb import *
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


def cosine_kernel(
    emb: Pointer[Float32, MutUntrackedOrigin],
    q: Pointer[Float32, MutUntrackedOrigin],
    dist: Pointer[Float32, MutUntrackedOrigin],
    qnorm: Float32,
):
    """One warp per row: each lane sums a strided part of the dot product and
    the row norm, then `warp.sum` combines the lanes."""
    var row = Int(block_idx.x) * ROWS_PER_BLOCK + Int(thread_idx.x) // WARP_SIZE
    var lane = Int(thread_idx.x) % WARP_SIZE
    var dot = Float32(0)
    var norm = Float32(0)
    var i = lane
    while i < K:
        var v = emb[unsafe_offset=row * K + i]
        dot += v * q[unsafe_offset=i]
        norm += v * v
        i += WARP_SIZE
    dot = warp.sum(dot)
    norm = warp.sum(norm)
    if lane == 0:
        var denom = sqrt(norm) * qnorm
        dist[unsafe_offset=row] = Float32(1) - dot / denom if denom != 0 else Float32(1)


def top_k(dist: Pointer[Float32, _]) -> List[Int64]:
    """Indices of the TOP_K smallest distances, nearest first."""
    var best = List[Int64]()
    var best_d = List[Float32]()
    for row in range(N):
        var d = dist[unsafe_offset=row]
        if len(best) == TOP_K and d >= best_d[TOP_K - 1]:
            continue
        var pos = len(best)
        while pos > 0 and best_d[pos - 1] > d:
            pos -= 1
        best.insert(pos, Int64(row))
        best_d.insert(pos, d)
        if len(best) > TOP_K:
            _ = best.pop()
            _ = best_d.pop()
    return best^


def copy_vectors(
    con: Connection, sql: String, dest: Pointer[Float32, MutUntrackedOrigin]
) raises:
    """Copy a FLOAT[K] column into a contiguous row-major buffer."""
    var offset = 0
    for chunk in con.execute(sql).chunks():
        var rows = len(chunk)
        var values = chunk.get_vector(0).array_get_child()
        unsafe_memcpy(
            dest=dest.unsafe_offset(offset * K),
            src=values.get_data().unsafe_bitcast[Float32](),
            count=rows * K,
        )
        offset += rows


def ms(start: Int, end: Int) -> Float64:
    return Float64(end - start) / 1e6


def fmt(x: Float64) -> String:
    """Round to one decimal place for printing."""
    return String(Float64(Int(x * 10 + 0.5)) / 10)


def main() raises:
    comptime assert N % ROWS_PER_BLOCK == 0, "N must be a multiple of ROWS_PER_BLOCK"
    comptime if not has_accelerator():
        print("No GPU found, nothing to compare.")
        return

    var con = DuckDB.connect(":memory:")
    var random_vector = String(
        "list_transform(range(", K, "), x -> random()::FLOAT)::FLOAT[", K, "]"
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
        var ids = List[Int64]()
        var sql = String(
            "SELECT id FROM emb ORDER BY array_cosine_distance(v, ",
            "(SELECT v FROM queries WHERE qid = ", max(qid, 0), ")) LIMIT ", TOP_K,
        )
        for row in con.execute(sql):
            ids.append(row.get[Int64](col=0))
        if qid >= 0:
            cpu_results.append(ids^)
    var cpu_ms = ms(start, perf_counter_ns())

    # ---- GPU ----
    var ctx = DeviceContext()
    var emb_host = ctx.enqueue_create_host_buffer[DType.float32](N * K)
    var q_host = ctx.enqueue_create_host_buffer[DType.float32](QUERIES * K)
    var dist_host = ctx.enqueue_create_host_buffer[DType.float32](N)
    var emb_dev = ctx.enqueue_create_buffer[DType.float32](N * K)
    var q_dev = ctx.enqueue_create_buffer[DType.float32](K)
    var dist_dev = ctx.enqueue_create_buffer[DType.float32](N)
    ctx.synchronize()

    # One-time cost: read the table out of DuckDB and upload it.
    start = perf_counter_ns()
    copy_vectors(con, "SELECT v FROM emb ORDER BY id", emb_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())
    ctx.enqueue_copy(dst_buf=emb_dev, src_buf=emb_host)
    ctx.synchronize()
    var upload_ms = ms(start, perf_counter_ns())
    copy_vectors(con, "SELECT v FROM queries ORDER BY qid", q_host.unsafe_ptr().unsafe_origin_cast[MutUntrackedOrigin]())

    # qid -1 is an untimed warm-up run of query 0 (the first launch compiles
    # the kernel).
    var mismatches = 0
    for qid in range(-1, QUERIES):
        if qid == 0:
            start = perf_counter_ns()
        var q = q_host.unsafe_ptr().unsafe_offset(max(qid, 0) * K)
        var qnorm = Float32(0)
        for i in range(K):
            qnorm += q[unsafe_offset=i] * q[unsafe_offset=i]
        ctx.enqueue_copy(q_dev, q)
        ctx.enqueue_function[cosine_kernel](
            emb_dev, q_dev, dist_dev, sqrt(qnorm),
            grid_dim=N // ROWS_PER_BLOCK, block_dim=ROWS_PER_BLOCK * WARP_SIZE,
        )
        ctx.enqueue_copy(dst_buf=dist_host, src_buf=dist_dev)
        ctx.synchronize()
        var ids = top_k(dist_host.unsafe_ptr())
        if qid >= 0 and ids != cpu_results[qid]:
            mismatches += 1
    var gpu_ms = ms(start, perf_counter_ns())

    print("DuckDB array_cosine_distance: ", fmt(cpu_ms / QUERIES), " ms per query", sep="")
    print("GPU, table resident:          ", fmt(gpu_ms / QUERIES), " ms per query", sep="")
    print("  speedup:                    ", fmt(cpu_ms / gpu_ms), "x", sep="")
    print("  one-time upload:            ", fmt(upload_ms), " ms", sep="")
    print("Queries with a different top", TOP_K, ": ", mismatches, " of ", QUERIES, sep="")
