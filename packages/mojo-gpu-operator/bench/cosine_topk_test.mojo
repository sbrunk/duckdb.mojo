"""Exact GPU top-k test for the cosine/vector-search pin path.

Synthesizes an N x K embedding matrix + query vectors with a deterministic
generator (no random()), pins the matrix on the GPU, and runs the new
`mojo_gpu_pin_query_topk` / `mojo_gpu_pin_query_topk_batch` C-ABI entry points.

The GPU returns ONLY the k nearest rows (ids + distances). The CPU reference
computes all N cosine distances, stable-sorts by (dist asc, id asc), and takes
the first k. The test asserts the GPU's k ids AND dists equal the CPU top-k
exactly (dists within ~1e-6, ids identical under the same tie-break).

Run:
  pixi run mojo run -I packages/mojo-gpu-operator/src \
      packages/mojo-gpu-operator/bench/cosine_topk_test.mojo
"""

from std.sys import has_accelerator
from std.math import sqrt
from std.memory import alloc

from gpu_kernels import (
    mojo_gpu_pin,
    mojo_gpu_pin_query_topk,
    mojo_gpu_pin_query_topk_batch,
    mojo_gpu_pin_free,
    mojo_gpu_pin_f16,
    mojo_gpu_pin_query_topk_f16,
    mojo_gpu_pin_query_topk_batch_f16,
    mojo_gpu_pin_free_f16,
)

comptime N = 50_000
comptime K = 384


# Deterministic, index-varying generators (no random()). Mixed LCG-style so the
# distances are well-spread but reproducible run-to-run.
def emb_val(row: Int, i: Int) -> Float32:
    # Mix the row in non-periodically so distances genuinely vary across rows
    # (exercises the distance ordering), while the modulo still produces some
    # exact ties (exercises the (dist, rowid) tie-break).
    var idx = (row * 2654435761 + i * 40503 + 12345) & 0x7FFFFFFF
    return Float32(idx % 2048) * 0.001 + Float32(row % 7) * 0.0001


def query_val(m: Int, i: Int) -> Float32:
    return Float32(((m * 31 + i) * 48271) % 1024) * 0.002 + 0.001


# CPU reference: all N cosine distances for query at `q`, then a STABLE selection
# of the k smallest under (dist asc, id asc) -- matching the GPU tie-break.
def cpu_topk(
    emb: UnsafePointer[Float32, MutAnyOrigin],
    q: UnsafePointer[Float32, MutAnyOrigin],
    k: Int,
    out_ids: UnsafePointer[Int64, MutAnyOrigin],
    out_dists: UnsafePointer[Float32, MutAnyOrigin],
):
    var qnorm = Float32(0)
    for i in range(K):
        qnorm += q[i] * q[i]
    qnorm = sqrt(qnorm)

    var dists = alloc[Float32](N)
    for row in range(N):
        var base = row * K
        var dot = Float32(0)
        var na = Float32(0)
        for i in range(K):
            var av = emb[base + i]
            dot += av * q[i]
            na += av * av
        var denom = sqrt(na) * qnorm
        dists[row] = Float32(1) - dot / denom if denom != 0 else Float32(0)

    var taken = alloc[Bool](N)
    for r in range(N):
        taken[r] = False
    for slot in range(k):
        var best = -1
        var best_d = Float32(3.0e38)
        for r in range(N):
            if taken[r]:
                continue
            var d = dists[r]
            # (dist asc, id asc): on a tie the smaller id wins, and since `r`
            # ascends, the first-seen tie is already the smaller id.
            if best < 0 or d < best_d:
                best = r
                best_d = d
        taken[best] = True
        out_ids[slot] = Int64(best)
        out_dists[slot] = best_d
    dists.free()
    taken.free()


def check_topk(
    label: String,
    gpu_ids: UnsafePointer[Int64, MutAnyOrigin],
    gpu_dists: UnsafePointer[Float32, MutAnyOrigin],
    cpu_ids: UnsafePointer[Int64, MutAnyOrigin],
    cpu_dists: UnsafePointer[Float32, MutAnyOrigin],
    k: Int,
) raises:
    var ok = True
    for j in range(k):
        var id_ok = gpu_ids[j] == cpu_ids[j]
        var derr = abs(gpu_dists[j] - cpu_dists[j])
        var d_ok = derr <= Float32(1.0e-6)
        if not id_ok or not d_ok:
            ok = False
            print(
                "  MISMATCH",
                label,
                "slot",
                j,
                ": gpu(id=",
                gpu_ids[j],
                ", d=",
                gpu_dists[j],
                ") cpu(id=",
                cpu_ids[j],
                ", d=",
                cpu_dists[j],
                ")  |derr|=",
                derr,
            )
    if not ok:
        raise Error(String("FAIL: ") + label)
    print("  PASS", label, "(k =", k, ")")


def run_single(handle: Int, emb: UnsafePointer[Float32, MutAnyOrigin], k: Int) raises:
    var q = alloc[Float32](K)
    for i in range(K):
        q[i] = query_val(0, i)
    var q_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(q)
    )

    var gpu_ids = alloc[Int64](k)
    var gpu_dists = alloc[Float32](k)
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)
    var rc = mojo_gpu_pin_query_topk(h, q_imm, k, gpu_ids, gpu_dists)
    if rc != 0:
        raise Error(String("mojo_gpu_pin_query_topk rc=") + String(Int(rc)))

    var cpu_ids = alloc[Int64](k)
    var cpu_dists = alloc[Float32](k)
    cpu_topk(emb, q, k, cpu_ids, cpu_dists)

    check_topk(
        String("single k=") + String(k),
        gpu_ids,
        gpu_dists,
        cpu_ids,
        cpu_dists,
        k,
    )
    q.free()
    gpu_ids.free()
    gpu_dists.free()
    cpu_ids.free()
    cpu_dists.free()


def run_batch(handle: Int, emb: UnsafePointer[Float32, MutAnyOrigin], M: Int, k: Int) raises:
    var qs = alloc[Float32](M * K)
    for m in range(M):
        for i in range(K):
            qs[m * K + i] = query_val(m + 1, i)  # vary per batch query
    var qs_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(qs)
    )

    var gpu_ids = alloc[Int64](M * k)
    var gpu_dists = alloc[Float32](M * k)
    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)
    var rc = mojo_gpu_pin_query_topk_batch(h, qs_imm, M, k, gpu_ids, gpu_dists)
    if rc != 0:
        raise Error(
            String("mojo_gpu_pin_query_topk_batch rc=") + String(Int(rc))
        )

    var cpu_ids = alloc[Int64](k)
    var cpu_dists = alloc[Float32](k)
    for m in range(M):
        cpu_topk(emb, qs + m * K, k, cpu_ids, cpu_dists)
        check_topk(
            String("batch[") + String(m) + String("] k=") + String(k),
            gpu_ids + m * k,
            gpu_dists + m * k,
            cpu_ids,
            cpu_dists,
            k,
        )
    qs.free()
    gpu_ids.free()
    gpu_dists.free()
    cpu_ids.free()
    cpu_dists.free()


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("cosine top-k test: N =", N, " K =", K)

    var emb = alloc[Float32](N * K)
    for r in range(N):
        for i in range(K):
            emb[r * K + i] = emb_val(r, i)
    var emb_imm = UnsafePointer[Float32, ImmutAnyOrigin](
        unsafe_from_address=Int(emb)
    )

    var handle = mojo_gpu_pin(emb_imm, N, K)
    if handle == 0:
        raise Error("mojo_gpu_pin failed")

    run_single(handle, emb, 10)
    run_single(handle, emb, 100)
    run_batch(handle, emb, 8, 10)

    var h = UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=handle)
    mojo_gpu_pin_free(h)
    emb.free()
    print("ALL PASS")
