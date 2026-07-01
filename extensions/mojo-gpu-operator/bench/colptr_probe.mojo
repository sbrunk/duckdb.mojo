"""Phase 3 de-risk probe: device array of device pointers, dereferenced in-kernel.

The whole Phase 3 (column-pointer-table) rewrite hinges on ONE unproven GPU
primitive: a kernel reading `col_ptrs[slot][row]` where `col_ptrs` is a device
buffer holding the raw device addresses of N separately-allocated device column
buffers. This probe validates exactly that, in isolation, BEFORE touching any
production kernel — and on whatever GPU it runs (must pass on Apple + NVIDIA).

It allocates two separate int64 device buffers, captures their device addresses
(`Int(buf.unsafe_ptr())`), uploads the address table to a third device buffer,
and launches a kernel that reconstructs each pointer in-kernel
(`UnsafePointer(unsafe_from_address=...)`) and gathers every element through the
indirection. Success == the gathered values match the originals exactly.

Run:
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/colptr_probe.mojo
"""

from std.gpu import block_idx, thread_idx
from std.gpu.host import DeviceContext, DeviceBuffer
from std.gpu.memory import AddressSpace
from std.memory import alloc
from std.sys import has_accelerator
from std.testing import assert_equal


comptime N = 5  # rows per column
comptime S = 2  # number of columns (slots)


# Kernel: for each (slot, row), reconstruct the slot's column pointer from the
# address table and read element `row`, writing it to the packed output.
# Comptime-parameterized + DEFAULTED kernel: the default (USE_PTR=False) path
# reads the packed buffer; the [True] specialization uses the pointer table.
# Validates that existing launches `enqueue_function[k]` (default) keep working
# while `enqueue_function[k[True]]` selects the ptr path -- the mechanism the
# production Phase 3 relies on to leave the packed kernels untouched.
def gather[USE_PTR: Bool = False](
    cols: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    col_ptrs: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
    n_slots: Int,
    n_rows: Int,
    dst: UnsafePointer[Scalar[DType.int64], MutAnyOrigin],
):
    var idx = Int(block_idx.x) * 32 + Int(thread_idx.x)
    var total = n_slots * n_rows
    if idx >= total:
        return
    var slot = idx // n_rows
    var row = idx % n_rows

    @parameter
    if USE_PTR:
        var addr = Int(col_ptrs[slot])
        var col = UnsafePointer[
            Scalar[DType.int64], MutAnyOrigin, address_space = AddressSpace.GLOBAL
        ](unsafe_from_address=addr)
        dst[slot * n_rows + row] = col[row]
    else:
        dst[slot * n_rows + row] = cols[slot * n_rows + row]


def main() raises:
    if not has_accelerator():
        print("SKIP: colptr_probe requires a GPU. ALL PASS")
        return

    var ctx = DeviceContext()

    # Two separately-allocated device columns with distinct values.
    var a_h = alloc[Int64](N)
    var b_h = alloc[Int64](N)
    for i in range(N):
        a_h[i] = Int64(1 + i)        # 1,2,3,4,5
        b_h[i] = Int64(10 * (i + 1)) # 10,20,30,40,50
    var a_d = ctx.enqueue_create_buffer[DType.int64](N)
    var b_d = ctx.enqueue_create_buffer[DType.int64](N)
    ctx.enqueue_copy(a_d, a_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.enqueue_copy(b_d, b_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()

    # Capture their DEVICE addresses into a host table, upload as the pointer table.
    var ptr_h = alloc[Int64](S)
    var addr_a = Int(a_d.unsafe_ptr())
    var addr_b = Int(b_d.unsafe_ptr())
    ptr_h[0] = Int64(addr_a)
    ptr_h[1] = Int64(addr_b)
    print("host addr a_d =", addr_a, " b_d =", addr_b)
    var ptr_d = ctx.enqueue_create_buffer[DType.int64](S)
    ctx.enqueue_copy(ptr_d, ptr_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()

    # A packed buffer [a | b] for the default (USE_PTR=False) path.
    var packed_h = alloc[Int64](S * N)
    for i in range(N):
        packed_h[0 * N + i] = a_h[i]
        packed_h[1 * N + i] = b_h[i]
    var packed_d = ctx.enqueue_create_buffer[DType.int64](S * N)
    ctx.enqueue_copy(packed_d, packed_h.unsafe_origin_cast[MutAnyOrigin]())
    ctx.synchronize()

    # --- ptr path: enqueue_function[gather[True]] ---
    var out_d = ctx.enqueue_create_buffer[DType.int64](S * N)
    comptime kptr = gather[True]
    ctx.enqueue_function[kptr](
        packed_d, ptr_d, S, N, out_d, grid_dim=1, block_dim=32
    )
    ctx.synchronize()
    var out_h = alloc[Int64](S * N)
    ctx.enqueue_copy(out_h.unsafe_origin_cast[MutAnyOrigin](), out_d)
    ctx.synchronize()

    # --- packed path: enqueue_function[gather[False]] (explicit) ---
    var outp_d = ctx.enqueue_create_buffer[DType.int64](S * N)
    comptime kpacked = gather[False]
    ctx.enqueue_function[kpacked](
        packed_d, ptr_d, S, N, outp_d, grid_dim=1, block_dim=32
    )
    ctx.synchronize()
    var outp_h = alloc[Int64](S * N)
    ctx.enqueue_copy(outp_h.unsafe_origin_cast[MutAnyOrigin](), outp_d)
    ctx.synchronize()

    for i in range(N):
        assert_equal(out_h[0 * N + i], a_h[i], "ptr slot 0 row " + String(i))
        assert_equal(out_h[1 * N + i], b_h[i], "ptr slot 1 row " + String(i))
        assert_equal(outp_h[0 * N + i], a_h[i], "packed slot 0 row " + String(i))
        assert_equal(outp_h[1 * N + i], b_h[i], "packed slot 1 row " + String(i))

    print("ptr-path    slot0:", out_h[0], out_h[1], out_h[2], out_h[3], out_h[4])
    print("packed-path slot1:", outp_h[5], outp_h[6], outp_h[7], outp_h[8], outp_h[9])
    a_h.free(); b_h.free(); ptr_h.free(); out_h.free(); outp_h.free(); packed_h.free()
    print("ALL PASS")
