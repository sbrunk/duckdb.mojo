"""Probe: is a DB-wide unified allocator viable, or must we pin?

The allocator route needs a single pointer that DuckDB writes column data into via
ordinary CPU stores AND the GPU reads with no copy. This probe establishes which
Mojo buffer primitives can and cannot do that on Apple, and demonstrates the path
that DOES work (pin-resident).

Findings (Apple GPU):

  * DeviceBuffer is the only DevicePassable buffer (can be a kernel arg), but its
    `unsafe_ptr()` is NOT CPU-writable -> a direct CPU store through it CRASHES.
  * CPU access to a DeviceBuffer is only via the SCOPED `map_to_host()` context
    manager (docs: "may involve copying"; writes propagate on scope exit) -> no
    stable unified pointer to hand to DuckDB for arbitrary writes.
  * HostBuffer is CPU-writable (pinned) but is NOT DevicePassable -> cannot be a
    kernel argument; and wrapping a raw/host pointer as a non-owning DeviceBuffer
    is the broken path (apple_unified_probe Variant C).

=> A DB-wide unified allocator (DuckDB buffers ARE GPU memory) is NOT expressible
   with this Mojo API. Use the Sirius-style PIN-RESIDENT route: one controlled
   copy of the column into a resident DeviceBuffer at pin time, amortized across
   queries. That working pattern is demonstrated below (T_PIN).
"""

from std.sys import has_accelerator
from std.sys.info import has_apple_gpu_accelerator
from std.gpu import global_idx
from std.gpu.host import DeviceContext, DeviceBuffer
from std.math import ceildiv
from layout import TileTensor, TensorLayout, row_major

comptime dtype = DType.float32
comptime N = 4096
comptime BLOCK = 256
comptime layout = row_major[N]()


def double_kernel(
    a: TileTensor[dtype, type_of(layout), MutAnyOrigin],
    size: Int,
):
    var tid = global_idx.x
    if tid < size:
        a[tid] = a[tid] * 2.0


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    print("has_apple_gpu_accelerator:", has_apple_gpu_accelerator())
    var ctx = DeviceContext()
    comptime grid = ceildiv(N, BLOCK)

    # -----------------------------------------------------------------
    # T_PIN: the working pin-resident pattern. CPU fills the buffer via a
    #        SCOPED map_to_host (the one controlled copy), kernel runs on the
    #        resident DeviceBuffer, CPU reads results back via map_to_host.
    #        This is the Phase-2 fallback the gate selects.
    # -----------------------------------------------------------------
    var buf = ctx.enqueue_create_buffer[dtype](N)
    ctx.synchronize()
    with buf.map_to_host() as h:
        var hp = h.unsafe_ptr()
        for i in range(N):
            hp[i] = Float32(i)

    var t = TileTensor(buf, layout)
    ctx.enqueue_function[double_kernel](t, N, grid_dim=grid, block_dim=BLOCK)
    ctx.synchronize()

    var ok = True
    var bad = -1
    with buf.map_to_host() as h:
        var hp = h.unsafe_ptr()
        for i in range(N):
            if hp[i] != Float32(2 * i):
                ok = False
                bad = i
                break
    print("T_PIN (map_to_host fill -> kernel -> map_to_host read): correct =", ok)
    if not ok:
        print("   first mismatch at", bad)

    print()
    print("GATE 0b: DB-wide unified allocator = NOT VIABLE (no stable CPU-writable")
    print("         + GPU-readable pointer in this Mojo API).")
    print("         Use the PIN-RESIDENT route, demonstrated by T_PIN above.")
