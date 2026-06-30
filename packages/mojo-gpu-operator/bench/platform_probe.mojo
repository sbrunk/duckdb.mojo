"""Stage-0 de-risk probe: confirm the GPU target-introspection API under the
pinned Mojo compiler before refactoring kernels for portability.

Resolves the plan's flagged risk: which symbols give (a) the warp size, (b) a
HOST decision "does this machine have an NVIDIA/Apple GPU", and (c) an
inside-kernel TARGET dispatch "am I being compiled for NVIDIA/AMD" (where
64-bit atomics are available).

Run: pixi run mojo run packages/mojo-gpu-operator/bench/platform_probe.mojo
"""

from std.gpu import WARP_SIZE, thread_idx, block_idx
from std.gpu.host import DeviceContext
from std.sys import has_accelerator
from std.sys.info import (
    is_gpu,
    is_nvidia_gpu,
    is_amd_gpu,
    is_apple_gpu,
    has_nvidia_gpu_accelerator,
    has_amd_gpu_accelerator,
    has_apple_gpu_accelerator,
)


# Inside-kernel TARGET dispatch: is_* reflects the GPU we are compiled for.
# Writes a small tag per the target's atomics capability so we can confirm the
# comptime branch compiles and runs on the actual device.
def probe_kernel(out_buf: UnsafePointer[Scalar[DType.int32], MutAnyOrigin]):
    if Int(block_idx.x) == 0 and Int(thread_idx.x) == 0:
        comptime has_64 = is_nvidia_gpu() or is_amd_gpu()
        comptime if has_64:
            out_buf[0] = 64  # NVIDIA / AMD: native 64-bit atomics path
        else:
            out_buf[0] = 32  # Apple (or other): no 64-bit atomics
        out_buf[1] = Int32(WARP_SIZE)
        comptime if is_apple_gpu():
            out_buf[2] = 1
        elif is_nvidia_gpu():
            out_buf[2] = 2
        elif is_amd_gpu():
            out_buf[2] = 3
        else:
            out_buf[2] = 0


def main() raises:
    # HOST decisions: has_* reflects the machine running this code.
    print("== host (has_*) ==")
    print("has_accelerator           :", has_accelerator())
    print("has_apple_gpu_accelerator :", has_apple_gpu_accelerator())
    print("has_nvidia_gpu_accelerator:", has_nvidia_gpu_accelerator())
    print("has_amd_gpu_accelerator   :", has_amd_gpu_accelerator())
    print("WARP_SIZE (host view)     :", WARP_SIZE)

    comptime if not has_accelerator():
        print("no accelerator; skipping kernel probe")
        return

    var ctx = DeviceContext()
    var buf = ctx.enqueue_create_buffer[DType.int32](3)
    buf.enqueue_fill(-1)
    ctx.enqueue_function[probe_kernel](buf.unsafe_ptr(), grid_dim=1, block_dim=32)
    ctx.synchronize()
    with buf.map_to_host() as host:
        print("== device (is_*, in-kernel) ==")
        print("atomics tag (64=native / 32=none):", host[0])
        print("WARP_SIZE (device view)          :", host[1])
        print("vendor tag (1=apple 2=nv 3=amd)  :", host[2])
