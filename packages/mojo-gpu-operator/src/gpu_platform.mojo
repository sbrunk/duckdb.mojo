"""Portable GPU platform constants for the mojo-gpu-operator kernels.

`WARP` is the target warp / SIMD-group width — 32 on Apple, NVIDIA and AMD RDNA;
64 on AMD CDNA. `WARP_SIZE` resolves correctly both host-side (for `block_dim=`)
and device-side (in-kernel strides), verified by `bench/platform_probe.mojo`.

64-bit-atomics gating is intentionally NOT a module-level constant here: `is_*`
target checks must be evaluated INSIDE kernel code (the GPU compilation target),
not in host context. Kernels gate with a local
`comptime has_64 = is_nvidia_gpu() or is_amd_gpu()` (see memory
`mojo-gpu-target-introspection`).
"""

from std.gpu import WARP_SIZE

comptime WARP = WARP_SIZE
