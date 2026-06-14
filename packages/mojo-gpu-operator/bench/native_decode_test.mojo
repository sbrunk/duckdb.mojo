"""Phase-B bit-exact test for the portable Mojo GPU native-storage decoders.

Builds synthetic column segments in DuckDB's EXACT BITPACKING byte layout from a
known host int32 / int64 array, uploads the raw bytes as uint8, runs the GPU
decode kernel from `native_decode.mojo`, and ASSERTS the decoded output equals
the original element-for-element.

Segment layout built here (matches duckdb/common/bitpacking.hpp +
storage/compression/bitpacking.hpp + Sirius gpu_decode_bitpacking.cu):

    [0..8)              uint64 metadata_end (offset of END of trailer in segment)
    ...group data...        FOR:      [T frame][T width][packed LSB-first]
                            CONSTANT: [T value]
    ...trailer (REVERSED): entry K (group K) at metadata_end-(K+1)*4
                            low 24 bits = data_off, high 8 bits = BitpackingMode

The host bit-packer is the exact inverse of `unpack_value` (LSB-first width-bit
fields in a uint32 stream), so a correct decode round-trips bit-for-bit.

Covers:
  * CONSTANT (all-equal) for int32 and int64
  * FOR (random in range) for int32 and int64, full + short final group
  * UNCOMPRESSED int32 (D2D typed copy)

Run from the repo root:
    pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/native_decode_test.mojo
"""

from std.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.math import ceildiv

from native_decode import (
    bitpacking_decode_kernel,
    uncompressed_decode_kernel,
    BP_META_GROUP_SIZE,
    BPMODE_CONSTANT,
    BPMODE_FOR,
)


# ---------------------------------------------------------------------------
# Host-side helpers operating on a growable byte buffer.
# ---------------------------------------------------------------------------
def put_u64(buf: UnsafePointer[UInt8, MutAnyOrigin], off: Int, v: UInt64):
    for b in range(8):
        buf[off + b] = UInt8((v >> UInt64(8 * b)) & 0xFF)


def put_u32(buf: UnsafePointer[UInt8, MutAnyOrigin], off: Int, v: UInt32):
    for b in range(4):
        buf[off + b] = UInt8((v >> UInt32(8 * b)) & 0xFF)


# Minimum bit width to store the unsigned values 0..maxv inclusive (LSB-first
# FOR offsets are non-negative). width 0 only if maxv == 0.
def min_width(maxv: UInt64) -> Int:
    if maxv == 0:
        return 0
    var w = 0
    var v = maxv
    while v != 0:
        w += 1
        v >>= 1
    return w


# Pack `vals[0..count)` (each already a width-bit unsigned offset) LSB-first into
# the uint32 stream `packed_bytes` starting at byte 0. This is the exact inverse
# of `unpack_value`. `packed_bytes` must be zero-initialized and large enough
# (ceil(count*width/32)*4 bytes + a guard word).
def pack_lsb_first(
    packed: UnsafePointer[UInt32, MutAnyOrigin], vals: UnsafePointer[UInt64, MutAnyOrigin], count: Int, width: Int
):
    if width == 0:
        return
    for idx in range(count):
        var v = vals[idx] & ((UInt64(1) << UInt64(width)) - 1) if width < 64 else vals[idx]
        var bit_pos = UInt64(idx) * UInt64(width)
        var word_idx = Int(bit_pos // 32)
        var bit_off = Int(bit_pos % 32)
        # low part into word_idx
        packed[word_idx] |= UInt32((v << UInt64(bit_off)) & 0xFFFFFFFF)
        var consumed = 32 - bit_off
        if width > consumed:
            packed[word_idx + 1] |= UInt32((v >> UInt64(consumed)) & 0xFFFFFFFF)
            # third word for 64-bit fields spanning 3 words
            if width > consumed + 32:
                packed[word_idx + 2] |= UInt32(
                    (v >> UInt64(consumed + 32)) & 0xFFFFFFFF
                )


# ---------------------------------------------------------------------------
# Build a single BITPACKING segment (one or more groups) in DuckDB byte layout
# for type T from host values `vals[0..n)`. Returns (bytes_ptr, seg_bytes).
# Each group uses FOR (frame=min over the group) unless all values in the group
# are equal, in which case CONSTANT is emitted (exercising both modes from one
# builder, exactly as DuckDB chooses per group).
# ---------------------------------------------------------------------------
struct BuiltSegment:
    var data: UnsafePointer[UInt8, MutAnyOrigin]
    var nbytes: Int
    var n_groups: Int

    def __init__(out self, data: UnsafePointer[UInt8, MutAnyOrigin], nbytes: Int, n_groups: Int):
        self.data = data
        self.nbytes = nbytes
        self.n_groups = n_groups


def build_bitpacking_segment[
    T: DType
](vals: UnsafePointer[Scalar[T], MutAnyOrigin], n: Int, force_constant: Bool) -> BuiltSegment:
    comptime TBYTES = 4 if T == DType.int32 else 8
    var n_groups = ceildiv(n, BP_META_GROUP_SIZE)

    # First pass: lay out group data after the 8-byte header, recording each
    # group's data_off + mode for the trailer.
    # Generous over-allocation: header + per group (header + worst-case packed) +
    # trailer; worst case packed = TBYTES per row.
    var cap = 8 + n * (TBYTES + 1) + 3 * TBYTES * n_groups + 4 * n_groups + 64
    var buf = alloc[UInt8](cap)
    for j in range(cap):
        buf[j] = 0

    var data_offs = alloc[Int](n_groups)
    var modes = alloc[Int](n_groups)

    var cursor = 8  # group data starts right after the header
    for g in range(n_groups):
        var g0 = g * BP_META_GROUP_SIZE
        var rows = BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= n else n - g0

        # Per-group min/max for FOR frame + width.
        var mn = vals[g0]
        var mx = vals[g0]
        for r in range(1, rows):
            var x = vals[g0 + r]
            if x < mn:
                mn = x
            if x > mx:
                mx = x

        var all_equal = mn == mx
        data_offs[g] = cursor

        if force_constant or all_equal:
            # CONSTANT: [T value]
            modes[g] = BPMODE_CONSTANT
            var v: UInt64
            comptime if T == DType.int32:
                v = UInt64(UInt32(Int32(mn)))  # zero-extend the 32-bit pattern
            else:
                v = UInt64(Int64(mn))
            put_u64_t[T](buf, cursor, v)
            cursor += TBYTES
        else:
            # FOR: [T frame][T width][packed offsets LSB-first]
            modes[g] = BPMODE_FOR
            var frame = mn
            # offset range = max - min (unsigned)
            var range_u: UInt64
            comptime if T == DType.int32:
                range_u = UInt64(UInt32(Int32(mx) - Int32(mn)))
            else:
                range_u = UInt64(Int64(mx) - Int64(mn))
            var width = min_width(range_u)

            # write frame (T) then width (1 byte significant, stored as T slot)
            var fbits: UInt64
            comptime if T == DType.int32:
                fbits = UInt64(UInt32(Int32(frame)))
            else:
                fbits = UInt64(Int64(frame))
            put_u64_t[T](buf, cursor, fbits)
            # width occupies the next T slot; only the low byte is meaningful
            # (the kernel reads seg[data_off + TBYTES] as the width byte).
            put_u64_t[T](buf, cursor + TBYTES, UInt64(width))

            var packed_off = cursor + 2 * TBYTES
            # build the per-row unsigned offsets
            var offs = alloc[UInt64](rows)
            for r in range(rows):
                comptime if T == DType.int32:
                    offs[r] = UInt64(UInt32(Int32(vals[g0 + r]) - Int32(frame)))
                else:
                    offs[r] = UInt64(Int64(vals[g0 + r]) - Int64(frame))
            var packed_words = ceildiv(rows * width, 32)
            var packed_ptr = (buf + packed_off).bitcast[UInt32]()
            pack_lsb_first(packed_ptr, offs, rows, width)
            offs.free()
            cursor += 2 * TBYTES + packed_words * 4

    # Trailer: one uint32 per group, REVERSED. metadata_end = end of trailer.
    var trailer_start = cursor
    var metadata_end = trailer_start + n_groups * 4
    for g in range(n_groups):
        var encoded = (UInt32(modes[g]) << 24) | (UInt32(data_offs[g]) & 0x00FFFFFF)
        var entry_off = metadata_end - (g + 1) * 4
        put_u32(buf, entry_off, encoded)

    put_u64(buf, 0, UInt64(metadata_end))

    data_offs.free()
    modes.free()
    return BuiltSegment(buf, metadata_end, n_groups)


# Store the low TBYTES bytes of an unsigned bit-pattern into buf at off.
def put_u64_t[T: DType](buf: UnsafePointer[UInt8, MutAnyOrigin], off: Int, v: UInt64):
    comptime TBYTES = 4 if T == DType.int32 else 8
    for b in range(TBYTES):
        buf[off + b] = UInt8((v >> UInt64(8 * b)) & 0xFF)


# ---------------------------------------------------------------------------
# Decode a built segment on the GPU and assert bit-exact equality with `vals`.
# ---------------------------------------------------------------------------
def run_bitpacking_case[
    T: DType
](
    ctx: DeviceContext,
    name: String,
    vals: UnsafePointer[Scalar[T], MutAnyOrigin],
    n: Int,
    force_constant: Bool,
) raises -> Bool:
    var seg = build_bitpacking_segment[T](vals, n, force_constant)

    # Upload raw segment bytes as uint8.
    var seg_d = ctx.enqueue_create_buffer[DType.uint8](seg.nbytes)
    var out_d = ctx.enqueue_create_buffer[T](n)
    ctx.synchronize()
    ctx.enqueue_copy(seg_d, seg.data)
    ctx.synchronize()

    # One block per metadata group; 256 threads (covers 2048 rows in 8 strides).
    comptime kernel = bitpacking_decode_kernel[T]
    for g in range(seg.n_groups):
        var g0 = g * BP_META_GROUP_SIZE
        var rows = BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= n else n - g0
        ctx.enqueue_function[kernel](
            seg_d,
            seg.nbytes,
            out_d,
            g,         # group_idx
            rows,      # group_rows
            g0,        # out_row_offset
            grid_dim=1,
            block_dim=256,
        )
    ctx.synchronize()

    var out_h = alloc[Scalar[T]](n)
    var out_sub = DeviceBuffer(ctx, out_d.unsafe_ptr(), n, owning=False)
    ctx.enqueue_copy(out_h, out_sub)
    ctx.synchronize()

    var ok = True
    var first_bad = -1
    for i in range(n):
        if out_h[i] != vals[i]:
            ok = False
            if first_bad < 0:
                first_bad = i
    if ok:
        print("  PASS", name, "(", n, "rows,", seg.n_groups, "groups,", seg.nbytes, "bytes )")
    else:
        print("  FAIL", name, "first mismatch at row", first_bad,
              "got", out_h[first_bad], "want", vals[first_bad])

    out_h.free()
    seg.data.free()
    return ok


def run_uncompressed_case[
    T: DType
](ctx: DeviceContext, name: String, vals: UnsafePointer[Scalar[T], MutAnyOrigin], n: Int) raises -> Bool:
    comptime TBYTES = 4 if T == DType.int32 else 8
    # Raw segment = the values laid out contiguously (data_off = 0).
    var nbytes = n * TBYTES
    var buf = alloc[UInt8](nbytes)
    for i in range(n):
        var v = UInt64(0)
        comptime if T == DType.int32:
            v = UInt64(UInt32(Int32(vals[i])))
        else:
            v = UInt64(Int64(vals[i]))
        for b in range(TBYTES):
            buf[i * TBYTES + b] = UInt8((v >> UInt64(8 * b)) & 0xFF)

    var seg_d = ctx.enqueue_create_buffer[DType.uint8](nbytes)
    var out_d = ctx.enqueue_create_buffer[T](n)
    ctx.synchronize()
    ctx.enqueue_copy(seg_d, buf)
    ctx.synchronize()

    comptime kernel = uncompressed_decode_kernel[T]
    ctx.enqueue_function[kernel](
        seg_d, out_d, n, 0, 0,
        grid_dim=256, block_dim=256,
    )
    ctx.synchronize()

    var out_h = alloc[Scalar[T]](n)
    var out_sub = DeviceBuffer(ctx, out_d.unsafe_ptr(), n, owning=False)
    ctx.enqueue_copy(out_h, out_sub)
    ctx.synchronize()

    var ok = True
    var first_bad = -1
    for i in range(n):
        if out_h[i] != vals[i]:
            ok = False
            if first_bad < 0:
                first_bad = i
    if ok:
        print("  PASS", name, "(", n, "rows uncompressed )")
    else:
        print("  FAIL", name, "first mismatch at row", first_bad,
              "got", out_h[first_bad], "want", vals[first_bad])
    out_h.free()
    buf.free()
    return ok


# Simple LCG for reproducible pseudo-random test data.
def lcg(mut state: UInt64) -> UInt64:
    state = state * 6364136223846793005 + 1442695040888963407
    return state >> 16


def main() raises:
    comptime assert has_accelerator(), "requires a GPU"
    var ctx = DeviceContext()
    var all_ok = True

    print("=== Phase-B native-storage decode bit-exact test ===")

    # ---- FOR int32, full group (2048) + short final group ----
    var n1 = 2048 + 777
    var v32 = alloc[Int32](n1)
    var st = UInt64(0x1234567)
    var base32 = Int32(19723)  # a plausible date-day frame (l_shipdate-like)
    for i in range(n1):
        v32[i] = base32 + Int32(lcg(st) % 2000)  # range fits ~11 bits
    all_ok = run_bitpacking_case[DType.int32](
        ctx, "FOR int32 (2 groups, short tail)", v32, n1, False
    ) and all_ok

    # ---- FOR int64, full group + short final group ----
    var n2 = 2048 + 1
    var v64 = alloc[Int64](n2)
    var st2 = UInt64(0x9999)
    var base64 = Int64(1_000_000_000)
    for i in range(n2):
        v64[i] = base64 + Int64(lcg(st2) % 5_000_000)  # ~23-bit offsets
    all_ok = run_bitpacking_case[DType.int64](
        ctx, "FOR int64 (2 groups, 1-row tail)", v64, n2, False
    ) and all_ok

    # ---- FOR int32 with NEGATIVE frame (signed frame, two's-complement) ----
    var n3 = 1000
    var v3 = alloc[Int32](n3)
    var st3 = UInt64(0x55)
    for i in range(n3):
        v3[i] = Int32(-500) + Int32(lcg(st3) % 1000)  # spans negative..positive
    all_ok = run_bitpacking_case[DType.int32](
        ctx, "FOR int32 negative frame", v3, n3, False
    ) and all_ok

    # ---- CONSTANT int32 (all-equal) ----
    var n4 = 3000
    var v4 = alloc[Int32](n4)
    for i in range(n4):
        v4[i] = Int32(424242)
    all_ok = run_bitpacking_case[DType.int32](
        ctx, "CONSTANT int32 (all-equal)", v4, n4, True
    ) and all_ok

    # ---- CONSTANT int64 (all-equal) ----
    var n5 = 5000
    var v5 = alloc[Int64](n5)
    for i in range(n5):
        v5[i] = Int64(-7_000_000_000)
    all_ok = run_bitpacking_case[DType.int64](
        ctx, "CONSTANT int64 (all-equal, negative)", v5, n5, True
    ) and all_ok

    # ---- width edge cases: width 1 (0/1) and width that fills the type ----
    var n6 = 2048
    var v6 = alloc[Int32](n6)
    var st6 = UInt64(0xABCDEF)
    for i in range(n6):
        v6[i] = Int32(lcg(st6) & 1)  # only 0/1 -> width 1, frame 0
    all_ok = run_bitpacking_case[DType.int32](
        ctx, "FOR int32 width=1 (0/1)", v6, n6, False
    ) and all_ok

    # ---- UNCOMPRESSED int32 + int64 (D2D typed copy) ----
    var n7 = 4097
    var v7 = alloc[Int32](n7)
    var st7 = UInt64(0xDEAD)
    for i in range(n7):
        v7[i] = Int32(lcg(st7) % 0xFFFFFFFF) - Int32(2_000_000_000)
    all_ok = run_uncompressed_case[DType.int32](
        ctx, "UNCOMPRESSED int32", v7, n7
    ) and all_ok

    var n8 = 1234
    var v8 = alloc[Int64](n8)
    var st8 = UInt64(0xBEEF)
    for i in range(n8):
        v8[i] = Int64(lcg(st8)) - Int64(5_000_000_000)
    all_ok = run_uncompressed_case[DType.int64](
        ctx, "UNCOMPRESSED int64", v8, n8
    ) and all_ok

    v32.free(); v64.free(); v3.free(); v4.free(); v5.free()
    v6.free(); v7.free(); v8.free()

    if all_ok:
        print("ALL PASS")
    else:
        print("SOME FAILED")
        raise Error("native_decode_test: bit-exact assertion failed")
