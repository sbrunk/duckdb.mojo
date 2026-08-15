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
    pixi run mojo run -I extensions/mojo-gpu-operator/src \
        extensions/mojo-gpu-operator/bench/native_decode_test.mojo
"""

from max.gpu.host import DeviceContext, DeviceBuffer
from std.memory import alloc
from std.sys import has_accelerator
from std.math import ceildiv

from native_decode import (
    bitpacking_decode_kernel,
    uncompressed_decode_kernel,
    BP_META_GROUP_SIZE,
    BPMODE_CONSTANT,
    BPMODE_CONSTANT_DELTA,
    BPMODE_DELTA_FOR,
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
@fieldwise_init
struct BuiltSegment(Copyable, Movable):
    var data: UnsafePointer[UInt8, MutAnyOrigin]
    var nbytes: Int
    var n_groups: Int


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


# Two's-complement bit-pattern (as UInt64) of a signed value of type T.
def _bits_t[T: DType](v: Scalar[T]) -> UInt64:
    comptime if T == DType.int32:
        return UInt64(UInt32(Int32(v)))
    else:
        return UInt64(Int64(v))


# ---------------------------------------------------------------------------
# Build a single CONSTANT_DELTA BITPACKING segment in DuckDB byte layout.
# Every metadata group is CONSTANT_DELTA: [T frame][T delta], decoded as
# out[i] = frame + i*delta (group_offset resets to 0 per metadata group).
# `vals` MUST already be the arithmetic series the caller chose so the decode
# round-trips: vals[g0 + i] == frame_g + i*delta_g.  `frames`/`deltas` give the
# per-group (frame, delta) used to generate them.
# ---------------------------------------------------------------------------
def build_constant_delta_segment[
    T: DType
](
    frames: UnsafePointer[Scalar[T], MutAnyOrigin],
    deltas: UnsafePointer[Scalar[T], MutAnyOrigin],
    n: Int,
) -> BuiltSegment:
    comptime TBYTES = 4 if T == DType.int32 else 8
    var n_groups = ceildiv(n, BP_META_GROUP_SIZE)

    var cap = 8 + 2 * TBYTES * n_groups + 4 * n_groups + 64
    var buf = alloc[UInt8](cap)
    for j in range(cap):
        buf[j] = 0

    var data_offs = alloc[Int](n_groups)
    var cursor = 8
    for g in range(n_groups):
        data_offs[g] = cursor
        # [T frame][T delta]
        put_u64_t[T](buf, cursor, _bits_t[T](frames[g]))
        put_u64_t[T](buf, cursor + TBYTES, _bits_t[T](deltas[g]))
        cursor += 2 * TBYTES

    var trailer_start = cursor
    var metadata_end = trailer_start + n_groups * 4
    for g in range(n_groups):
        var encoded = (UInt32(BPMODE_CONSTANT_DELTA) << 24) | (
            UInt32(data_offs[g]) & 0x00FFFFFF
        )
        var entry_off = metadata_end - (g + 1) * 4
        put_u32(buf, entry_off, encoded)
    put_u64(buf, 0, UInt64(metadata_end))

    data_offs.free()
    return BuiltSegment(buf, metadata_end, n_groups)


# ---------------------------------------------------------------------------
# Build a single DELTA_FOR BITPACKING segment in DuckDB byte layout from target
# output values `vals`. Each metadata group is DELTA_FOR:
#   [T frame][T width][T delta_offset][packed deltas LSB-first]
# This is the EXACT inverse of DuckDB's encoder (storage/compression/bitpacking.cpp
# BitpackingState::Flush + WriteDeltaFor) so the GPU decode round-trips:
#   frame        = minimum_delta = min_{i in [1,rows)}(vals[g0+i] - vals[g0+i-1])
#   delta_offset = vals[g0] - frame
#   packed[0]    = 0
#   packed[i]    = (vals[g0+i] - vals[g0+i-1]) - frame      (>= 0 by construction)
#   width        = min_width(max packed)
# Decode: out[i] = delta_offset + inclusive_prefix_sum_{j<=i}(frame + packed[j]).
# ---------------------------------------------------------------------------
def build_delta_for_segment[
    T: DType
](vals: UnsafePointer[Scalar[T], MutAnyOrigin], n: Int) -> BuiltSegment:
    comptime TBYTES = 4 if T == DType.int32 else 8
    var n_groups = ceildiv(n, BP_META_GROUP_SIZE)

    # header + per group (3*T header + worst-case packed TBYTES/row) + trailer.
    var cap = 8 + n * (TBYTES + 1) + 3 * TBYTES * n_groups + 4 * n_groups + 64
    var buf = alloc[UInt8](cap)
    for j in range(cap):
        buf[j] = 0

    var data_offs = alloc[Int](n_groups)
    var cursor = 8
    for g in range(n_groups):
        var g0 = g * BP_META_GROUP_SIZE
        var rows = BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= n else n - g0
        data_offs[g] = cursor

        # Signed deltas d[i] = vals[g0+i] - vals[g0+i-1] for i>=1.
        # minimum_delta = min over i in [1, rows); for a 1-row group DuckDB would
        # not pick DELTA_FOR, but keep it well-defined (frame=0).
        var min_delta = Int64(0)
        var have_delta = False
        for i in range(1, rows):
            var d = Int64(vals[g0 + i]) - Int64(vals[g0 + i - 1])
            if (not have_delta) or d < min_delta:
                min_delta = d
                have_delta = True
        var frame = min_delta  # frame_of_reference = minimum_delta (signed)

        # delta_offset = vals[g0] - frame   (seed for the inclusive scan)
        var delta_offset = Int64(vals[g0]) - frame

        # packed[i] = (signed delta - frame); packed[0] := 0 (DuckDB sets
        # delta_buffer[0] = minimum_delta, so packed[0] = min - min = 0).
        var offs = alloc[UInt64](rows)
        offs[0] = UInt64(0)
        var max_off = UInt64(0)
        for i in range(1, rows):
            var d = Int64(vals[g0 + i]) - Int64(vals[g0 + i - 1])
            var off_u = UInt64(d - frame)  # >= 0 since frame is the min delta
            offs[i] = off_u
            if off_u > max_off:
                max_off = off_u
        var width = min_width(max_off)

        # [T frame][T width][T delta_offset]
        var frame_bits: UInt64
        comptime if T == DType.int32:
            frame_bits = UInt64(UInt32(Int32(frame)))
        else:
            frame_bits = UInt64(Int64(frame))
        var doff_bits: UInt64
        comptime if T == DType.int32:
            doff_bits = UInt64(UInt32(Int32(delta_offset)))
        else:
            doff_bits = UInt64(Int64(delta_offset))
        put_u64_t[T](buf, cursor, frame_bits)
        put_u64_t[T](buf, cursor + TBYTES, UInt64(width))
        put_u64_t[T](buf, cursor + 2 * TBYTES, doff_bits)

        var packed_off = cursor + 3 * TBYTES
        var packed_words = ceildiv(rows * width, 32)
        var packed_ptr = (buf + packed_off).bitcast[UInt32]()
        pack_lsb_first(packed_ptr, offs, rows, width)
        offs.free()
        cursor += 3 * TBYTES + packed_words * 4

    var trailer_start = cursor
    var metadata_end = trailer_start + n_groups * 4
    for g in range(n_groups):
        var encoded = (UInt32(BPMODE_DELTA_FOR) << 24) | (
            UInt32(data_offs[g]) & 0x00FFFFFF
        )
        var entry_off = metadata_end - (g + 1) * 4
        put_u32(buf, entry_off, encoded)
    put_u64(buf, 0, UInt64(metadata_end))

    data_offs.free()
    return BuiltSegment(buf, metadata_end, n_groups)


# ---------------------------------------------------------------------------
# Decode a built segment on the GPU and assert bit-exact equality with `vals`.
# ---------------------------------------------------------------------------
# Decode an already-built segment on the GPU (one block per metadata group) and
# assert bit-exact equality with `vals`. Frees seg.data.
def run_built_segment[
    T: DType
](
    ctx: DeviceContext,
    name: String,
    var seg: BuiltSegment,
    vals: UnsafePointer[Scalar[T], MutAnyOrigin],
    n: Int,
) raises -> Bool:
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
    return run_built_segment[T](ctx, name, seg^, vals, n)


def run_constant_delta_case[
    T: DType
](
    ctx: DeviceContext,
    name: String,
    frames: UnsafePointer[Scalar[T], MutAnyOrigin],
    deltas: UnsafePointer[Scalar[T], MutAnyOrigin],
    vals: UnsafePointer[Scalar[T], MutAnyOrigin],
    n: Int,
) raises -> Bool:
    var seg = build_constant_delta_segment[T](frames, deltas, n)
    return run_built_segment[T](ctx, name, seg^, vals, n)


def run_delta_for_case[
    T: DType
](
    ctx: DeviceContext,
    name: String,
    vals: UnsafePointer[Scalar[T], MutAnyOrigin],
    n: Int,
) raises -> Bool:
    var seg = build_delta_for_segment[T](vals, n)
    return run_built_segment[T](ctx, name, seg^, vals, n)


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

    print("--- Phase D: CONSTANT_DELTA + DELTA_FOR ---")

    # ---- CONSTANT_DELTA int32 (2 groups + short tail) ----
    # Per-group arithmetic series: out[i] = frame_g + i*delta_g (group_offset
    # resets to 0 at the start of each metadata group).
    var nd1 = 2048 + 513
    var ngd1 = ceildiv(nd1, BP_META_GROUP_SIZE)
    var cd_frames32 = alloc[Int32](ngd1)
    var cd_deltas32 = alloc[Int32](ngd1)
    cd_frames32[0] = Int32(100000); cd_deltas32[0] = Int32(7)
    cd_frames32[1] = Int32(-2500);  cd_deltas32[1] = Int32(-3)  # negative frame + delta
    var vd1 = alloc[Int32](nd1)
    for g in range(ngd1):
        var g0 = g * BP_META_GROUP_SIZE
        var rows = BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= nd1 else nd1 - g0
        for i in range(rows):
            vd1[g0 + i] = cd_frames32[g] + Int32(i) * cd_deltas32[g]
    all_ok = run_constant_delta_case[DType.int32](
        ctx, "CONSTANT_DELTA int32 (2 groups, short tail, neg delta)",
        cd_frames32, cd_deltas32, vd1, nd1
    ) and all_ok

    # ---- CONSTANT_DELTA int64 (1 full group + 1-row tail) ----
    var nd2 = 2048 + 1
    var ngd2 = ceildiv(nd2, BP_META_GROUP_SIZE)
    var cd_frames64 = alloc[Int64](ngd2)
    var cd_deltas64 = alloc[Int64](ngd2)
    cd_frames64[0] = Int64(5_000_000_000); cd_deltas64[0] = Int64(86400)
    cd_frames64[1] = Int64(-9_000_000_000); cd_deltas64[1] = Int64(13)
    var vd2 = alloc[Int64](nd2)
    for g in range(ngd2):
        var g0 = g * BP_META_GROUP_SIZE
        var rows = BP_META_GROUP_SIZE if g0 + BP_META_GROUP_SIZE <= nd2 else nd2 - g0
        for i in range(rows):
            vd2[g0 + i] = cd_frames64[g] + Int64(i) * cd_deltas64[g]
    all_ok = run_constant_delta_case[DType.int64](
        ctx, "CONSTANT_DELTA int64 (2 groups, 1-row tail)",
        cd_frames64, cd_deltas64, vd2, nd2
    ) and all_ok

    # ---- DELTA_FOR int32: near-monotonic series (l_orderkey-like) ----
    # Mixed positive deltas with occasional jumps + a short final group.
    var nf1 = 2048 + 777
    var vf1 = alloc[Int32](nf1)
    var stf = UInt64(0x2468)
    var acc32 = Int32(50000)
    for i in range(nf1):
        acc32 = acc32 + Int32(1) + Int32(lcg(stf) % 40)  # strictly increasing, varied step
        vf1[i] = acc32
    all_ok = run_delta_for_case[DType.int32](
        ctx, "DELTA_FOR int32 (2 groups, short tail, increasing)", vf1, nf1
    ) and all_ok

    # ---- DELTA_FOR int32: NON-monotonic (negative deltas mixed in) ----
    var nf2 = 3000
    var vf2 = alloc[Int32](nf2)
    var stf2 = UInt64(0x13579)
    var acc2 = Int32(0)
    for i in range(nf2):
        acc2 = acc2 + Int32(lcg(stf2) % 200) - Int32(100)  # delta in [-100, +99]
        vf2[i] = acc2
    all_ok = run_delta_for_case[DType.int32](
        ctx, "DELTA_FOR int32 (non-monotonic, +/- deltas)", vf2, nf2
    ) and all_ok

    # ---- DELTA_FOR int64: large-magnitude near-monotonic (l_orderkey 8B-like) ----
    var nf3 = 2048 + 100
    var vf3 = alloc[Int64](nf3)
    var stf3 = UInt64(0xCAFE)
    var acc3 = Int64(1_000_000_000_000)
    for i in range(nf3):
        acc3 = acc3 + Int64(1) + Int64(lcg(stf3) % 1000)
        vf3[i] = acc3
    all_ok = run_delta_for_case[DType.int64](
        ctx, "DELTA_FOR int64 (2 groups, short tail, increasing)", vf3, nf3
    ) and all_ok

    # ---- DELTA_FOR int64: decreasing series (all-negative deltas) ----
    var nf4 = 2048
    var vf4 = alloc[Int64](nf4)
    var stf4 = UInt64(0xF00D)
    var acc4 = Int64(9_000_000_000)
    for i in range(nf4):
        acc4 = acc4 - Int64(1) - Int64(lcg(stf4) % 500)
        vf4[i] = acc4
    all_ok = run_delta_for_case[DType.int64](
        ctx, "DELTA_FOR int64 (decreasing, negative deltas)", vf4, nf4
    ) and all_ok

    cd_frames32.free(); cd_deltas32.free(); vd1.free()
    cd_frames64.free(); cd_deltas64.free(); vd2.free()
    vf1.free(); vf2.free(); vf3.free(); vf4.free()

    v32.free(); v64.free(); v3.free(); v4.free(); v5.free()
    v6.free(); v7.free(); v8.free()

    if all_ok:
        print("ALL PASS")
    else:
        print("SOME FAILED")
        raise Error("native_decode_test: bit-exact assertion failed")
