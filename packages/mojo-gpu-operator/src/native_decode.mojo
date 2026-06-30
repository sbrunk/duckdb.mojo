"""Portable Mojo GPU decoders for DuckDB's native fixed-width column storage.

This is the GPU-direct decode path: read DuckDB's on-disk/pinned column-segment
bytes and decode them ON the GPU, in pure portable Mojo (Sirius's equivalents
are CUDA-only). It is a *parallel*, bit-exact verification path next to the
existing CPU-materialize cold path -- NOT yet a replacement for it.

Two codecs are implemented here, comptime-specialized per element type T
(int32 / int64):

  * UNCOMPRESSED fixed-width -- a straight device-to-device typed copy.
  * BITPACKING -- one CTA (block) per 2048-row metadata group. The block reads
    the segment trailer for its group, recovers the per-group mode + header,
    then decodes each row. All four fixed-width modes are implemented:
    CONSTANT, FOR, CONSTANT_DELTA, and DELTA_FOR (the last needs an in-group
    prefix-sum). INVALID / unknown modes route to a deterministic zero-fill.

BITPACKING byte layout per segment (matches DuckDB v1.5.4
common/bitpacking.hpp + storage/compression/bitpacking.hpp and Sirius's
gpu_decode_bitpacking.cu):

    base[0..8)              uint64 metadata_end  (offset of END of trailer within segment)
    ...data groups...
    ...trailer (one uint32 per group, REVERSED): entry K at metadata_end-(K+1)*4
                                                  low 24 bits = data_off,
                                                  high 8 bits = BitpackingMode

  Group data (FOR):            [T frame][T width][packed...]  packed @ data_off+2*sizeof(T)
                               out[i] = frame + unpack_value(width, i)
  Group data (CONSTANT):       [T value]   broadcast to every row
  Group data (CONSTANT_DELTA): [T frame][T delta]   (no packed stream)
                               out[i] = frame + i*delta
  Group data (DELTA_FOR):      [T frame][T width][T delta_offset][packed...]
                               packed @ data_off+3*sizeof(T). Decode = unpack
                               each width-bit value, add frame to EACH, take the
                               inclusive prefix-sum over the group, then add the
                               delta_offset bias:
                                 out[i] = delta_offset + sum_{j=0..i}(frame+unpack(j))
                               (matches DuckDB ApplyFrameOfReference + DeltaDecode
                               in storage/compression/bitpacking.cpp; the bias is
                               folded into data[0] before the inclusive scan.)

`unpack_value` is the single load-bearing bit-twiddle (LSB-first width-bit field
extraction from a uint32 stream), ported exactly from Sirius
include/cuda/scan/unpack_value.cuh.

These kernels live here and are imported by gpu_kernels.mojo (the dylib root,
where the @export C-ABI wrappers must live per the build).
"""

from std.gpu import block_idx, thread_idx, block_dim, grid_dim, barrier
from std.gpu.memory import AddressSpace
from std.memory import stack_allocation, bitcast
from std.math import ceildiv


# DuckDB's metadata group size for bitpacking (storage/compression bitpacking):
# one trailer entry per 2048-row group. (NB: the *packing* granularity inside a
# group is 32, but `unpack_value(idx)` treats the whole group as one contiguous
# LSB-first stream, which is exactly how DuckDB lays it out.)
comptime BP_META_GROUP_SIZE = 2048

# BitpackingMode enum (duckdb/storage/compression/bitpacking.hpp):
#   INVALID=0, AUTO=1, CONSTANT=2, CONSTANT_DELTA=3, DELTA_FOR=4, FOR=5
comptime BPMODE_INVALID = 0
comptime BPMODE_AUTO = 1
comptime BPMODE_CONSTANT = 2
comptime BPMODE_CONSTANT_DELTA = 3
comptime BPMODE_DELTA_FOR = 4
comptime BPMODE_FOR = 5


# Helper: unsigned DType of the same width as T (for the bitcast round-trip).
@always_inline
def _unsigned_of[T: DType]() -> DType:
    comptime if T == DType.int32:
        return DType.uint32
    else:
        return DType.uint64


# Helper: byte width of T (comptime). int32 -> 4, int64 -> 8.
@always_inline
def _tbytes[T: DType]() -> Int:
    comptime if T == DType.int32:
        return 4
    else:
        return 8


# ---------------------------------------------------------------------------
# unpack_value: read one width-bit value from a uint32 LSB-first packed stream
# at logical index `idx`. Ported exactly from Sirius unpack_value.cuh.
#
# For T wider than 32 bits a value can span three 32-bit words when bit_off>0
# and bit_off+width>64; the caller must guarantee one guard word past the live
# data so that third read is in-bounds (the test/decode path zero-pads).
# Returns an unsigned 64-bit field; the caller casts to T (two's-complement
# bit-reinterpret) and adds the frame.
# ---------------------------------------------------------------------------
@always_inline
def unpack_value(
    packed: UnsafePointer[Scalar[DType.uint32], MutAnyOrigin],
    idx: Int,
    width: Int,
) -> UInt64:
    comptime WORD_BITS = 32
    if width == 0:
        return UInt64(0)

    var bit_pos = UInt64(idx) * UInt64(width)
    var word_idx = Int(bit_pos // UInt64(WORD_BITS))
    var bit_off = Int(bit_pos % UInt64(WORD_BITS))

    var result = UInt64(packed[word_idx])
    if bit_off + width > WORD_BITS:
        result |= UInt64(packed[word_idx + 1]) << UInt64(WORD_BITS)
    result >>= UInt64(bit_off)

    # For 8-byte types a value can need bits from the second-next word.
    if bit_off > 0 and bit_off + width > 2 * WORD_BITS:
        result |= UInt64(packed[word_idx + 2]) << UInt64(64 - bit_off)

    var mask: UInt64
    if width >= 64:
        mask = ~UInt64(0)
    else:
        mask = (UInt64(1) << UInt64(width)) - 1
    return result & mask


# ---------------------------------------------------------------------------
# Typed little-endian scalar loads from a raw byte pointer at a byte offset.
# (memcpy-equivalent; the segment bytes are native little-endian.)
# ---------------------------------------------------------------------------
@always_inline
def _load_u64(p: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin], off: Int) -> UInt64:
    var v = UInt64(0)
    comptime for b in range(8):
        v |= UInt64(p[off + b]) << UInt64(8 * b)
    return v


@always_inline
def _load_u32(p: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin], off: Int) -> UInt32:
    var v = UInt32(0)
    comptime for b in range(4):
        v |= UInt32(p[off + b]) << UInt32(8 * b)
    return v


# ---------------------------------------------------------------------------
# UNCOMPRESSED fixed-width: device-to-device typed copy of `n_rows` values of
# type T from the segment payload (starting at `data_off` bytes) into dst[].
# One thread per row, grid-strided. T is comptime-specialized (int32/int64).
# ---------------------------------------------------------------------------
def uncompressed_decode_kernel[
    T: DType
](
    seg: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    dst: UnsafePointer[Scalar[T], MutAnyOrigin],
    n_rows: Int,
    data_off: Int,
    out_row_offset: Int,
):
    comptime TBYTES = _tbytes[T]()
    comptime UT = _unsigned_of[T]()
    var tid = Int(block_idx.x) * Int(block_dim.x) + Int(thread_idx.x)
    var stride = Int(block_dim.x) * Int(grid_dim.x)
    var i = tid
    while i < n_rows:
        # Reassemble the little-endian T value byte-by-byte (no alignment
        # assumptions on the source offset).
        var base = data_off + i * TBYTES
        var v = UInt64(0)
        comptime for b in range(TBYTES):
            v |= UInt64(seg[base + b]) << UInt64(8 * b)
        dst[out_row_offset + i] = bitcast[T, 1](v.cast[UT]())
        i += stride


# ---------------------------------------------------------------------------
# BITPACKING decode: ONE block per 2048-row metadata group.
#
# `seg`         raw segment bytes
# `seg_bytes`   size of the staged segment buffer (defensive bound)
# `dst`         decoded column output
# `group_idx`   this group's metadata-group index within the segment
# `group_rows`  rows in this group (last group may be < 2048)
# `out_row_offset` output offset, in rows, for this group (segment_start + g*2048)
#
# Thread 0 parses the trailer + per-group header into shared scalars; the block
# barriers, then every thread strides its rows and stores frame + unpack(width).
# CONSTANT broadcasts the single stored value. DELTA_FOR / INVALID / unknown
# modes zero-fill the group's row range deterministically.
# ---------------------------------------------------------------------------
def bitpacking_decode_kernel[
    T: DType
](
    seg: UnsafePointer[Scalar[DType.uint8], MutAnyOrigin],
    seg_bytes: Int,
    dst: UnsafePointer[Scalar[T], MutAnyOrigin],
    group_idx: Int,
    group_rows: Int,
    out_row_offset: Int,
):
    comptime TBYTES = _tbytes[T]()
    comptime UT = _unsigned_of[T]()

    # Shared per-group scalars (written by thread 0, read by all after barrier).
    var sm_mode = stack_allocation[
        1, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()
    var sm_width = stack_allocation[
        1, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()
    var sm_packed_off = stack_allocation[
        1, Scalar[DType.int32], address_space = AddressSpace.SHARED
    ]()
    # Frame (FOR / CONSTANT_DELTA / DELTA_FOR), carried as the unsigned bit-pattern of T.
    var sm_frame = stack_allocation[
        1, Scalar[UT], address_space = AddressSpace.SHARED
    ]()
    # Mode-overloaded aux value (unsigned bit-pattern of T):
    #   CONSTANT       -> the constant value (broadcast to every output row)
    #   CONSTANT_DELTA -> the per-row delta  (out[i] = frame + i*delta)
    #   FOR            -> unused
    #   DELTA_FOR      -> the initial prefix-sum bias (delta_offset)
    var sm_aux = stack_allocation[
        1, Scalar[UT], address_space = AddressSpace.SHARED
    ]()

    var tid = Int(thread_idx.x)
    var nthreads = Int(block_dim.x)

    if tid == 0:
        sm_mode[0] = Int32(BPMODE_INVALID)
        sm_width[0] = 0
        sm_packed_off[0] = 0
        sm_frame[0] = 0
        sm_aux[0] = 0

        var metadata_end = Int(_load_u64(seg, 0))
        # Trailer entry K (=group_idx) at metadata_end - (K+1)*4, and the trailer
        # plus 8-byte header must lie inside the segment.
        var ok = (
            metadata_end >= 8 + (group_idx + 1) * 4
            and metadata_end <= seg_bytes
        )
        if ok:
            var entry_off = metadata_end - (group_idx + 1) * 4
            var encoded = _load_u32(seg, entry_off)
            var data_off = Int(encoded & 0x00FFFFFF)
            var parsed_mode = Int((encoded >> 24) & 0xFF)

            # Bound the unconditional 2*sizeof(T) header read.
            if data_off + 2 * TBYTES <= metadata_end:
                # v0 / v1 are read unconditionally for every header below; v2
                # (DELTA_FOR's delta_offset) is read with an extra bound check.
                var v0 = UInt64(0)
                for b in range(TBYTES):
                    v0 |= UInt64(seg[data_off + b]) << UInt64(8 * b)
                var v1 = UInt64(0)
                for b in range(TBYTES):
                    v1 |= UInt64(seg[data_off + TBYTES + b]) << UInt64(8 * b)

                if parsed_mode == BPMODE_CONSTANT:
                    # [T value]
                    sm_mode[0] = Int32(BPMODE_CONSTANT)
                    sm_aux[0] = v0.cast[UT]()
                elif parsed_mode == BPMODE_CONSTANT_DELTA:
                    # [T frame][T delta]   (no packed stream)
                    #   out[i] = frame + i*delta
                    sm_mode[0] = Int32(BPMODE_CONSTANT_DELTA)
                    sm_frame[0] = v0.cast[UT]()
                    sm_aux[0] = v1.cast[UT]()
                elif parsed_mode == BPMODE_FOR:
                    # [T frame][T width][packed...]
                    var width = Int(seg[data_off + TBYTES])  # width fits in 1 byte
                    var packed_off = data_off + 2 * TBYTES
                    var valid = width <= TBYTES * 8
                    if valid:
                        # Ensure the packed stream stays inside the segment.
                        var packed_words = ceildiv(group_rows * width, 32)
                        var packed_end = packed_off + packed_words * 4
                        if packed_end <= metadata_end:
                            sm_mode[0] = Int32(BPMODE_FOR)
                            sm_width[0] = Int32(width)
                            sm_packed_off[0] = Int32(packed_off)
                            sm_frame[0] = v0.cast[UT]()
                elif parsed_mode == BPMODE_DELTA_FOR:
                    # [T frame][T width][T delta_offset][packed...]
                    # DELTA_FOR adds a third T (delta_offset) before the packed
                    # stream -- re-bound to catch tight segments where the third
                    # read would alias the metadata trailer.
                    if data_off + 3 * TBYTES <= metadata_end:
                        var width = Int(seg[data_off + TBYTES])  # width fits in 1 byte
                        var delta_off_u = UInt64(0)
                        for b in range(TBYTES):
                            delta_off_u |= (
                                UInt64(seg[data_off + 2 * TBYTES + b])
                                << UInt64(8 * b)
                            )
                        var packed_off = data_off + 3 * TBYTES
                        var valid = width <= TBYTES * 8
                        if valid:
                            var packed_words = ceildiv(group_rows * width, 32)
                            var packed_end = packed_off + packed_words * 4
                            if packed_end <= metadata_end:
                                sm_mode[0] = Int32(BPMODE_DELTA_FOR)
                                sm_width[0] = Int32(width)
                                sm_packed_off[0] = Int32(packed_off)
                                sm_frame[0] = v0.cast[UT]()
                                sm_aux[0] = delta_off_u.cast[UT]()
                # INVALID / AUTO / unknown: leave INVALID.
    barrier()

    var mode = Int(sm_mode[0])
    var out_base = out_row_offset

    if mode == BPMODE_CONSTANT:
        var val = bitcast[T, 1](sm_aux[0])
        var i = tid
        while i < group_rows:
            dst[out_base + i] = val
            i += nthreads
        return

    if mode == BPMODE_CONSTANT_DELTA:
        # out[i] = frame + i*delta, in the unsigned (two's-complement) domain to
        # match DuckDB's CONSTANT_DELTA reconstruction:
        #   target[i] = constant * (group_offset + i) + frame_of_reference
        # group_offset is 0 at the start of each metadata group (one CTA = one
        # group), so it reduces to frame + i*delta.
        var frame_u = sm_frame[0]
        var delta_u = sm_aux[0]
        var i = tid
        while i < group_rows:
            var summed = frame_u + UInt64(i).cast[UT]() * delta_u
            dst[out_base + i] = bitcast[T, 1](summed)
            i += nthreads
        return

    if mode == BPMODE_FOR:
        # FOR: out[i] = frame + unpack_value(width, i).
        # Do the add in the UNSIGNED domain (two's-complement wrap matches DuckDB's
        # frame-of-reference decode) then reinterpret the bits to the signed T.
        var width = Int(sm_width[0])
        var packed = (seg + Int(sm_packed_off[0])).bitcast[Scalar[DType.uint32]]()
        var frame_u = sm_frame[0]  # frame as the unsigned bit-pattern of T
        var i = tid
        while i < group_rows:
            var raw = unpack_value(packed, i, width)  # UInt64 offset
            var summed = frame_u + raw.cast[UT]()
            dst[out_base + i] = bitcast[T, 1](summed)
            i += nthreads
        return

    if mode == BPMODE_DELTA_FOR:
        # DELTA_FOR: out[i] = delta_offset + sum_{j=0..i}(frame + unpack(j)),
        # i.e. an INCLUSIVE prefix-sum (within the 2048-row metadata group) of
        # the frame-of-reference-decoded deltas, seeded with delta_offset.
        #
        # This matches DuckDB's per-algorithm-group decode (bitpacking.cpp):
        #   ApplyFrameOfReference(buf, frame, n)  -> buf[k] += frame
        #   DeltaDecode(buf, prev_delta, n)       -> buf[0] += prev_delta; inclusive scan
        #   prev_delta = buf[n-1]  (carries across the 32-value algorithm groups)
        # Carried across all algorithm groups in the metadata group, the running
        # bias starts at the header's delta_offset, so it folds into a single
        # group-wide inclusive scan seeded with delta_offset. All arithmetic is
        # done in the unsigned (two's-complement) domain.
        #
        # Shared scratch holds frame-decoded deltas, then the prefix sums. A
        # single-thread serial scan over <= BP_META_GROUP_SIZE elements keeps
        # this bulletproof for arbitrary block_dim / short tails (correctness
        # over cleverness; portable, no warp/scan intrinsics).
        var sm_scan = stack_allocation[
            BP_META_GROUP_SIZE, Scalar[UT], address_space = AddressSpace.SHARED
        ]()
        var width = Int(sm_width[0])
        var packed = (seg + Int(sm_packed_off[0])).bitcast[Scalar[DType.uint32]]()
        var frame_u = sm_frame[0]

        # Stage 1 (parallel): each thread frame-decodes its strided rows.
        var i = tid
        while i < group_rows:
            var raw = unpack_value(packed, i, width)
            sm_scan[i] = frame_u + raw.cast[UT]()
            i += nthreads
        barrier()

        # Stage 2 (serial, thread 0): inclusive prefix-sum seeded with delta_offset.
        if tid == 0:
            var running = sm_aux[0]  # delta_offset bias
            for k in range(group_rows):
                running = running + sm_scan[k]
                sm_scan[k] = running
        barrier()

        # Stage 3 (parallel): write the scanned values out.
        var j = tid
        while j < group_rows:
            dst[out_base + j] = bitcast[T, 1](sm_scan[j])
            j += nthreads
        return

    # INVALID / AUTO / unknown -> deterministic zero-fill.
    var i = tid
    while i < group_rows:
        dst[out_base + i] = Scalar[T](0)
        i += nthreads
