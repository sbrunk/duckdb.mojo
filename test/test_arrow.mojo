"""Arrow interop: batched export, the exported schema, reading a batch back, the
scan of an exported stream, and handing the C structs to a consumer."""

from std.ffi import c_char
from std.memory.alloc import unsafe_alloc
from std.sys import size_of
from std.testing import assert_equal, assert_false, assert_raises, assert_true
from std.testing.suite import TestSuite
from duckdb import *
from duckdb._libduckdb_v2 import ArrowArray, ArrowArrayStream, ArrowSchema
from duckdb.api_v2 import null


@fieldwise_init
struct _AB(Copyable, Defaultable, Movable):
    """`STRUCT(a INTEGER, b VARCHAR)`."""

    var a: Int32
    var b: String

    def __init__(out self):
        self.a = 0
        self.b = ""


def _alloc[T: AnyType]() -> Pointer[T, MutUntrackedOrigin]:
    """A zeroed heap `T`, as a consumer allocates one (the library's `_zeroed` is
    private)."""
    var p = unsafe_alloc[T](1)
    var bytes = p.unsafe_bitcast[UInt8]()
    for i in range(size_of[T]()):
        bytes[unsafe_offset=i] = 0
    return p


def _cstr(p: Pointer[c_char, ImmUntrackedOrigin]) -> String:
    """A NUL-terminated C string field, `<null>` when it is NULL."""
    if Int(p) == 0:
        return "<null>"
    return String(unsafe_from_utf8_ptr=p)


def _child(p: Pointer[ArrowSchema, MutUntrackedOrigin], i: Int) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    return p[].children[unsafe_offset=i]


# Export


def _check_int_column(a: OwnedArrowArray, start: Int64) raises:
    """The flat INT64 child of `a` (buffer 0 validity, buffer 1 data) holds `start`,
    `start+1`, ..."""
    var p = a.unsafe_ptr()
    assert_equal(Int(p[].n_children), 2)
    var col = p[].children[unsafe_offset=0]
    assert_equal(Int(col[].null_count), 0)
    assert_equal(Int(col[].length), Int(p[].length))
    var data = col[].buffers[unsafe_offset=1].unsafe_bitcast[Int64]()
    for k in range(Int(p[].length)):
        assert_equal(data[unsafe_offset=k], start + Int64(k))


def test_arrow_batched_export() raises:
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = "SELECT i, 'row ' || i AS s FROM range(5000) t(i)"
    var r = src.execute_arrow(sql, batch_size=1000)
    var schema = r.schema()
    assert_equal(schema.n_children(), 2)

    var rows = 0
    var batches = 0
    var next_i = Int64(0)
    while True:
        var a = r.fetch_array()
        if not a:
            break
        var arr = a.take()
        var n = len(arr)
        assert_equal(n, 1000)
        _check_int_column(arr, next_i)
        _ = arr^
        next_i += Int64(n)
        rows += n
        batches += 1
    assert_equal(rows, 5000)
    assert_equal(batches, 5)
    assert_equal(Int(next_i), 5000)

    var want = con.execute(sql).fetchall()
    assert_equal(len(want), rows)
    assert_equal(want.column_count(), schema.n_children())
    assert_equal(want.get[Int64](col=0, row=0), Int64(0))
    assert_equal(want.get[Int64](col=0, row=4999), Int64(4999))
    assert_equal(want.get[String](col=1, row=4999), "row 4999")


def test_arrow_schema_formats() raises:
    var con = Database().connect()
    _ = con.execute("SET TimeZone='UTC'")
    _ = con.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
    var sql = (
        "SELECT 1.234::DECIMAL(11,3) AS dv, DATE '2025-03-29' AS dt,"
        " TIMESTAMP '2025-03-29 10:11:12' AS ts, TIMESTAMPTZ '2025-03-29 10:11:12' AS tstz,"
        " INTERVAL '1 month 2 days 3 hours' AS iv, 'happy'::mood AS en,"
        " {'a': 1, 'b': 'x'} AS st, [1, 2]::INTEGER[] AS l, [1, 2, 3]::INTEGER[3] AS ar,"
        " MAP {'a': 1} AS mp, uuid() AS uu, '{\"a\": 1}'::JSON AS j"
    )
    var r = con.execute_arrow(sql)
    var schema = r.schema()
    var p = schema.unsafe_ptr()
    assert_equal(_cstr(p[].format), "+s")
    assert_equal(Int(p[].n_children), 12)

    var want_names = ["dv", "dt", "ts", "tstz", "iv", "en", "st", "l", "ar", "mp", "uu", "j"]
    # DuckDB 2.0.1 defaults: arrow_output_version <= 1.4, 32-bit offsets, `u` strings.
    var want_formats = [
        "d:11,3,128",  # DECIMAL: precision, scale, bit width
        "tdD",
        "tsu:",  # TIMESTAMP (microseconds, no zone)
        "tsu:UTC",  # TIMESTAMPTZ (microseconds, then the session's zone)
        "tin",  # INTERVAL (months, days, microseconds)
        "C",  # ENUM: dictionary-encoded, values in the dictionary child
        "+s",
        "+l",
        "+w:3",  # FIXED_SIZE_LIST(3)
        "+m",
        "u",  # UUID as VARCHAR
        "u",  # JSON as VARCHAR
    ]
    for i in range(len(want_names)):
        var c = _child(p, i)
        assert_equal(_cstr(c[].name), want_names[i])
        assert_equal(_cstr(c[].format), want_formats[i])

    var en = _child(p, 5)
    assert_equal(_cstr(en[].name), "en")
    assert_equal(Int(en[].n_children), 0)
    assert_true(Int(en[].dictionary) != 0)
    assert_equal(_cstr(en[].dictionary[].format), "u")

    var st = _child(p, 6)
    assert_equal(Int(st[].n_children), 2)
    assert_equal(_cstr(_child(st, 0)[].name), "a")
    assert_equal(_cstr(_child(st, 0)[].format), "i")
    assert_equal(_cstr(_child(st, 1)[].name), "b")
    assert_equal(_cstr(_child(st, 1)[].format), "u")

    assert_equal(_cstr(_child(_child(p, 7), 0)[].name), "l")
    assert_equal(_cstr(_child(_child(p, 7), 0)[].format), "i")
    assert_equal(_cstr(_child(_child(p, 8), 0)[].format), "i")

    var entries = _child(_child(p, 9), 0)
    assert_equal(Int(_child(p, 9)[].n_children), 1)
    assert_equal(_cstr(entries[].name), "entries")
    assert_equal(_cstr(entries[].format), "+s")
    assert_equal(_cstr(_child(entries, 0)[].format), "u")
    assert_equal(_cstr(_child(entries, 1)[].format), "i")

    # UUID and JSON carry no extension metadata.
    assert_equal(_cstr(_child(p, 10)[].metadata), "<null>")
    assert_equal(_cstr(_child(p, 11)[].metadata), "<null>")

    var want = con.execute(sql).fetchall()
    assert_equal(schema.n_children(), want.column_count())
    _ = schema^


def test_arrow_export_drains_to_empty_array() raises:
    var con = Database().connect()
    var r = con.execute_arrow("SELECT 1 AS one")
    var first = r.fetch_array()
    assert_true(first.__bool__())
    assert_equal(len(first.value()), 1)
    assert_false(r.fetch_array().__bool__())


# Import (scan a stream)


def test_arrow_round_trip() raises:
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var stream = src.execute_arrow(
        "SELECT i, 'name number ' || i AS s, [i, i + 1] AS l, {'x': i} AS st, CASE WHEN i % 2 = 0 THEN i END AS n"
        " FROM range(5000) t(i)"
    ).into_stream()
    var back = con.arrow_scan(stream).fetchall()
    _ = stream^
    assert_equal(len(back), 5000)
    assert_equal(back.get[String](col=1, row=4999), "name number 4999")
    assert_equal(back.get[List[Int64]](col=2, row=3)[1], 4)
    assert_false(back.get_optional[Int64](col=4, row=1).__bool__())

    var want = con.execute(
        "SELECT i, 'name number ' || i AS s, [i, i + 1] AS l, {'x': i} AS st, CASE WHEN i % 2 = 0 THEN i END AS n"
        " FROM range(5000) t(i)"
    ).fetchall()
    assert_equal(len(want), len(back))
    assert_equal(want.get[Int64](col=0, row=42), back.get[Int64](col=0, row=42))
    assert_equal(want.get[String](col=1, row=42), back.get[String](col=1, row=42))

    var s2 = src.execute_arrow("SELECT i, i * 2 AS d FROM range(10000) t(i)").into_stream()
    var agg = con.arrow_scan(s2).filter("i % 2 = 0").aggregate("sum(d)").get[Int128]()
    _ = s2^
    assert_equal(agg[0], 49990000)

    var s3 = src.execute_arrow("SELECT 1").into_stream()
    assert_equal(len(con.arrow_scan(s3).fetchall()), 1)
    assert_equal(len(con.arrow_scan(s3).fetchall()), 0)
    _ = s3^


def test_arrow_type_round_trip() raises:
    var db = Database()
    var setup = db.connect()
    var src = db.connect()
    var con = db.connect()
    _ = setup.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
    _ = setup.execute(
        "CREATE TABLE t (id INTEGER, dv DECIMAL(11,3), dt DATE, ts TIMESTAMP, tstz TIMESTAMPTZ,"
        " iv INTERVAL, en mood, st STRUCT(a INTEGER, b VARCHAR), mp MAP(VARCHAR, INTEGER))"
    )
    _ = setup.execute(
        "INSERT INTO t VALUES"
        " (1, 1.234, DATE '2025-03-29', TIMESTAMP '2025-03-29 10:11:12',"
        " TIMESTAMPTZ '2025-03-29 10:11:12', INTERVAL '1 month 2 days 3 hours', 'happy',"
        " {'a': 7, 'b': 'seven'}, MAP {'a': 1, 'b': 2}),"
        " (2, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL),"
        " (3, -0.5, DATE '1969-12-31', TIMESTAMP '1969-12-31 23:59:59',"
        " TIMESTAMPTZ '1969-12-31 23:59:59', INTERVAL '-45 seconds', 'sad',"
        " {'a': 0, 'b': ''}, MAP {'z': 0})"
    )
    var sql = "SELECT id, dv, dt, ts, tstz, iv, en, st, mp FROM t ORDER BY id"

    var want = setup.execute(sql).fetchall()
    var stream = src.execute_arrow(sql).into_stream()
    var got = con.arrow_scan(stream).fetchall()
    _ = stream^
    assert_equal(len(got), 3)
    assert_equal(got.column_count(), want.column_count())
    for row in range(3):
        assert_equal(got.get[Int32](col=0, row=row), want.get[Int32](col=0, row=row))

    assert_equal(got.get[Decimal](col=1, row=0), Decimal(11, 3, Int128(1234)))
    assert_equal(got.get[Decimal](col=1, row=0), want.get[Decimal](col=1, row=0))
    assert_equal(String(got.get[Date](col=2, row=0)), "2025-03-29")
    assert_equal(got.get[Date](col=2, row=0), want.get[Date](col=2, row=0))
    assert_equal(got.get[Timestamp](col=3, row=0).micros, want.get[Timestamp](col=3, row=0).micros)
    assert_true(got.get[Timestamp](col=3, row=0).micros != 0)
    assert_equal(got.get[TimestampTZ](col=4, row=0).micros, want.get[TimestampTZ](col=4, row=0).micros)
    assert_equal(got.get[Interval](col=5, row=0), Interval(1, 2, 3 * 3600 * 1_000_000))
    assert_equal(got.get[Interval](col=5, row=0), want.get[Interval](col=5, row=0))
    assert_equal(got.get[String](col=6, row=0), "happy")
    assert_equal(got.get[String](col=6, row=0), want.get[String](col=6, row=0))
    var st0 = got.get[_AB](col=7, row=0)
    assert_equal(st0.a, 7)
    assert_equal(st0.b, "seven")
    assert_equal(st0.a, want.get[_AB](col=7, row=0).a)
    var mp0 = got.get[Dict[String, Int32]](col=8, row=0)
    assert_equal(len(mp0), 2)
    assert_equal(mp0["b"], 2)
    assert_equal(mp0["b"], want.get[Dict[String, Int32]](col=8, row=0)["b"])

    assert_false(got.get_optional[Decimal](col=1, row=1).__bool__())
    assert_false(got.get_optional[Date](col=2, row=1).__bool__())
    assert_false(got.get_optional[Timestamp](col=3, row=1).__bool__())
    assert_false(got.get_optional[TimestampTZ](col=4, row=1).__bool__())
    assert_false(got.get_optional[Interval](col=5, row=1).__bool__())
    assert_false(got.get_optional[String](col=6, row=1).__bool__())
    assert_false(got.get_optional[_AB](col=7, row=1).__bool__())
    assert_false(got.get_optional[Dict[String, Int32]](col=8, row=1).__bool__())
    assert_false(want.get_optional[Decimal](col=1, row=1).__bool__())

    # Values after a NULL row keep their offsets.
    assert_equal(got.get[Decimal](col=1, row=2), Decimal(11, 3, Int128(-500)))
    assert_equal(String(got.get[Date](col=2, row=2)), "1969-12-31")
    assert_equal(got.get[Timestamp](col=3, row=2).micros, want.get[Timestamp](col=3, row=2).micros)
    assert_equal(got.get[Interval](col=5, row=2).micros, Int64(-45_000_000))
    assert_equal(got.get[String](col=6, row=2), "sad")
    assert_equal(got.get[_AB](col=7, row=2).b, "")
    assert_equal(len(got.get[Dict[String, Int32]](col=8, row=2)), 1)


def test_arrow_release_to() raises:
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var r = src.execute_arrow("SELECT i FROM range(300) t(i)", batch_size=100)

    var schema = r.schema()
    var want_flags = Int(schema.unsafe_ptr()[].flags)
    var want_children = Int(schema.unsafe_ptr()[].n_children)
    var own_schema = _alloc[ArrowSchema]()
    schema.release_to(own_schema)
    assert_equal(_cstr(own_schema[].format), "+s")
    assert_equal(Int(own_schema[].n_children), 1)
    # Every field moved, including those at the struct's end.
    assert_equal(Int(own_schema[].flags), want_flags)
    assert_equal(Int(own_schema[].n_children), want_children)
    assert_true(Int(own_schema[].children) != 0)
    assert_true(schema.is_released())
    assert_equal(schema.n_children(), 0)

    var a = r.fetch_array()
    assert_true(a.__bool__())
    var arr = a.take()
    var want_buffers = Int(arr.unsafe_ptr()[].n_buffers)
    var want_array_children = Int(arr.unsafe_ptr()[].n_children)
    var want_offset = Int(arr.unsafe_ptr()[].offset)
    var own_array = _alloc[ArrowArray]()
    arr.release_to(own_array)
    assert_equal(Int(own_array[].length), 100)
    assert_equal(Int(own_array[].n_buffers), want_buffers)
    assert_equal(Int(own_array[].n_children), want_array_children)
    assert_equal(Int(own_array[].offset), want_offset)
    assert_true(Int(own_array[].buffers) != 0)
    assert_true(arr.is_released())
    _ = arr^
    _ = a^

    var stream = src.execute_arrow("SELECT i FROM range(7) t(i)").into_stream()
    var own_stream = _alloc[ArrowArrayStream]()
    stream.release_to(own_stream)
    assert_true(Pointer(to=own_stream[].get_schema).unsafe_bitcast[Int64]()[] != 0)
    assert_true(Pointer(to=own_stream[].get_next).unsafe_bitcast[Int64]()[] != 0)
    assert_true(Pointer(to=own_stream[].get_last_error).unsafe_bitcast[Int64]()[] != 0)
    assert_true(stream.is_released())

    var rows = con.arrow_scan(own_stream).fetchall()
    assert_equal(len(rows), 7)
    assert_equal(rows.get[Int64](col=0, row=6), 6)
    with assert_raises(contains="already read or released"):
        _ = con.arrow_scan(stream.unsafe_ptr()).fetchall()
    # Its destructor must leave `own_stream` alone.
    _ = stream^
    assert_equal(len(con.arrow_scan(own_stream).fetchall()), 0)

    assert_true(Pointer(to=own_stream[].release).unsafe_bitcast[Int64]()[] != 0)
    own_stream[].release(own_stream)
    assert_true(Pointer(to=own_array[].release).unsafe_bitcast[Int64]()[] != 0)
    own_array[].release(own_array)
    assert_true(Pointer(to=own_schema[].release).unsafe_bitcast[Int64]()[] != 0)
    own_schema[].release(own_schema)
    own_stream.unsafe_free()
    own_array.unsafe_free()
    own_schema.unsafe_free()


# Reading a batch


def test_arrow_read_scalar_types() raises:
    """A column per scalar type `ArrowBatch` reads, against the normal path."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = (
        "SELECT 7::TINYINT AS i8, -700::SMALLINT AS i16, 7000000::INTEGER AS i32,"
        " -7000000000000::BIGINT AS i64, 250::UTINYINT AS u8, 65000::USMALLINT AS u16,"
        " 4000000000::UINTEGER AS u32, 18000000000000000000::UBIGINT AS u64,"
        " 1.5::REAL AS f32, -2.25::DOUBLE AS f64, True AS bl,"
        " 1.234::DECIMAL(11,3) AS dv, DATE '2025-03-29' AS dt,"
        " TIMESTAMP '2025-03-29 10:11:12' AS ts, TIMESTAMPTZ '2025-03-29 10:11:12' AS tstz,"
        " INTERVAL '1 month 2 days 3 hours' AS iv, 'hello' AS s, 'blob!'::BLOB AS bb,"
        " 170141183460469231731687303715884105727::HUGEINT AS h"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(len(b), 1)
    assert_equal(b.column_count(), 19)

    assert_equal(b.column_name(0), "i8")
    assert_equal(b.column_name(16), "s")
    assert_equal(b.column_format(0), "c")
    assert_equal(b.column_format(7), "L")
    assert_equal(b.column_format(11), "d:11,3,128")
    assert_equal(b.column_format(12), "tdD")
    assert_equal(b.column_format(15), "tin")
    assert_equal(b.column_format(18), "d:38,0")

    assert_equal(b.get[Int8](col=0, row=0), Int8(7))
    assert_equal(b.get[Int8](col=0, row=0), want.get[Int8](col=0, row=0))
    assert_equal(b.get[Int16](col=1, row=0), Int16(-700))
    assert_equal(b.get[Int16](col=1, row=0), want.get[Int16](col=1, row=0))
    assert_equal(b.get[Int32](col=2, row=0), Int32(7000000))
    assert_equal(b.get[Int32](col=2, row=0), want.get[Int32](col=2, row=0))
    assert_equal(b.get[Int64](col=3, row=0), Int64(-7000000000000))
    assert_equal(b.get[Int64](col=3, row=0), want.get[Int64](col=3, row=0))
    assert_equal(b.get[UInt8](col=4, row=0), UInt8(250))
    assert_equal(b.get[UInt8](col=4, row=0), want.get[UInt8](col=4, row=0))
    assert_equal(b.get[UInt16](col=5, row=0), UInt16(65000))
    assert_equal(b.get[UInt16](col=5, row=0), want.get[UInt16](col=5, row=0))
    assert_equal(b.get[UInt32](col=6, row=0), UInt32(4000000000))
    assert_equal(b.get[UInt32](col=6, row=0), want.get[UInt32](col=6, row=0))
    assert_equal(b.get[UInt64](col=7, row=0), UInt64(18000000000000000000))
    assert_equal(b.get[UInt64](col=7, row=0), want.get[UInt64](col=7, row=0))
    assert_equal(b.get[Float32](col=8, row=0), Float32(1.5))
    assert_equal(b.get[Float32](col=8, row=0), want.get[Float32](col=8, row=0))
    assert_equal(b.get[Float64](col=9, row=0), Float64(-2.25))
    assert_equal(b.get[Float64](col=9, row=0), want.get[Float64](col=9, row=0))
    assert_true(b.get[Bool](col=10, row=0))
    assert_equal(b.get[Decimal](col=11, row=0), Decimal(11, 3, Int128(1234)))
    assert_equal(b.get[Decimal](col=11, row=0), want.get[Decimal](col=11, row=0))
    assert_equal(String(b.get[Date](col=12, row=0)), "2025-03-29")
    assert_equal(b.get[Date](col=12, row=0), want.get[Date](col=12, row=0))
    assert_true(b.get[Timestamp](col=13, row=0).micros != 0)
    assert_equal(b.get[Timestamp](col=13, row=0).micros, want.get[Timestamp](col=13, row=0).micros)
    assert_equal(b.get[TimestampTZ](col=14, row=0).micros, want.get[TimestampTZ](col=14, row=0).micros)
    assert_equal(b.get[Interval](col=15, row=0), Interval(1, 2, 3 * 3600 * 1_000_000))
    assert_equal(b.get[Interval](col=15, row=0), want.get[Interval](col=15, row=0))
    assert_equal(b.get[String](col=16, row=0), "hello")
    assert_equal(b.get[String](col=16, row=0), want.get[String](col=16, row=0))
    assert_equal(b.get[Blob](col=17, row=0), Blob([UInt8(98), 108, 111, 98, 33]))
    assert_equal(b.get[Blob](col=17, row=0), want.get[Blob](col=17, row=0))
    assert_equal(b.get[Int128](col=18, row=0), Int128(170141183460469231731687303715884105727))
    assert_equal(b.get[Int128](col=18, row=0), want.get[Int128](col=18, row=0))


def test_arrow_read_nulls() raises:
    """NULLs: get_optional, to_list, is_valid, has_nulls. Indexing a NULL row raises."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = "SELECT CASE WHEN i % 3 = 0 THEN NULL ELSE i END AS n, i AS k FROM range(7) t(i)"
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(len(b), 7)

    var c = b.column[Int64](0)
    assert_true(c.has_nulls())
    assert_false(b.column[Int64](1).has_nulls())
    assert_false(c.is_valid(0))
    assert_true(c.is_valid(1))
    assert_true(c.is_valid(2))
    for row in range(7):
        assert_equal(c.is_valid(row), want.get_optional[Int64](col=0, row=row).__bool__())
        var w = want.get_optional[Int64](col=0, row=row)
        var v = c.get(row)
        assert_equal(v.__bool__(), w.__bool__())
        assert_equal(b.get_optional[Int64](col=0, row=row).__bool__(), w.__bool__())
        if v.__bool__():
            assert_equal(v.value(), w.value())
            assert_equal(b.get[Int64](col=0, row=row), w.value())
        else:
            assert_false(b.get_optional[Int64](col=0, row=row).__bool__())
            with assert_raises(contains="is NULL"):
                _ = c[row]

    var row = 0
    for var v in c.to_list():
        if row % 3 == 0:
            assert_false(v.__bool__())
        else:
            assert_equal(v.value(), Int64(row))
        row += 1
    assert_equal(row, 7)


def test_arrow_read_enum() raises:
    """A DuckDB ENUM is a dictionary column. It reads as String, NULLs included."""
    var db = Database()
    var setup = db.connect()
    var src = db.connect()
    _ = setup.execute("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')")
    var sql = "SELECT CASE i WHEN 0 THEN 'sad'::mood WHEN 2 THEN 'happy'::mood END AS en FROM range(3) t(i)"
    var want = setup.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "C")

    var c = b.column[String](0)
    assert_equal(c[0], "sad")
    assert_equal(c[2], "happy")
    assert_equal(c[0], want.get[String](col=0, row=0))
    assert_equal(c[2], want.get[String](col=0, row=2))
    assert_true(c.has_nulls())
    assert_false(c.is_valid(1))
    assert_false(c.get(1).__bool__())
    assert_false(b.get_optional[String](col=0, row=1).__bool__())
    assert_true(want.get_optional[String](col=0, row=1).__bool__() == False)
    with assert_raises(contains="is NULL"):
        _ = c[1]


def test_arrow_read_lists() raises:
    """Lists, a fixed-size list, a NULL list and a list with a NULL element."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = (
        "SELECT [i, i + 1, i + 2] AS l, [['a', 'b'], ['c']] AS ll,"
        " [10, 20, 30]::INTEGER[3] AS ar, CASE WHEN i = 0 THEN NULL ELSE [i] END AS nl,"
        " [1, NULL, 3]::BIGINT[] AS ne FROM range(2) t(i)"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "+l")
    assert_equal(b.column_format(3), "+l")
    assert_equal(b.column_format(2), "+w:3")

    var l = b.column[List[Int64]](0)
    for row in range(2):
        var got = l[row]
        var exp = want.get[List[Int64]](col=0, row=row)
        assert_equal(len(got), 3)
        assert_equal(len(got), len(exp))
        for k in range(3):
            assert_equal(got[k], Int64(row) + Int64(k))
            assert_equal(got[k], exp[k])

    var ll = b.column[List[List[String]]](1)
    var want_ll: List[List[String]] = [["a", "b"], ["c"]]
    for row in range(2):
        var got = ll[row]
        assert_equal(len(got), 2)
        assert_equal(got, want_ll)
        var exp = want.get[List[List[String]]](col=1, row=row)
        assert_equal(got, exp)
        assert_equal(got[0][1], "b")
        assert_equal(got[1][0], exp[1][0])

    var ar = b.column[List[Int32]](2)
    var want_ar: List[Int32] = [10, 20, 30]
    assert_equal(ar[0], want_ar)
    assert_equal(ar[1], want.get[List[Int32]](col=2, row=1))

    var nl = b.column[List[Int64]](3)
    assert_true(nl.has_nulls())
    assert_false(nl.is_valid(0))
    assert_false(nl.get(0).__bool__())
    assert_false(b.get_optional[List[Int64]](col=3, row=0).__bool__())
    var one_opt = nl.get(1)
    assert_true(one_opt.__bool__())
    var one = one_opt.take()
    assert_equal(len(one), 1)
    assert_equal(one[0], Int64(1))
    assert_equal(one[0], want.get[List[Int64]](col=3, row=1)[0])

    var ne = b.column[List[Int64]](4)
    assert_true(ne.is_valid(0))
    with assert_raises(contains="NULL value"):
        _ = ne[0]
    with assert_raises(contains="NULL value"):
        _ = b.get[List[Int64]](col=4, row=1)


def test_arrow_read_nested_columns_raise() raises:
    """STRUCT and MAP columns: the error names arrow_scan."""
    var db = Database()
    var src = db.connect()
    var r = src.execute_arrow("SELECT {'a': 1, 'b': 'x'} AS st, MAP {'a': 1} AS mp FROM range(2) t(i)")
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "+s")
    assert_equal(b.column_format(1), "+m")
    with assert_raises(contains="arrow_scan"):
        _ = b.column[_AB](0)[0]
    with assert_raises(contains="arrow_scan"):
        _ = b.get[Dict[String, Int32]](col=1, row=0)
    with assert_raises(contains="arrow_scan"):
        _ = b.column[Int64](0)[0]
    with assert_raises(contains="arrow_scan"):
        _ = b.column[Int64](1)[0]


def test_arrow_read_type_mismatch() raises:
    """`column[T]` names the format and the type when `T` does not match the column."""
    var db = Database()
    var src = db.connect()
    var r = src.execute_arrow("SELECT 42::BIGINT AS i, 'x' AS s FROM range(2) t(i)")
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    with assert_raises(contains="cannot be read as"):
        _ = b.column[String](0)[0]
    with assert_raises(contains="cannot be read as"):
        _ = b.get[List[Int64]](col=0, row=0)
    with assert_raises(contains="cannot be read as"):
        _ = b.column[Int64](1)[0]
    assert_equal(b.column[Int64](0)[0], Int64(42))
    assert_equal(b.column[String](1)[0], "x")


def test_arrow_read_batches() raises:
    """Batches of 1000 rows over 5000 rows, each read before the next is fetched."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = "SELECT i, CASE WHEN i % 7 = 0 THEN NULL ELSE i END AS n FROM range(5000) t(i)"
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql, batch_size=1000)

    var batches = 0
    var first = Int64(0)
    while True:
        var maybe = r.fetch_batch()
        if not maybe:
            break
        var b = maybe.take()
        assert_equal(len(b), 1000)
        var ids = b.column[Int64](0)
        var nulls = b.column[Int64](1)
        for row in range(1000):
            var g = first + Int64(row)
            assert_equal(ids[row], g)
            assert_true(ids.is_valid(row))
            var v = nulls.get(row)
            var w = want.get_optional[Int64](col=1, row=Int(g))
            assert_equal(v.__bool__(), w.__bool__())
            if v.__bool__():
                assert_equal(v.value(), g)
        if batches == 3:
            assert_equal(ids[0], Int64(3000))
            assert_true(nulls.is_valid(0))
            assert_equal(nulls[7], Int64(3007))
            assert_false(nulls.get(3).__bool__())  # global row 3003, the batch's first NULL

        # span(): only the NULL-free column keeps Int64's own layout.
        var s = ids.span()
        assert_true(s.__bool__())
        var values = s.value()
        assert_equal(len(values), 1000)
        for row in range(len(values)):
            assert_equal(values[row], first + Int64(row))
        assert_false(nulls.span().__bool__())

        batches += 1
        first += Int64(1000)
    assert_equal(batches, 5)
    assert_false(r.fetch_batch().__bool__())


def test_arrow_read_string_views() raises:
    """Arrow 1.4 exports strings and blobs as 16-byte views: inline up to 12 bytes,
    longer ones in a data buffer. DuckDB 2.0.1 uses `vz` for a BLOB at version 1.4
    and needs `produce_arrow_string_view` for `vu`."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    _ = src.execute("SET arrow_output_version='1.4'")
    _ = src.execute("SET produce_arrow_string_view=true")
    var sql = (
        "SELECT CASE i WHEN 0 THEN 'a blob longer than twelve bytes'::BLOB"
        " WHEN 1 THEN CAST(NULL AS BLOB) ELSE 'tiny'::BLOB END AS bb,"
        " CASE i WHEN 0 THEN 'a varchar longer than twelve bytes'"
        " WHEN 1 THEN CAST(NULL AS VARCHAR) ELSE 'short' END AS sv,"
        " CASE i WHEN 0 THEN 'exactly12345' WHEN 1 THEN 'exactly123456' END AS ed"
        " FROM range(3) t(i)"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "vz")
    assert_equal(b.column_format(1), "vu")

    var long_blob = b.get[Blob](col=0, row=0)
    assert_equal(len(long_blob), 31)
    assert_equal(long_blob, want.get[Blob](col=0, row=0))
    assert_equal(b.get[Blob](col=0, row=2), Blob([UInt8(116), 105, 110, 121]))  # 'tiny', inline
    assert_equal(b.get[Blob](col=0, row=2), want.get[Blob](col=0, row=2))

    assert_equal(b.get[String](col=1, row=0), "a varchar longer than twelve bytes")
    assert_equal(b.get[String](col=1, row=0), want.get[String](col=1, row=0))
    assert_equal(b.get[String](col=1, row=2), "short")
    assert_equal(b.get[String](col=1, row=2), want.get[String](col=1, row=2))

    # The view boundary: 12 bytes stay inline, 13 use the data buffer.
    assert_equal(b.get[String](col=2, row=0), "exactly12345")
    assert_equal(b.get[String](col=2, row=0).byte_length(), 12)
    assert_equal(b.get[String](col=2, row=1), "exactly123456")
    assert_equal(b.get[String](col=2, row=1).byte_length(), 13)
    assert_equal(b.get[String](col=2, row=1), want.get[String](col=2, row=1))

    assert_false(b.column[String](1).is_valid(1))
    assert_false(b.get_optional[String](col=1, row=1).__bool__())
    assert_false(b.get_optional[Blob](col=0, row=1).__bool__())
    with assert_raises(contains="is NULL"):
        _ = b.get[String](col=1, row=1)


def test_arrow_read_bool_column() raises:
    """A `b` column stores its values in a bitmap, read at the byte and bit the row
    number gives."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = (
        "SELECT (i % 2 = 0) AS bl, CASE WHEN i = 4 THEN NULL ELSE (i % 3 = 0) END AS nb"
        " FROM range(9) t(i)"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "b")
    assert_equal(b.column_format(1), "b")

    var bl = b.column[Bool](0)
    var expected: List[Bool] = [True, False, True, False, True, False, True, False, True]
    for row in range(9):
        assert_equal(bl[row], expected[row])
        assert_equal(bl[row], want.get[Bool](col=0, row=row))
        assert_true(bl.is_valid(row))
    assert_false(bl.has_nulls())

    var nb = b.column[Bool](1)
    assert_true(nb.has_nulls())
    for row in range(9):
        var w = want.get_optional[Bool](col=1, row=row)
        assert_equal(nb.is_valid(row), w.__bool__())
        assert_equal(b.get_optional[Bool](col=1, row=row).__bool__(), w.__bool__())
        if row == 4:
            assert_false(nb.get(4).__bool__())
            with assert_raises(contains="is NULL"):
                _ = nb[4]
        else:
            var v = nb.get(row)
            assert_true(v.__bool__())
            assert_equal(v.take(), w.value())
            assert_equal(b.get[Bool](col=1, row=row), w.value())


def test_arrow_read_list_element_type() raises:
    """`column[List[T]]` checks the element type as well as the list itself."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = "SELECT [i, i + 1]::INTEGER[] AS li, [CONCAT('v', i)]::VARCHAR[] AS ls FROM range(2) t(i)"
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()

    with assert_raises(contains="cannot be read as"):
        _ = b.column[List[String]](0)[0]
    with assert_raises(contains="cannot be read as"):
        _ = b.get[List[Int64]](col=1, row=0)

    var li = b.column[List[Int32]](0)
    var want_li: List[Int32] = [0, 1]
    assert_equal(li[0], want_li)
    assert_equal(li[1], want.get[List[Int32]](col=0, row=1))
    var ls = b.column[List[String]](1)
    assert_equal(ls[0][0], "v0")
    assert_equal(ls[1][0], want.get[List[String]](col=1, row=1)[0])


def test_arrow_read_row_dependent_lists() raises:
    """A fixed-size list's per-row stride and a list's offsets. The values change
    every row, so an off-by-one shows up in them."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = (
        "SELECT [i, i + 1, i + 2]::INTEGER[3] AS ar, [[CONCAT('a', i), 'b'], ['c']] AS ll"
        " FROM range(2) t(i)"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_format(0), "+w:3")
    assert_equal(b.column_format(1), "+l")

    var ar = b.column[List[Int32]](0)
    assert_equal(len(ar[0]), 3)
    assert_equal(ar[1][0], Int32(1))
    assert_equal(ar[1][1], Int32(2))
    assert_equal(ar[1][2], Int32(3))
    for row in range(2):
        var got = ar[row]
        var exp = want.get[List[Int32]](col=0, row=row)
        assert_equal(len(got), len(exp))
        for k in range(3):
            assert_equal(got[k], exp[k])

    var ll = b.column[List[List[String]]](1)
    assert_equal(ll[1][0][0], "a1")
    assert_equal(ll[0][0][0], "a0")
    assert_equal(ll[1][0][1], "b")
    assert_equal(ll[1][1][0], "c")
    assert_equal(len(ll[0]), 2)
    assert_equal(len(ll[1][1]), 1)
    assert_equal(ll[1][0][0], want.get[List[List[String]]](col=1, row=1)[0][0])


def test_arrow_read_column_range() raises:
    """`column_name` and `column_format` check the column index."""
    var db = Database()
    var src = db.connect()
    var r = src.execute_arrow("SELECT 1::BIGINT AS a, 2::BIGINT AS b")
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()
    assert_equal(b.column_name(0), "a")
    assert_equal(b.column_name(1), "b")
    with assert_raises(contains="out of range"):
        _ = b.column_name(99)
    with assert_raises(contains="out of range"):
        _ = b.column_format(-1)
    with assert_raises(contains="out of range"):
        _ = b.column[Int64](2)[0]
    with assert_raises(contains="out of range"):
        _ = b.get[Int64](col=0, row=2)


def test_arrow_read_two_batches() raises:
    """A batch and its readers stay alive while the next batch is fetched. Each
    batch owns its array, so a read sees its own buffers."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = "SELECT i, CONCAT('row', i) AS s FROM range(3000) t(i)"
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql, batch_size=1000)

    var m1 = r.fetch_batch()
    assert_true(m1.__bool__())
    var b1 = m1.take()
    var ids1 = b1.column[Int64](0)
    var names1 = b1.column[String](1)

    var m2 = r.fetch_batch()
    assert_true(m2.__bool__())
    var b2 = m2.take()
    var ids2 = b2.column[Int64](0)

    assert_equal(len(b1), 1000)
    assert_equal(len(b2), 1000)
    assert_equal(ids1[0], Int64(0))
    assert_equal(ids2[0], Int64(1000))
    assert_equal(ids1[999], Int64(999))
    assert_equal(ids2[999], Int64(1999))
    assert_equal(names1[0], "row0")
    assert_equal(b2.get[String](col=1, row=0), "row1000")
    assert_equal(ids1[0], want.get[Int64](col=0, row=0))
    assert_equal(ids2[0], want.get[Int64](col=0, row=1000))
    assert_equal(names1[999], want.get[String](col=1, row=999))
    assert_equal(b2.get[String](col=1, row=0), want.get[String](col=1, row=1000))

    var m3 = r.fetch_batch()
    assert_true(m3.__bool__())
    var b3 = m3.take()
    assert_equal(b3.get[Int64](col=0, row=0), Int64(2000))
    assert_equal(ids1[0], Int64(0))
    assert_equal(ids2[0], Int64(1000))
    assert_false(r.fetch_batch().__bool__())


def test_arrow_read_whole_column() raises:
    """`get`/`get_optional` with only a column index give the whole column, as
    `Chunk` and `MaterializedResult` do."""
    var db = Database()
    var src = db.connect()
    var con = db.connect()
    var sql = (
        "SELECT i, CASE WHEN i % 2 = 0 THEN NULL ELSE i END AS n, CONCAT('s', i) AS s,"
        " [i, i + 1]::INTEGER[] AS l FROM range(5) t(i)"
    )
    var want = con.execute(sql).fetchall()
    var r = src.execute_arrow(sql)
    var maybe = r.fetch_batch()
    assert_true(maybe.__bool__())
    var b = maybe.take()

    var ids = b.get[Int64](col=0)
    assert_equal(len(ids), 5)
    for row in range(5):
        assert_equal(ids[row], want.get[Int64](col=0, row=row))

    var names = b.get[String](col=2)
    assert_equal(names[4], "s4")
    assert_equal(names, want.get[String](col=2))

    var nulls = b.get_optional[Int64](col=1)
    assert_equal(len(nulls), 5)
    for row in range(5):
        var w = want.get_optional[Int64](col=1, row=row)
        assert_equal(nulls[row].__bool__(), w.__bool__())
        if w.__bool__():
            assert_equal(nulls[row].value(), w.value())
    with assert_raises(contains="is NULL"):
        _ = b.get[Int64](col=1)

    var lists = b.get[List[Int32]](col=3)
    assert_equal(len(lists), 5)
    assert_equal(len(lists[4]), 2)
    assert_equal(lists[4][1], Int32(5))
    assert_equal(lists[4], want.get[List[Int32]](col=3, row=4))


# Hand-built batches: formats DuckDB 2.0.1 does not export


def _immp[T: AnyType](p: Pointer[T, MutUntrackedOrigin]) -> Pointer[T, ImmUntrackedOrigin]:
    """The same address, as a C struct field stores it."""
    return Pointer[T, ImmUntrackedOrigin](unsafe_from_address=Int(p))


def _mutp[T: AnyType](p: Pointer[T, ImmUntrackedOrigin]) -> Pointer[T, MutUntrackedOrigin]:
    """The same address, as memory `_release_array` and `_release_schema` free."""
    return Pointer[T, MutUntrackedOrigin](unsafe_from_address=Int(p))


def _move_into[T: AnyType](src: Pointer[T, MutUntrackedOrigin], dst: Pointer[T, MutUntrackedOrigin]):
    """The Arrow spec's move, duplicated from `arrow.mojo`'s private `_move_into`."""
    var s = src.unsafe_bitcast[UInt8]()
    var d = dst.unsafe_bitcast[UInt8]()
    for i in range(size_of[T]()):
        d[unsafe_offset=i] = s[unsafe_offset=i]
        s[unsafe_offset=i] = 0


def _bytes(s: String) -> Pointer[NoneType, ImmUntrackedOrigin]:
    """`s` as a string column's data buffer, plus a NUL."""
    return _cbytes(s).unsafe_bitcast[NoneType]()


def _cbytes(s: String) -> Pointer[c_char, ImmUntrackedOrigin]:
    """A heap NUL-terminated copy of `s` (a schema `format` or `name`)."""
    var p = unsafe_alloc[c_char](s.byte_length() + 1)
    var bytes = p.unsafe_bitcast[UInt8]()
    for i, ch in enumerate(s.bytes()):
        bytes[unsafe_offset=i] = ch
    bytes[unsafe_offset=s.byte_length()] = 0
    return _immp(p)


def _fill[T: ImplicitlyCopyable & Deinitable](vals: List[T]) -> Pointer[NoneType, ImmUntrackedOrigin]:
    """An Arrow data buffer holding `vals`."""
    var p = unsafe_alloc[T](len(vals))
    for i in range(len(vals)):
        p[unsafe_offset=i] = vals[i]
    return _immp(p.unsafe_bitcast[NoneType]())


def _no_validity() -> Pointer[NoneType, ImmUntrackedOrigin]:
    return null[Pointer[NoneType, ImmUntrackedOrigin]]()


def _no_children() -> List[Pointer[ArrowArray, MutUntrackedOrigin]]:
    return List[Pointer[ArrowArray, MutUntrackedOrigin]]()


def _no_dict() -> Pointer[ArrowArray, MutUntrackedOrigin]:
    return null[Pointer[ArrowArray, MutUntrackedOrigin]]()


def _no_schema_children() -> List[Pointer[ArrowSchema, MutUntrackedOrigin]]:
    return List[Pointer[ArrowSchema, MutUntrackedOrigin]]()


def _no_schema_dict() -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    return null[Pointer[ArrowSchema, MutUntrackedOrigin]]()


def _release_array(p: Pointer[ArrowArray, MutUntrackedOrigin]) abi("C"):
    """Frees a hand-built array: its buffers, then its children and dictionary (the
    C interface lets the consumer release those, keeping the harness leak-free).
    Zeroing `release` leaves the empty struct the spec asks for, and the root's
    `OwnedArrowArray` frees the root struct."""
    if Pointer(to=p[].release).unsafe_bitcast[Int64]()[] == 0:
        return
    ref a = p[]
    var kids = a.children
    for i in range(Int(a.n_children)):
        var kid = kids[unsafe_offset=i]
        if Int(kid) != 0:
            _release_array(kid)
            kid.unsafe_free()
    if Int(a.children) != 0:
        a.children.unsafe_free()
    if Int(a.dictionary) != 0:
        var dict = a.dictionary
        _release_array(dict)
        dict.unsafe_free()
    var bufs = a.buffers
    for i in range(Int(a.n_buffers)):
        var buf = _mutp(bufs[unsafe_offset=i])
        if Int(buf) != 0:
            buf.unsafe_free()
    if Int(a.buffers) != 0:
        a.buffers.unsafe_free()
    Pointer(to=a.release).unsafe_bitcast[Int64]()[unsafe_offset=0] = 0


def _release_schema(p: Pointer[ArrowSchema, MutUntrackedOrigin]) abi("C"):
    """Frees a hand-built schema: its strings, children and dictionary."""
    if Pointer(to=p[].release).unsafe_bitcast[Int64]()[] == 0:
        return
    ref s = p[]
    if Int(s.format) != 0:
        _mutp(s.format).unsafe_free()
    if Int(s.name) != 0:
        _mutp(s.name).unsafe_free()
    if Int(s.metadata) != 0:
        _mutp(s.metadata).unsafe_free()
    var kids = s.children
    for i in range(Int(s.n_children)):
        var kid = kids[unsafe_offset=i]
        if Int(kid) != 0:
            _release_schema(kid)
            kid.unsafe_free()
    if Int(s.children) != 0:
        s.children.unsafe_free()
    if Int(s.dictionary) != 0:
        var dict = s.dictionary
        _release_schema(dict)
        dict.unsafe_free()
    Pointer(to=s.release).unsafe_bitcast[Int64]()[unsafe_offset=0] = 0


def _array_node(
    length: Int,
    offset: Int,
    buffers: List[Pointer[NoneType, ImmUntrackedOrigin]],
    children: List[Pointer[ArrowArray, MutUntrackedOrigin]],
    dictionary: Pointer[ArrowArray, MutUntrackedOrigin],
) -> Pointer[ArrowArray, MutUntrackedOrigin]:
    """A hand-built `ArrowArray`, freed by `_release_array`."""
    var a = _alloc[ArrowArray]()
    a[].length = Int64(length)
    a[].offset = Int64(offset)
    a[].null_count = 0
    a[].n_buffers = Int64(len(buffers))
    if len(buffers):
        var bufs = unsafe_alloc[Pointer[NoneType, ImmUntrackedOrigin]](len(buffers))
        for i in range(len(buffers)):
            bufs[unsafe_offset=i] = buffers[i]
        a[].buffers = bufs
    a[].n_children = Int64(len(children))
    if len(children):
        var kids = unsafe_alloc[Pointer[ArrowArray, MutUntrackedOrigin]](len(children))
        for i in range(len(children)):
            kids[unsafe_offset=i] = children[i]
        a[].children = kids
    if Int(dictionary) != 0:
        a[].dictionary = dictionary
    a[].release = _release_array
    return a


def _array_leaf(
    length: Int, offset: Int, buffers: List[Pointer[NoneType, ImmUntrackedOrigin]]
) -> Pointer[ArrowArray, MutUntrackedOrigin]:
    return _array_node(length, offset, buffers, _no_children(), _no_dict())


def _array_child(
    length: Int,
    offset: Int,
    buffers: List[Pointer[NoneType, ImmUntrackedOrigin]],
    child: Pointer[ArrowArray, MutUntrackedOrigin],
) -> Pointer[ArrowArray, MutUntrackedOrigin]:
    var kids = List[Pointer[ArrowArray, MutUntrackedOrigin]]()
    kids.append(child)
    return _array_node(length, offset, buffers, kids, _no_dict())


def _array_dict(
    length: Int,
    offset: Int,
    buffers: List[Pointer[NoneType, ImmUntrackedOrigin]],
    values: Pointer[ArrowArray, MutUntrackedOrigin],
) -> Pointer[ArrowArray, MutUntrackedOrigin]:
    return _array_node(length, offset, buffers, _no_children(), values)


def _schema_node(
    format: String,
    name: String,
    children: List[Pointer[ArrowSchema, MutUntrackedOrigin]],
    dictionary: Pointer[ArrowSchema, MutUntrackedOrigin],
) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    """A hand-built `ArrowSchema`, freed by `_release_schema`."""
    var s = _alloc[ArrowSchema]()
    s[].format = _cbytes(format)
    if name != "":
        s[].name = _cbytes(name)
    s[].n_children = Int64(len(children))
    if len(children):
        var kids = unsafe_alloc[Pointer[ArrowSchema, MutUntrackedOrigin]](len(children))
        for i in range(len(children)):
            kids[unsafe_offset=i] = children[i]
        s[].children = kids
    if Int(dictionary) != 0:
        s[].dictionary = dictionary
    s[].release = _release_schema
    return s


def _schema_leaf(format: String, name: String) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    return _schema_node(format, name, _no_schema_children(), _no_schema_dict())


def _schema_child(
    format: String, name: String, child: Pointer[ArrowSchema, MutUntrackedOrigin]
) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    var kids = List[Pointer[ArrowSchema, MutUntrackedOrigin]]()
    kids.append(child)
    return _schema_node(format, name, kids, _no_schema_dict())


def _schema_dict(
    format: String, name: String, values: Pointer[ArrowSchema, MutUntrackedOrigin]
) -> Pointer[ArrowSchema, MutUntrackedOrigin]:
    return _schema_node(format, name, _no_schema_children(), values)


def _own_array(var a: OwnedArrowArray, src: Pointer[ArrowArray, MutUntrackedOrigin]) -> OwnedArrowArray:
    """Moves a hand-built array into an `OwnedArrowArray` and frees `src`."""
    _move_into(src, a.unsafe_ptr())
    src.unsafe_free()
    return a^


def _own_schema(var s: OwnedArrowSchema, src: Pointer[ArrowSchema, MutUntrackedOrigin]) -> OwnedArrowSchema:
    """Moves a hand-built schema into an `OwnedArrowSchema` and frees `src`."""
    _move_into(src, s.unsafe_ptr())
    src.unsafe_free()
    return s^


def test_arrow_batch_guards() raises:
    """`ArrowBatch` checks the schema and the array before it plans them."""
    var flat_array = _own_array(OwnedArrowArray(), _array_leaf(3, 0, [_no_validity(), _fill[Int32]([1, 2, 3])]))
    var flat_schema = _own_schema(OwnedArrowSchema(), _schema_leaf("i", "x"))
    with assert_raises(contains="needs a struct array"):
        _ = ArrowBatch(flat_array^, flat_schema^)

    var sliced_array = _own_array(OwnedArrowArray(), _array_leaf(3, 2, [_no_validity()]))
    var sliced_schema = _own_schema(OwnedArrowSchema(), _schema_leaf("+s", ""))
    with assert_raises(contains="cannot be sliced"):
        _ = ArrowBatch(sliced_array^, sliced_schema^)

    var short_array = _own_array(
        OwnedArrowArray(),
        _array_child(5, 0, [_no_validity()], _array_leaf(3, 0, [_no_validity(), _fill[Int32]([1, 2, 3])])),
    )
    var short_schema = _own_schema(OwnedArrowSchema(), _schema_child("+s", "", _schema_leaf("i", "c")))
    with assert_raises(contains="rows, the batch has"):
        _ = ArrowBatch(short_array^, short_schema^)

    var mixed_array = _own_array(OwnedArrowArray(), _array_leaf(2, 0, [_no_validity()]))
    var mixed_schema = _own_schema(OwnedArrowSchema(), _schema_child("+s", "", _schema_leaf("i", "c")))
    with assert_raises(contains="children, its schema"):
        _ = ArrowBatch(mixed_array^, mixed_schema^)

    var ok_array = _own_array(
        OwnedArrowArray(),
        _array_child(2, 0, [_no_validity()], _array_leaf(2, 0, [_no_validity(), _fill[Int64]([7, 8])])),
    )
    var ok_schema = _own_schema(OwnedArrowSchema(), _schema_child("+s", "", _schema_leaf("l", "c")))
    var b = ArrowBatch(ok_array^, ok_schema^)
    assert_equal(len(b), 2)
    assert_equal(b.column_count(), 1)
    assert_equal(b.column_name(0), "c")
    assert_equal(b.column_format(0), "l")
    assert_equal(b.get[Int64](col=0, row=1), Int64(8))


def test_arrow_read_synthetic_formats() raises:
    """DuckDB 2.0.1 does not export these formats, so the batch is built by hand:
    time32, date64, a large list, `w:N`, 32- and 64-bit decimals, dictionary
    indices of 16 and 64 bits, and a child array with a non-zero offset."""
    var rows = 3

    # 0 `tts` (time32 seconds), 1 `ttm` (time32 milliseconds), 2 `tdm` (date64 ms).
    var tts = _array_leaf(rows, 0, [_no_validity(), _fill[Int32]([0, 3661, 86399])])
    var ttm = _array_leaf(rows, 0, [_no_validity(), _fill[Int32]([0, 1, 3600000])])
    var tdm = _array_leaf(rows, 0, [_no_validity(), _fill[Int64]([0, 86400000, -86400000])])

    # 3 `+L`: a large list, its offsets 64 bits wide.
    var large_child = _array_leaf(5, 0, [_no_validity(), _fill[Int32]([10, 11, 20, 21, 22])])
    var large_list = _array_child(rows, 0, [_no_validity(), _fill[Int64]([0, 2, 2, 5])], large_child)

    # 4 `w:4`: 4 bytes per value.
    var fixed = _array_leaf(rows, 0, [_no_validity(), _fill[UInt8]([97, 98, 99, 100, 120, 121, 122, 48, 0, 0, 0, 0])])

    # 5 `d:9,2,32` and 6 `d:18,4,64`: decimals narrower than DuckDB's 128 bits.
    var dec32 = _array_leaf(rows, 0, [_no_validity(), _fill[Int32]([1234, -1234, 0])])
    var dec64 = _array_leaf(rows, 0, [_no_validity(), _fill[Int64]([123456, -987654321, 0])])

    # 7 a 16-bit index over `u` values, 8 a 64-bit index over `z` values.
    var dict16_values = _array_leaf(2, 0, [_no_validity(), _fill[Int32]([0, 4, 7]), _bytes("zeroone")])
    var dict16 = _array_dict(rows, 0, [_no_validity(), _fill[UInt16]([1, 0, 1])], dict16_values)
    var dict64_values = _array_leaf(3, 0, [_no_validity(), _fill[Int32]([0, 2, 5, 9]), _bytes("aabbbcccc")])
    var dict64 = _array_dict(rows, 0, [_no_validity(), _fill[UInt64]([0, 2, 1])], dict64_values)

    # 9 `+l` whose child is sliced: its logical indices start at physical 2.
    var sliced_child = _array_leaf(3, 2, [_no_validity(), _fill[Int32]([97, 98, 10, 11, 12])])
    var sliced_list = _array_child(rows, 0, [_no_validity(), _fill[Int32]([0, 3, 3, 3])], sliced_child)

    var columns: List[Pointer[ArrowArray, MutUntrackedOrigin]] = [
        tts, ttm, tdm, large_list, fixed, dec32, dec64, dict16, dict64, sliced_list,
    ]
    var array = _own_array(OwnedArrowArray(), _array_node(rows, 0, [_no_validity()], columns, _no_dict()))
    var fields: List[Pointer[ArrowSchema, MutUntrackedOrigin]] = [
        _schema_leaf("tts", "tts"),
        _schema_leaf("ttm", "ttm"),
        _schema_leaf("tdm", "tdm"),
        _schema_child("+L", "big", _schema_leaf("i", "big")),
        _schema_leaf("w:4", "fixed"),
        _schema_leaf("d:9,2,32", "d32"),
        _schema_leaf("d:18,4,64", "d64"),
        _schema_dict("S", "e16", _schema_leaf("u", "e16")),
        _schema_dict("L", "e64", _schema_leaf("z", "e64")),
        _schema_child("+l", "cut", _schema_leaf("i", "cut")),
    ]
    var schema = _own_schema(OwnedArrowSchema(), _schema_node("+s", "", fields, _no_schema_dict()))

    var b = ArrowBatch(array^, schema^)
    assert_equal(len(b), 3)
    assert_equal(b.column_count(), 10)
    var want_formats: List[String] = [
        "tts", "ttm", "tdm", "+L", "w:4", "d:9,2,32", "d:18,4,64", "S", "L", "+l",
    ]
    for i in range(10):
        assert_equal(b.column_format(i), want_formats[i])

    # time32 stores seconds and milliseconds, and `Time` holds microseconds.
    assert_equal(String(b.get[Time](col=0, row=0)), "00:00:00")
    assert_equal(String(b.get[Time](col=0, row=1)), "01:01:01")
    assert_equal(b.get[Time](col=0, row=2).micros, Int64(86399000000))
    assert_equal(b.get[Time](col=1, row=1).micros, Int64(1000))
    assert_equal(b.get[Time](col=1, row=2).micros, Int64(3600000000))

    # date64: milliseconds, including the day before the epoch.
    assert_equal(String(b.get[Date](col=2, row=0)), "1970-01-01")
    assert_equal(String(b.get[Date](col=2, row=1)), "1970-01-02")
    assert_equal(String(b.get[Date](col=2, row=2)), "1969-12-31")

    var big = b.column[List[Int32]](3)
    var want_first: List[Int32] = [10, 11]
    var want_empty: List[Int32] = []
    var want_last: List[Int32] = [20, 21, 22]
    assert_equal(big[0], want_first)
    assert_equal(big[1], want_empty)
    assert_equal(big[2], want_last)

    var fixed_col = b.column[Blob](4)
    assert_equal(fixed_col[0], Blob([UInt8(97), UInt8(98), UInt8(99), UInt8(100)]))
    assert_equal(fixed_col[1], Blob([UInt8(120), UInt8(121), UInt8(122), UInt8(48)]))
    assert_equal(fixed_col[2], Blob([UInt8(0), UInt8(0), UInt8(0), UInt8(0)]))

    # Width and scale come from the format, and a 32-bit negative is sign extended.
    var d32 = b.column[Decimal](5)
    assert_equal(d32[0], Decimal(9, 2, Int128(1234)))
    assert_equal(d32[1], Decimal(9, 2, Int128(-1234)))
    assert_equal(d32[2], Decimal(9, 2, Int128(0)))
    var d64 = b.column[Decimal](6)
    assert_equal(d64[0], Decimal(18, 4, Int128(123456)))
    assert_equal(d64[1], Decimal(18, 4, Int128(-987654321)))
    assert_equal(d64[2], Decimal(18, 4, Int128(0)))

    assert_equal(b.get[String](col=7, row=0), "one")
    assert_equal(b.get[String](col=7, row=1), "zero")
    assert_equal(b.get[String](col=7, row=2), "one")
    assert_equal(b.get[Blob](col=8, row=0), Blob([UInt8(97), UInt8(97)]))
    assert_equal(b.get[Blob](col=8, row=1), Blob([UInt8(99), UInt8(99), UInt8(99), UInt8(99)]))
    assert_equal(b.get[Blob](col=8, row=2), Blob([UInt8(98), UInt8(98), UInt8(98)]))

    # The child's `offset` shifts every element the list's offsets name.
    var cut = b.column[List[Int32]](9)
    var want_cut: List[Int32] = [10, 11, 12]
    assert_equal(cut[0], want_cut)
    assert_equal(cut[1], want_empty)
    assert_equal(cut[2], want_empty)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
