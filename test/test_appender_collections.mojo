"""Tests for the Appender API with collection types (List, Variant, Dict, Array, Bit)."""

from duckdb import *
from duckdb.duckdb_type import Bit, TimeNS
from std.collections import Optional, Dict
from std.utils import Variant
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


# ─── List tests ──────────────────────────────────────────────────


def test_append_list_int32():
    """Append a List[Int32] into a LIST(INTEGER) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    var appender = Appender(con, "t")

    var l1 = List[Int32]()
    l1.append(1)
    l1.append(2)
    l1.append(3)
    appender.append_value(l1)
    appender.end_row()

    var l2 = List[Int32]()
    l2.append(4)
    l2.append(5)
    appender.append_value(l2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var lists = chunk.get[List[Optional[Int32]]](col=0)
    assert_equal(len(lists), 2)

    var row0 = lists[0].copy()
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)

    var row1 = lists[1].copy()
    assert_equal(len(row1), 2)
    assert_equal(row1[0].value(), 4)
    assert_equal(row1[1].value(), 5)


def test_append_list_int64():
    """Append a List[Int64] into a LIST(BIGINT) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums BIGINT[])")
    var appender = Appender(con, "t")

    var l = List[Int64]()
    l.append(100)
    l.append(200)
    l.append(300)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Int64]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), 100)
    assert_equal(row[1].value(), 200)
    assert_equal(row[2].value(), 300)


def test_append_list_float64():
    """Append a List[Float64] into a LIST(DOUBLE) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (vals DOUBLE[])")
    var appender = Appender(con, "t")

    var l = List[Float64]()
    l.append(1.5)
    l.append(2.5)
    l.append(3.5)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT vals FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Float64]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), 1.5)
    assert_equal(row[1].value(), 2.5)
    assert_equal(row[2].value(), 3.5)


def test_append_list_bool():
    """Append a List[Bool] into a LIST(BOOLEAN) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (flags BOOLEAN[])")
    var appender = Appender(con, "t")

    var l = List[Bool]()
    l.append(True)
    l.append(False)
    l.append(True)
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT flags FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Bool]]](col=0, row=0)
    assert_equal(len(row), 3)
    assert_equal(row[0].value(), True)
    assert_equal(row[1].value(), False)
    assert_equal(row[2].value(), True)


def test_append_empty_list():
    """Append an empty list."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (nums INTEGER[])")
    var appender = Appender(con, "t")
    appender.append_value(List[Int32]())
    appender.end_row()
    appender.close()

    result = con.execute("SELECT nums FROM t")
    var chunk = result.fetch_chunk()
    var row = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(row), 0)


# ─── Variant / UNION tests ──────────────────────────────────────


def test_append_variant_int_member():
    """Append a Variant whose active member is an integer."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(42)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v.isa[Int32]())
    assert_equal(v[Int32], 42)


def test_append_variant_str_member():
    """Append a Variant whose active member is a string."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v.isa[String]())
    assert_equal(v[String], "hello")


def test_append_variant_multiple_rows():
    """Append several rows with different active Variant members."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(1)))
    appender.end_row()
    appender.append_value(Variant[Int32, String](String("two")))
    appender.end_row()
    appender.append_value(Variant[Int32, String](Int32(3)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var vals = chunk.get[Variant[Int32, String]](col=0)
    assert_equal(len(vals), 3)

    assert_true(vals[0].isa[Int32]())
    assert_equal(vals[0][Int32], 1)

    assert_true(vals[1].isa[String]())
    assert_equal(vals[1][String], "two")

    assert_true(vals[2].isa[Int32]())
    assert_equal(vals[2][Int32], 3)


def test_append_variant_three_members():
    """Append to a UNION with three member types."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(i INTEGER, f FLOAT, s VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, Float32, String](Float32(3.14)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t")
    var chunk = result.fetch_chunk()
    var v = chunk.get[Variant[Int32, Float32, String]](col=0, row=0)
    assert_true(v.isa[Float32]())


def test_append_variant_with_id_column():
    """Append Variant to a table that also has a regular column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (id INTEGER, u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.append_value(Int32(2))
    appender.append_value(Variant[Int32, String](Int32(99)))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id, u FROM t ORDER BY id")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    var v0 = chunk.get[Variant[Int32, String]](col=1, row=0)
    assert_true(v0.isa[String]())
    assert_equal(v0[String], "hello")

    assert_equal(chunk.get[Int32](col=0, row=1), Int32(2))
    var v1 = chunk.get[Variant[Int32, String]](col=1, row=1)
    assert_true(v1.isa[Int32]())
    assert_equal(v1[Int32], 99)


def test_append_variant_roundtrip():
    """Append Variant values and read them back via Variant deserialization."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (u UNION(num INTEGER, str VARCHAR))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Variant[Int32, String](Int32(42)))
    appender.end_row()
    appender.append_value(Variant[Int32, String](String("hello")))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT u FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var v0 = chunk.get[Variant[Int32, String]](col=0, row=0)
    assert_true(v0.isa[Int32]())
    assert_equal(v0[Int32], 42)

    var v1 = chunk.get[Variant[Int32, String]](col=0, row=1)
    assert_true(v1.isa[String]())
    assert_equal(v1[String], "hello")


# ─── Dict / MAP tests ────────────────────────────────────────────


def test_append_dict_map():
    """Append a Dict to a MAP column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    var appender = Appender(con, "t")
    var d: Dict[String, Int32] = {'a': 1, 'b': 2}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, Int32]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["a"], 1)
    assert_equal(m["b"], 2)


def test_append_dict_map_multiple_rows():
    """Append multiple Dict rows to a MAP column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    var appender = Appender(con, "t")

    var d1: Dict[String, Int32] = {'x': 10}
    appender.append_value(d1)
    appender.end_row()

    var d2: Dict[String, Int32] = {'y': 20, 'z': 30}
    appender.append_value(d2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var dicts = chunk.get[Dict[String, Int32]](col=0)
    assert_equal(len(dicts), 2)

    var m0 = dicts[0].copy()
    assert_equal(len(m0), 1)
    assert_equal(m0["x"], 10)

    var m1 = dicts[1].copy()
    assert_equal(len(m1), 2)
    assert_equal(m1["y"], 20)
    assert_equal(m1["z"], 30)


def test_append_dict_map_with_id():
    """Append Dict to table with regular column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (id INTEGER, m MAP(VARCHAR, INTEGER))"
    )
    var appender = Appender(con, "t")
    appender.append_value(Int32(1))
    var d: Dict[String, Int32] = {'key': 42}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT id, m FROM t")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), Int32(1))
    var m = chunk.get[Dict[String, Int32]](col=1, row=0)
    assert_equal(m["key"], 42)


def test_append_dict_map_roundtrip():
    """Append Dict values and read back as Dict — full roundtrip."""
    con = DuckDB.connect(":memory:")
    _ = con.execute(
        "CREATE TABLE t (m MAP(INTEGER, VARCHAR))"
    )
    var appender = Appender(con, "t")
    var d = Dict[Int32, String]()
    d[Int32(1)] = "one"
    d[Int32(2)] = "two"
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[Int32, String]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m[Int32(1)], "one")
    assert_equal(m[Int32(2)], "two")


def test_append_dict_map_mojo_int():
    """Append Dict[String, Int] using Mojo's native Int type."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, BIGINT))")
    var appender = Appender(con, "t")
    var d: Dict[String, Int] = {'a': 1, 'b': 2}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, Int]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["a"], 1)
    assert_equal(m["b"], 2)


def test_append_dict_map_mojo_uint():
    """Append Dict[String, UInt] using Mojo's native UInt type."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, UBIGINT))")
    var appender = Appender(con, "t")
    var d: Dict[String, UInt] = {'x': 10, 'y': 20}
    appender.append_value(d)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT m FROM t")
    var chunk = result.fetch_chunk()
    var m = chunk.get[Dict[String, UInt]](col=0, row=0)
    assert_equal(len(m), 2)
    assert_equal(m["x"], 10)
    assert_equal(m["y"], 20)


# ─── Mojo native Int/UInt tests ─────────────────────────────────


def test_appender_mojo_int():
    """Append and read back Mojo's native Int type as a scalar."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (v BIGINT)")
    var appender = Appender(con, "t")
    appender.append_value(Int(42))
    appender.end_row()
    appender.append_value(Int(-7))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT v FROM t ORDER BY v")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int](col=0, row=0), -7)
    assert_equal(chunk.get[Int](col=0, row=1), 42)


def test_appender_mojo_uint():
    """Append and read back Mojo's native UInt type as a scalar."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (v UBIGINT)")
    var appender = Appender(con, "t")
    appender.append_value(UInt(100))
    appender.end_row()
    appender.append_value(UInt(200))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT v FROM t ORDER BY v")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[UInt](col=0, row=0), 100)
    assert_equal(chunk.get[UInt](col=0, row=1), 200)


# ─── TimeNS, Array, Bit tests ───────────────────────────────────


def test_appender_time_ns():
    """Append and read back TIME_NS values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (t TIME_NS)")
    var appender = Appender(con, "t")
    appender.append_value(TimeNS(0))
    appender.end_row()
    appender.append_value(TimeNS(3_600_000_000_000))  # 1 hour
    appender.end_row()
    appender.close()

    result = con.execute("SELECT t::VARCHAR FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "00:00:00")
    assert_equal(chunk.get[String](col=0, row=1), "01:00:00")


def test_appender_array_int32():
    """Append List[Int32] to an ARRAY column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr INTEGER[3])")
    var appender = Appender(con, "t")
    var l1: List[Int32] = [1, 2, 3]
    appender.append_value(l1)
    appender.end_row()
    var l2: List[Int32] = [4, 5, 6]
    appender.append_value(l2)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT arr FROM t ORDER BY rowid")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[Optional[Int32]]](col=0, row=0)
    assert_equal(len(row0), 3)
    assert_equal(row0[0].value(), 1)
    assert_equal(row0[1].value(), 2)
    assert_equal(row0[2].value(), 3)
    var row1 = chunk.get[List[Optional[Int32]]](col=0, row=1)
    assert_equal(row1[0].value(), 4)


def test_appender_array_varchar():
    """Append List[String] to an ARRAY(VARCHAR) column."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (arr VARCHAR[2])")
    var appender = Appender(con, "t")
    var l: List[String] = ["hello", "world"]
    appender.append_value(l)
    appender.end_row()
    appender.close()

    result = con.execute("SELECT arr FROM t")
    var chunk = result.fetch_chunk()
    var row0 = chunk.get[List[Optional[String]]](col=0, row=0)
    assert_equal(len(row0), 2)
    assert_equal(row0[0].value(), "hello")
    assert_equal(row0[1].value(), "world")


def test_appender_bit():
    """Test appending Bit values."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit("10110"))
    appender.end_row()
    appender.append_value(Bit("0"))
    appender.end_row()
    appender.close()

    # Read back as VARCHAR to verify
    result = con.execute("SELECT b::VARCHAR FROM t ORDER BY rowid")
    chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "10110")
    assert_equal(chunk.get[String](col=0, row=1), "0")


def test_appender_bit_round_trip():
    """Test round-trip of BIT through appender and typed API."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit("10110"))
    appender.end_row()
    appender.close()

    result = con.execute("SELECT b FROM t")
    chunk = result.fetch_chunk()
    var val = chunk.get[Bit](col=0, row=0)
    assert_equal(String(val), "10110")
    assert_equal(len(val), 5)


def test_appender_bit_from_int32():
    """Test appending Bit constructed from Int32 matches DuckDB cast."""
    con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE t (b BIT)")
    var appender = Appender(con, "t")
    appender.append_value(Bit(Int32(123)))
    appender.end_row()
    appender.close()

    # Read back the appended value
    result = con.execute("SELECT b::VARCHAR FROM t")
    chunk = result.fetch_chunk()
    var appended = chunk.get[String](col=0, row=0)

    # Compare with DuckDB's own cast
    result2 = con.execute("SELECT (123::INTEGER::BITSTRING)::VARCHAR")
    chunk2 = result2.fetch_chunk()
    var duckdb_cast = chunk2.get[String](col=0, row=0)

    assert_equal(appended, duckdb_cast)


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
