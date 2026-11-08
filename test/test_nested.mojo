"""Tests for nested types (emberserde-backed): reading LIST, ARRAY, STRUCT,
MAP and UNION values, rows as structs and tuples, nested values and
parameters, and the appender."""

from std.testing import assert_equal, assert_false, assert_raises, assert_true
from std.testing.suite import TestSuite
from std.utils import Variant
from emberserde import Alias, Rename
from duckdb import *
from duckdb.scalar_types import Date, Decimal


@fieldwise_init
struct User(Copyable, Defaultable, Movable):
    var id: Int32
    var name: String
    var age: Optional[Int32]

    def __init__(out self):
        self.id = 0
        self.name = ""
        self.age = None


@__annotation(Rename("num"))
@fieldwise_init
struct Num(Copyable, Movable):
    var v: Int32


@__annotation(Rename("txt"))
@fieldwise_init
struct Txt(Copyable, Defaultable, Movable):
    var v: String

    def __init__(out self):
        self.v = ""


@fieldwise_init
struct Point(Copyable, Movable):
    var x: Int32
    var y: Int32


@fieldwise_init
struct Tagged(Copyable, Defaultable, Movable):
    var label: String
    var tags: List[String]
    var pos: Point

    def __init__(out self):
        self.label = ""
        self.tags = []
        self.pos = Point(0, 0)


struct Renamed(Copyable, Defaultable, Movable):
    @__annotation(Rename("NAME"))
    var user_name: String

    def __init__(out self):
        self.user_name = ""


def _users() raises -> Connection:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    _ = con.execute("INSERT INTO users VALUES (1, 'Ada', 36), (2, 'Grace', 45), (3, 'Linus', NULL)")
    return con^


def _error_of(var r: Result) -> Optional[DuckDBError]:
    try:
        _ = r^.fetchall()
    except e:
        return e^
    return None


# ── Reading nested columns ───────────────────────────────────────


def test_list_column() raises:
    var con = Database().connect()
    var r = con.execute("SELECT [1, 2, 3]::INTEGER[] AS l UNION ALL SELECT [] UNION ALL SELECT [NULL, 5]").fetchall()
    var a = r.get[List[Int32]](col=0, row=0)
    assert_equal(len(a), 3)
    assert_equal(a[2], 3)
    assert_equal(len(r.get[List[Int32]](col=0, row=1)), 0)
    var c = r.get[List[Optional[Int32]]](col=0, row=2)
    assert_false(c[0].__bool__())
    assert_equal(c[1].value(), 5)
    # A NULL element without Optional is an error with a path.
    var failed = False
    try:
        _ = r.get[List[Int32]](col=0, row=2)
    except e:
        failed = True
        assert_true("NULL" in e.message, e.message)
    assert_true(failed)


def test_nested_list_of_strings_and_structs() raises:
    var con = Database().connect()
    var r = con.execute(
        "SELECT [['a', 'a string longer than twelve'], []] AS ll, [{'x': 1, 'y': 2}, {'x': 3, 'y': 4}] AS ps"
    ).fetchall()
    var ll = r.get[List[List[String]]](col=0, row=0)
    assert_equal(ll[0][1], "a string longer than twelve")
    assert_equal(len(ll[1]), 0)
    var ps = r.get[List[Point]](col=1, row=0)
    assert_equal(ps[1].y, 4)


def test_struct_column_by_name() raises:
    var con = Database().connect()
    # Field order differs and names differ in case.
    var r = con.execute("SELECT {'Y': 2, 'X': 1} AS p, NULL::STRUCT(x INTEGER, y INTEGER) AS q").fetchall()
    var p = r.get[Point](col=0, row=0)
    assert_equal(p.x, 1)
    assert_equal(p.y, 2)
    assert_false(r.get[Optional[Point]](col=1, row=0).__bool__())
    assert_false(r.get_optional[Point](col=1, row=0).__bool__())


def test_map_column() raises:
    var con = Database().connect()
    var r = con.execute("SELECT MAP {'a': 1, 'b': 2} AS m").fetchall()
    var d = r.get[Dict[String, Int32]](col=0, row=0)
    assert_equal(len(d), 2)
    assert_equal(d["b"], 2)
    # A MAP also reads as a list of key/value tuples, in order.
    var kv = r.get[List[Tuple[String, Int32]]](col=0, row=0)
    assert_equal(kv[0][0], "a")
    assert_equal(kv[1][1], 2)


def test_array_column() raises:
    var con = Database().connect()
    var r = con.execute("SELECT [1.5, 2.5, 3.5]::DOUBLE[3] AS a").fetchall()
    var a = r.get[List[Float64]](col=0, row=0)
    assert_equal(len(a), 3)
    assert_equal(a[2], 3.5)
    var t = r.get[Tuple[Float64, Float64, Float64]](col=0, row=0)
    assert_equal(t[1], 2.5)


def test_union_column() raises:
    var con = Database().connect()
    var r = con.execute(
        "SELECT u FROM (VALUES (union_value(i := 7)::UNION(i INTEGER, s VARCHAR)), ('text'::UNION(i INTEGER, s VARCHAR))) t(u)"
    ).fetchall()
    var a = r.get[Variant[Int32, String]](col=0, row=0)
    assert_true(a.isa[Int32]())
    assert_equal(a[Int32], 7)
    var b = r.get[Variant[Int32, String]](col=0, row=1)
    assert_true(b.isa[String]())
    assert_equal(b[String], "text")


def test_union_matching() raises:
    var con = Database().connect()
    # Member order differs from the arm order: matched by type.
    var r = con.execute("SELECT 'text'::UNION(s VARCHAR, i INTEGER) AS u, 5::UNION(s VARCHAR, i INTEGER) AS v").fetchall()
    assert_equal(r.get[Variant[Int32, String]](col=0, row=0)[String], "text")
    assert_equal(r.get[Variant[Int32, String]](col=1, row=0)[Int32], 5)
    # Several members may go to one arm.
    var two = con.execute("SELECT union_value(b := 7)::UNION(a INTEGER, b INTEGER) AS u").fetchall()
    assert_equal(two.get[Variant[Int32, String]](col=0, row=0)[Int32], 7)
    # A member that fits several arms is an error...
    var failed = False
    try:
        _ = two.get[Variant[Int32, Optional[Int32]]](col=0, row=0)
    except e:
        failed = True
        assert_true("several Variant arms" in e.message, e.message)
    assert_true(failed)
    # ...unless the arms are named after the members.
    var named = con.execute(
        "SELECT union_value(txt := {'v': 'x'})::UNION(num STRUCT(v INTEGER), txt STRUCT(v VARCHAR)) AS u"
    ).fetchall()
    assert_equal(named.get[Variant[Txt, Num]](col=0, row=0)[Txt].v, "x")
    # Writing matches the same way.
    _ = con.execute("CREATE TABLE t (u UNION(s VARCHAR, i INTEGER))")
    var a = Appender(con, "t")
    a.append_value(Variant[Int32, String](Int32(9)))
    a.end_row()
    a.close()
    assert_equal(con.execute("SELECT u.i FROM t").get[Int32]()[0], 9)


def test_scalar_types_inside_nested() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TYPE mood AS ENUM ('sad', 'happy')")
    var r = con.execute(
        "SELECT [DATE '2024-01-02'] AS d, ['happy'::mood, 'sad'] AS e, [1.25::DECIMAL(5, 2)] AS dec"
    ).fetchall()
    assert_equal(r.get[List[Date]](col=0, row=0)[0], Date(2024, 1, 2))
    assert_equal(r.get[List[String]](col=1, row=0)[0], "happy")
    assert_equal(len(r.get[List[Decimal]](col=2, row=0)), 1)


def test_column_reader_nested() raises:
    var con = Database().connect()
    var r = con.execute("SELECT CASE WHEN i % 2 = 0 THEN [i, i] END AS l FROM range(3000) t(i)").fetchall()
    var count = 0
    for c in range(r.chunk_count()):
        var reader = r.chunk(c).column[List[Int64]](0)
        for i in range(len(reader)):
            var v = reader.get(i)
            if v:
                count += 1
                assert_equal(len(v.value()), 2)
    assert_equal(count, 1500)


def test_type_mismatch_in_nested() raises:
    var con = Database().connect()
    var r = con.execute("SELECT {'x': 'not a number', 'y': 1} AS p").fetchall()
    var failed = False
    try:
        _ = r.get[Point](col=0, row=0)
    except e:
        failed = True
        assert_true(e.code == ErrorCode.TYPE_MISMATCH, String(e))
        assert_true(".x" in e.message, e.message)
    assert_true(failed)


# ── Rows as structs and tuples ───────────────────────────────────


def test_rows_as_structs() raises:
    var con = _users()
    var users = con.execute("SELECT * FROM users ORDER BY id").fetchall().get[User]()
    assert_equal(len(users), 3)
    assert_equal(users[1].name, "Grace")
    assert_equal(users[0].age.value(), 36)
    assert_false(users[2].age.__bool__())
    # Column order and case do not matter.
    var r = con.execute("SELECT AGE, Name, ID FROM users ORDER BY id").fetchall()
    assert_equal(r.get[User](row=0).name, "Ada")
    assert_equal(r.row(1).get[User]().id, 2)
    var it_count = 0
    for row in r.rows():
        it_count += Int(row.get[User]().id)
    assert_equal(it_count, 6)


def test_rows_rename_and_missing_column() raises:
    var con = _users()
    var r = con.execute("SELECT name FROM users ORDER BY id").fetchall()
    assert_equal(r.get[Renamed](row=0).user_name, "Ada")
    var failed = False
    try:
        _ = r.get[User](row=0)
    except e:
        failed = True
        assert_true("missing field" in e.message, e.message)
    assert_true(failed)


def test_rows_as_tuples_and_scalars() raises:
    var con = _users()
    var t = con.execute("SELECT id, name FROM users ORDER BY id").fetchall().get[Tuple[Int32, String]]()
    assert_equal(t[2][1], "Linus")
    var ids = con.execute("SELECT id FROM users ORDER BY id").get[Int32]()
    assert_equal(ids[2], 3)
    var rel = con.table("users").order("id").select("name")
    assert_equal(rel.get[String]()[0], "Ada")


def test_fetchone_and_fetchmany() raises:
    var con = Database().connect()
    var r = con.execute("SELECT i::INTEGER AS id, 'n' || i AS name, NULL::INTEGER AS age FROM range(5000) t(i)")
    var first = r.fetchone[User]()
    assert_equal(first.value().id, 0)
    var batch = r.fetchmany[User](3000)  # crosses a chunk boundary
    assert_equal(len(batch), 3000)
    assert_equal(batch[2999].name, "n3000")
    var rest = r.fetchmany[User](10000)
    assert_equal(len(rest), 1999)
    assert_false(r.fetchone[User]().__bool__())


# ── Values and parameters ────────────────────────────────────────


def test_nested_values_round_trip() raises:
    var l = Value([Int32(1), 2, 3])
    assert_equal(String(l.type()), "INTEGER[]")
    assert_equal(l.get[List[Int32]]()[2], 3)
    var empty = Value(List[String]())
    assert_equal(String(empty.type()), "VARCHAR[]")
    var t = Value(Tagged("p", ["a", "a long tag, longer than twelve"], Point(3, 4)))
    var back = t.get[Tagged]()
    assert_equal(back.tags[1], "a long tag, longer than twelve")
    assert_equal(back.pos.y, 4)
    var d = Dict[String, Int64]()
    d["k"] = 5
    assert_equal(Value(d).get[Dict[String, Int64]]()["k"], 5)
    var none = Value(Optional[Int32](None))
    assert_true(none.is_null())
    var v = Value(Variant[Int32, String](String("u")))
    assert_equal(v.get[Variant[Int32, String]]()[String], "u")


def test_nested_parameters() raises:
    var con = Database().connect()
    var ints: List[Int64] = [1, 2, 3]
    assert_equal(con.execute("SELECT list_sum($1)", [Int32(1), 2]).get[Int128]()[0], 3)
    var r = con.execute("SELECT list_sum($1) AS s, struct_extract($2, 'x') AS x", ints, Point(5, 6)).fetchall()
    assert_equal(r.get[Int128](col=0, row=0), 6)
    assert_equal(r.get[Int32](col=1, row=0), 5)


def test_struct_parameters() raises:
    var con = _users()
    var r = con.execute("SELECT name FROM users WHERE id >= $x AND id <= $y ORDER BY id", params=Point(2, 3)).fetchall()
    assert_equal(len(r), 2)
    assert_equal(r.get[String](col=0, row=0), "Grace")


# ── Appender ─────────────────────────────────────────────────────


def test_appender_rows_and_values() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE users (id INTEGER, name VARCHAR, age INTEGER)")
    var a = Appender(con, "users")
    a.append_row(User(1, "Ada", Int32(36)))
    a.append_row(User(2, "a name longer than twelve bytes", None))
    a.append_value(Int(3))  # cast to INTEGER
    a.append_value(String("Linus"))
    a.append_null()
    a.end_row()
    a.append_row((Int32(4), String("Tuple"), Int32(9)))
    assert_equal(a.flush(), 4)
    var users = con.execute("SELECT * FROM users ORDER BY id").fetchall().get[User]()
    assert_equal(len(users), 4)
    assert_equal(users[1].name, "a name longer than twelve bytes")
    assert_false(users[1].age.__bool__())
    assert_equal(users[2].id, 3)
    assert_false(users[2].age.__bool__())
    assert_equal(users[3].age.value(), 9)


def test_appender_many_rows_nested() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (label VARCHAR, tags VARCHAR[], pos STRUCT(x INTEGER, y INTEGER))")
    var a = Appender(con, "t")
    for i in range(5000):
        var tags = List[String]()
        for j in range(i % 4):
            tags.append(String("a tag that is longer than twelve bytes ", j))
        a.append_row(Tagged(String("row ", i), tags^, Point(Int32(i), Int32(-i))))
    a.close()
    var r = con.execute("SELECT count(*), sum(len(tags)), sum(pos.x) FROM t").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 5000)
    assert_equal(r.get[Int128](col=1, row=0), 7500)
    var last = con.execute("SELECT * FROM t WHERE label = 'row 4999'").fetchall().get[Tagged](row=0)
    assert_equal(len(last.tags), 3)
    assert_equal(last.tags[2], "a tag that is longer than twelve bytes 2")
    assert_equal(last.pos.y, -4999)


def test_appender_map_union_array() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER), u UNION(i INTEGER, s VARCHAR), a DOUBLE[2], l INTEGER[][])")
    var a = Appender(con, "t")
    var m = Dict[String, Int32]()
    m["one"] = 1
    m["two"] = 2
    a.append_value(m)
    a.append_value(Variant[Int32, String](String("text")))
    a.append_value([1.5, 2.5])  # a list literal
    var ll: List[List[Int32]] = [[Int32(1)], [Int32(2), 3]]
    a.append_value(ll)
    a.end_row()
    a.close()
    var r = con.execute("SELECT m['two'], u.s, a[2], len(l[2]) FROM t").fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 2)
    assert_equal(r.get[String](col=1, row=0), "text")
    assert_equal(r.get[Float64](col=2, row=0), 2.5)
    assert_equal(r.get[Int64](col=3, row=0), 2)


def test_appender_columns_and_defaults() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (id INTEGER, note VARCHAR DEFAULT 'none', n INTEGER)")
    var cols: List[String] = ["n", "id"]
    var a = Appender(con, "t", columns=cols^)
    a.append_value(Int32(10))
    a.append_value(Int32(1))
    a.end_row()
    a.close()
    var r = con.execute("SELECT id, note, n FROM t").fetchall()
    assert_equal(r.get[Int32](col=0, row=0), 1)
    assert_equal(r.get[String](col=1, row=0), "none")
    assert_equal(r.get[Int32](col=2, row=0), 10)


def test_appender_errors_and_recovery() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (id INTEGER, pos STRUCT(x INTEGER, y INTEGER))")
    var a = Appender(con, "t")
    var failed = False
    try:
        a.end_row()  # no values
    except e:
        failed = True
    assert_true(failed)
    # A failed row leaves no trace: a value that cannot be cast.
    failed = False
    try:
        a.append_row((Int32(1), String("not a struct")))
    except e:
        failed = True
    assert_true(failed)
    a.append_row((Int32(2), Point(1, 2)))
    # A NULL stays NULL when the row goes on, also after a failed value.
    a.append_null()
    failed = False
    try:
        a.append_value(String("not a struct"))
    except e:
        failed = True
    assert_true(failed)
    a.append_value(Point(3, 4))
    a.end_row()
    a.close()
    var r = con.execute("SELECT id, pos.y FROM t ORDER BY id NULLS LAST").fetchall()
    assert_equal(len(r), 2)
    assert_equal(r.get[Int32](col=0, row=0), 2)
    assert_equal(r.get[Int32](col=1, row=0), 2)
    assert_false(r.get_optional[Int32](col=0, row=1).__bool__())
    assert_equal(r.get[Int32](col=1, row=1), 4)


def test_appender_native_casts() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TYPE mood AS ENUM ('sad', 'happy')")
    _ = con.execute("CREATE TABLE t (s SMALLINT, d DOUBLE, m mood)")
    var a = Appender(con, "t")
    a.append_row((Int(300), Int(2), String("happy")))
    a.append_row((Int8(-1), Float32(0.5), String("sad")))
    a.close()
    var r = con.execute("SELECT s, d, m::VARCHAR FROM t ORDER BY s").fetchall()
    assert_equal(r.get[Int16](col=0, row=1), 300)
    assert_equal(r.get[Float64](col=1, row=1), 2.0)
    assert_equal(r.get[Float64](col=1, row=0), 0.5)
    assert_equal(r.get[String](col=2, row=1), "happy")
    var failed = False
    try:
        a.append_row((Int(100000), Int(0), String("sad")))  # SMALLINT overflow
    except e:
        failed = True
    assert_true(failed)
    failed = False
    try:
        a.append_row((Int(1), Int(0), String("angry")))  # not in the ENUM
    except e:
        failed = True
    assert_true(failed)


def test_appender_on_conflict_and_flush_errors() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, name VARCHAR)")
    _ = con.execute("INSERT INTO t VALUES (1, 'old')")
    var a = Appender(con, "t")
    a.append_row((Int32(1), String("dup")))
    a.append_row((Int32(2), String("new")))
    var message = String()
    try:
        _ = a.flush()
    except e:
        message = e.message
    assert_true("2 appended rows were dropped" in message, message)
    assert_equal(con.execute("SELECT count(*) FROM t").get[Int64]()[0], 1)
    var ignore = Appender(con, "t", on_conflict=OnConflict.IGNORE)
    ignore.append_row((Int32(1), String("dup")))
    ignore.append_row((Int32(2), String("new")))
    assert_equal(ignore.flush(), 1)
    var replace = Appender(con, "t", on_conflict=OnConflict.REPLACE)
    replace.append_row((Int32(1), String("replaced")))
    replace.close()
    assert_equal(con.execute("SELECT name FROM t WHERE id = 1").get[String]()[0], "replaced")


def test_appender_defaults() raises:
    var con = Database().connect()
    _ = con.execute("CREATE SEQUENCE seq")
    _ = con.execute("CREATE TABLE t (id INTEGER DEFAULT nextval('seq'), note VARCHAR DEFAULT 'none', n INTEGER)")
    var a = Appender(con, "t")
    a.append_default()
    a.append_value(String("given"))
    a.append_value(Int32(1))
    a.end_row()
    a.append_default()
    a.append_default()
    a.append_value(Int32(2))
    a.end_row()
    a.append_row((Int32(100), String("row"), Int32(3)))
    a.append_default()
    a.append_value(String("again"))
    a.append_value(Int32(4))
    a.end_row()
    assert_equal(a.flush(), 4)
    var r = con.execute("SELECT id, note FROM t ORDER BY n").fetchall()
    assert_equal(r.get[String](col=1, row=1), "none")
    assert_equal(r.get[Int32](col=0, row=2), 100)
    # Each defaulted id took its own sequence value.
    var ids = con.execute("SELECT id FROM t WHERE n <> 3 ORDER BY id").get[Int32]()
    assert_equal(ids[0], 1)
    assert_equal(ids[2], 3)
    var failed = False
    try:
        a.append_default()
        a.append_default()
        a.append_default()
        a.end_row()
    except e:
        failed = True
    assert_true(failed)


def test_appender_auto_flush() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (i BIGINT)")
    var a = Appender(con, "t", flush_rows=4096)
    for i in range(5000):
        a.append_row(Tuple(Int64(i)))
    # Two full chunks (4096 rows) are already inserted, the rest is pending.
    assert_equal(con.execute("SELECT count(*) FROM t").get[Int64]()[0], 4096)
    a.close()
    assert_equal(con.execute("SELECT count(*) FROM t").get[Int64]()[0], 5000)


struct Aliased(Copyable, Defaultable, Movable):
    var id: Int32
    @__annotation(Alias("name"))
    var user_name: String
    var age: Optional[Int32]

    def __init__(out self):
        self.id = 0
        self.user_name = String()
        self.age = None


def test_alias_reads_and_writes() raises:
    """A field's `Alias` binds the column of that name in both directions."""
    var con = _users()
    var r = con.execute("SELECT id, name, age FROM users ORDER BY id").fetchall()
    assert_equal(r.get[Aliased](row=0).user_name, "Ada")
    var a = Appender(con, "users")
    var row = Aliased()
    row.id = 4
    row.user_name = "Alan"
    row.age = Optional[Int32](Int32(41))
    a.append_row(row)
    a.close()
    var back = con.execute("SELECT name, age FROM users WHERE id = 4").fetchall()
    assert_equal(back.get[String](col=0, row=0), "Alan")
    assert_equal(back.get[Optional[Int32]](col=1, row=0).value(), 41)


def test_map_tuple_cell() raises:
    """A `Tuple` written to a MAP column is one entry. Reading a one-entry
    MAP as a tuple gives the entry back."""
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t (m MAP(VARCHAR, INTEGER))")
    var a = Appender(con, "t")
    a.append_value(Tuple(String("one"), Int32(1)))
    a.end_row()
    a.close()
    var r = con.execute("SELECT cardinality(m)::BIGINT, m['one'] FROM t").fetchall()
    assert_equal(r.get[Int64](col=0, row=0), 1)
    assert_equal(r.get[Optional[Int32]](col=1, row=0).value(), 1)
    var m = con.execute("SELECT m FROM t").fetchall()
    var t = m.get[Tuple[String, Int32]](col=0, row=0)
    assert_equal(t[0], "one")
    assert_equal(t[1], 1)


def test_array_span() raises:
    var con = Database().connect()
    _ = con.execute("CREATE TABLE t AS SELECT i AS id, [i, i + 0.5, -i]::FLOAT[3] AS v FROM range(3000) r(i)")
    var seen = 0
    for chunk in con.execute("SELECT v FROM t ORDER BY id").chunks():
        var span = chunk.array_span[Float32](0).value()
        assert_equal(len(span), len(chunk) * 3)
        for r in range(len(chunk)):
            var i = Float32(seen + r)
            assert_equal(span[r * 3], i)
            assert_equal(span[r * 3 + 1], i + 0.5)
            assert_equal(span[r * 3 + 2], -i)
        seen += len(chunk)
    assert_equal(seen, 3000)
    # A NULL array or element: no span.
    var nulls = con.execute("SELECT [1, NULL, 3]::FLOAT[3] UNION ALL SELECT NULL").fetchall()
    assert_false(Bool(nulls.chunk(0).array_span[Float32](0)))
    # Wrong element type, or not an ARRAY.
    var res = con.execute("SELECT [1, 2]::DOUBLE[2] AS a, 1 AS b").fetchall()
    with assert_raises(contains="cannot be read as"):
        _ = res.chunk(0).array_span[Float32](0)
    with assert_raises(contains="not an ARRAY"):
        _ = res.chunk(0).array_span[Int32](1)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
