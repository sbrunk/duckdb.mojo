"""POC: read DuckDB V2 results through emberserde (duckdb/_serde_poc.mojo)."""

from duckdb._serde_poc import PocDatabase
from duckdb._libduckdb_v2 import *
from duckdb.duckdb_type import Date
from emberserde import Rename, Default
from std.testing import assert_equal, assert_true, assert_false
from std.testing.suite import TestSuite


@fieldwise_init
struct Address(Defaultable, Movable):
    var city: String
    var zip: Optional[String]

    def __init__(out self):
        self.city = ""
        self.zip = None


@fieldwise_init
struct Person(Defaultable, Movable):
    var id: Int64
    var name: String
    var address: Address  # STRUCT inside the row
    var tags: List[String]  # LIST
    var born: Date  # native DuckDB type, intercepted

    def __init__(out self):
        self.id = 0
        self.name = ""
        self.address = Address()
        self.tags = []
        self.born = Date(0)


def test_nested_struct_list_date() raises:
    var db = PocDatabase()
    var rows = db.query(
        "SELECT 1::BIGINT AS id, 'Mark' AS name,"
        " {'city': 'Amsterdam', 'zip': '1011'} AS address,"
        " ['a', 'a string longer than twelve'] AS tags,"
        " DATE '1970-01-11' AS born"
    ).rows[Person]()
    assert_equal(len(rows), 1)
    assert_equal(rows[0].id, 1)
    assert_equal(rows[0].name, "Mark")
    assert_equal(rows[0].address.city, "Amsterdam")
    assert_equal(rows[0].address.zip.value(), "1011")
    assert_equal(len(rows[0].tags), 2)
    assert_equal(rows[0].tags[1], "a string longer than twelve")
    assert_equal(rows[0].born.days, 10)


@fieldwise_init
struct Inner(Defaultable, Movable):
    var x: Int32
    var label: String

    def __init__(out self):
        self.x = 0
        self.label = ""


@fieldwise_init
struct Outer(Defaultable, Movable):
    var inner: Inner  # struct in struct
    var items: List[Inner]  # list of structs
    var nums: List[Optional[Int32]]  # list with NULL elements
    var maybe: Optional[Inner]  # nullable struct

    def __init__(out self):
        self.inner = Inner()
        self.items = []
        self.nums = []
        self.maybe = None


@fieldwise_init
struct Wrap(Defaultable, Movable):
    var o: Outer

    def __init__(out self):
        self.o = Outer()


def test_struct_in_struct_and_lists_of_structs() raises:
    var db = PocDatabase()
    var rows = db.query(
        "SELECT {'inner': {'x': 1, 'label': 'in'},"
        " 'items': [{'x': 2, 'label': 'a'}, {'x': 3, 'label': 'b'}],"
        " 'nums': [1, NULL, 3],"
        " 'maybe': NULL::STRUCT(x INTEGER, label VARCHAR)} AS o"
    ).rows[Wrap]()
    ref o = rows[0].o
    assert_equal(o.inner.x, 1)
    assert_equal(o.inner.label, "in")
    assert_equal(len(o.items), 2)
    assert_equal(o.items[1].x, 3)
    assert_equal(o.items[1].label, "b")
    assert_equal(len(o.nums), 3)
    assert_true(o.nums[1] is None)
    assert_equal(o.nums[2].value(), 3)
    assert_true(o.maybe is None)


@fieldwise_init
struct Pt(Defaultable, Movable):
    var i: Int64
    var c: String

    def __init__(out self):
        self.i = 0
        self.c = ""


def test_vector_kinds() raises:
    """Queries that produce sequence, constant and dictionary vectors inside DuckDB."""
    var db = PocDatabase()
    db.run("CREATE TABLE t AS SELECT range AS i, ('k' || (range % 3)) AS c FROM range(3000)")
    var seq = db.query("SELECT range AS i, 'const' AS c FROM range(2500)").rows[Pt]()
    assert_equal(len(seq), 2500)
    assert_equal(seq[2499].i, 2499)
    assert_equal(seq[7].c, "const")
    # A join on a small key produces dictionary vectors on the probe side.
    var dict_rows = db.query(
        "SELECT t.i, d.c FROM t JOIN (SELECT DISTINCT c FROM t) d USING (c) ORDER BY t.i"
    ).rows[Pt]()
    assert_equal(len(dict_rows), 3000)
    assert_equal(dict_rows[4].c, "k1")

    # Query results come out of DuckDB's result collection, so they are always
    # flat: the constant, join and range columns above are all FLAT here. Non-flat
    # vectors only reach callbacks (UDF arguments); the cursor still applies the
    # view's selection, but query results cannot exercise it.
    var r1 = db.query("SELECT range AS i, 'const' AS c FROM range(2500)")
    var k1 = r1.fetch_chunk().value().column_vector_types()
    assert_equal(k1[0], DUCKDB_V2_VECTOR_TYPE_FLAT)
    assert_equal(k1[1], DUCKDB_V2_VECTOR_TYPE_FLAT)


@fieldwise_init
struct Renamed(Defaultable, Movable):
    @__annotation(Rename("UserName"))
    var user: String
    var Count: Int64  # case-insensitive match against column "count"

    def __init__(out self):
        self.user = ""
        self.Count = 0


def test_rename_and_case_insensitive_names() raises:
    var db = PocDatabase()
    var rows = db.query("SELECT 'x' AS UserName, 5::BIGINT AS count").rows[Renamed]()
    assert_equal(rows[0].user, "x")
    assert_equal(rows[0].Count, 5)


def test_missing_column_and_null_errors() raises:
    var db = PocDatabase()
    var raised = False
    try:
        _ = db.query("SELECT 1::BIGINT AS i").rows[Pt]()
    except e:
        raised = True
        assert_true("missing field: c" in String(e), String(e))
    assert_true(raised)
    raised = False
    try:
        _ = db.query("SELECT {'inner': {'x': NULL::INTEGER, 'label': 'l'}, 'items': [], 'nums': [], 'maybe': NULL::STRUCT(x INTEGER, label VARCHAR)} AS o").rows[Wrap]()
    except e:
        raised = True
        var msg = String(e)
        assert_true("unexpected NULL" in msg and ".o.inner.x" in msg, msg)
    assert_true(raised)


def test_column_reader() raises:
    var db = PocDatabase()
    var r = db.query("SELECT [range, NULL] AS l FROM range(3)")
    var chunk = r.fetch_chunk()
    var col = chunk.value().column[List[Optional[Int64]]](0)
    assert_equal(len(col), 3)
    assert_equal(col[2][0].value(), 2)
    assert_true(col[2][1] is None)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
