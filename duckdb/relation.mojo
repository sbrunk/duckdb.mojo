"""Lazy, composable relations (DuckDB's `DuckDBPyRelation` for Mojo).

A `Relation` is a symbolic query: a connection plus the SQL text built so far.
Nothing runs until a terminal method is called. Every transform returns a new
relation that wraps the previous SQL as a subquery:

```mojo
var top = (
    con.sql("FROM read_csv('trips.csv')")
       .filter("fare > 0")
       .aggregate("payment, sum(fare) AS total", group="payment")
       .order("total DESC")
       .limit(10)
)
top.show()                    # executes here
var rows = top.fetchall()     # runs again, relations can be re-run
```

- A relation borrows its connection through an origin-tracked pointer, so
  the connection outlives it.
- Values are interpolated with `lit`, never bound as parameters. For
  parameterized queries use `Connection.execute` directly.
- Each transform nests in its own subquery, which keeps clause semantics
  correct (a `filter` after an `aggregate` filters the grouped rows). DuckDB's
  optimizer flattens the nesting.
"""

from duckdb._runtime import api_error
from duckdb._sql_util import _quote_ident, _quote_literal, _quote_qualified
from duckdb.database import Connection
from duckdb.error import DuckDBError
from duckdb.result import Column, MaterializedResult, Result


def _reader_call(fn_name: String, path: String, options: Dict[String, String]) -> String:
    """`read_*('path', key=value, ...)`. The path is quoted, option values are verbatim."""
    var out = String(fn_name, "(", _quote_literal(path))
    for entry in options.items():
        out += String(", ", entry.key, "=", entry.value)
    out += ")"
    return out^


struct Relation[origin: ImmOrigin](Copyable, Movable):
    """A lazy, composable SQL relation. See the module docs.

    Parameters:
        origin: The origin of the borrowed connection.
    """

    var _conn: Pointer[Connection, Self.origin]
    var _sql: String
    var _alias: String

    def __init__(out self, conn: Pointer[Connection, Self.origin], query: String, relation_alias: String = ""):
        self._conn = conn
        self._sql = query
        self._alias = relation_alias

    # ── Composition internals ──────────────────────────────────────

    def _from(self) -> String:
        """This relation as a FROM-clause subquery, with its alias if set."""
        if self._alias.byte_length() != 0:
            return String("(", self._sql, ") AS ", _quote_ident(self._alias))
        return String("(", self._sql, ")")

    def _wrap(self, query: String) -> Self:
        return Self(self._conn, query)

    def _run(self, query: String) raises DuckDBError -> Result:
        return self._conn[].execute(query)

    # ── Introspection ──────────────────────────────────────────────

    def sql_query(self) -> String:
        """The composed SQL text."""
        return self._sql.copy()

    def alias(self) -> String:
        return self._alias.copy()

    def set_alias(self, relation_alias: String) -> Self:
        """A copy with the given subquery alias, so `join` conditions can qualify columns."""
        return Self(self._conn, self._sql, relation_alias)

    # ── Transforms ─────────────────────────────────────────────────

    def select(self, *cols: String) -> Self:
        """Project columns or expressions (SQL fragments, see `col`, `lit`, `fn_`)."""
        var proj = String("")
        for i in range(len(cols)):
            if i > 0:
                proj += ", "
            proj += cols[i]
        if len(cols) == 0:
            proj = "*"
        return self._wrap(String("SELECT ", proj, " FROM ", self._from()))

    def filter(self, predicate: String) -> Self:
        return self._wrap(String("SELECT * FROM ", self._from(), " WHERE ", predicate))

    def order(self, expr: String) -> Self:
        return self._wrap(String("SELECT * FROM ", self._from(), " ORDER BY ", expr))

    def limit(self, n: Int, offset: Int = 0) -> Self:
        var s = String("SELECT * FROM ", self._from(), " LIMIT ", n)
        if offset > 0:
            s += String(" OFFSET ", offset)
        return self._wrap(s)

    def distinct(self) -> Self:
        return self._wrap(String("SELECT DISTINCT * FROM ", self._from()))

    def aggregate(self, aggr: String, group: String = "") -> Self:
        """`rel.aggregate("k, sum(v) AS s", group="k")` is `SELECT k, sum(v) AS s FROM (...) GROUP BY k`."""
        var s = String("SELECT ", aggr, " FROM ", self._from())
        if group.byte_length() != 0:
            s += String(" GROUP BY ", group)
        return self._wrap(s)

    # ── Joins and set operations ───────────────────────────────────

    def join[o2: ImmOrigin](self, other: Relation[o2], on: String, how: String = "inner") raises DuckDBError -> Self:
        """Join on `on`. `how` is `inner`, `left`, `right`, `outer`, `semi` or `anti`.

        Raises a `DuckDBError` for any other `how` (case-insensitive): `how`
        is spliced into the SQL, so an unknown word would build broken SQL or
        an unintended join kind.
        """
        var kind = String(how).lower()
        if (
            kind != "inner"
            and kind != "left"
            and kind != "right"
            and kind != "outer"
            and kind != "semi"
            and kind != "anti"
        ):
            raise api_error(String("unknown join kind '", how, "'; expected one of inner, left, right, outer, semi, anti"))
        return self._wrap(String("SELECT * FROM ", self._from(), " ", kind, " JOIN ", other._from(), " ON ", on))

    def cross[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        return self._wrap(String("SELECT * FROM ", self._from(), " CROSS JOIN ", other._from()))

    def union[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        return Self(self._conn, String("(", self._sql, ") UNION (", other._sql, ")"))

    def union_all[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        return Self(self._conn, String("(", self._sql, ") UNION ALL (", other._sql, ")"))

    def except_[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        return Self(self._conn, String("(", self._sql, ") EXCEPT (", other._sql, ")"))

    def intersect[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        return Self(self._conn, String("(", self._sql, ") INTERSECT (", other._sql, ")"))

    # ── Aggregate shorthands ───────────────────────────────────────

    def _agg(self, fn_name: String, column: String, group: String) -> Self:
        var proj = String(fn_name, "(", column, ")")
        if group.byte_length() != 0:
            return self._wrap(String("SELECT ", group, ", ", proj, " FROM ", self._from(), " GROUP BY ", group))
        return self._wrap(String("SELECT ", proj, " FROM ", self._from()))

    def sum(self, column: String, group: String = "") -> Self:
        return self._agg("sum", column, group)

    def avg(self, column: String, group: String = "") -> Self:
        return self._agg("avg", column, group)

    def min(self, column: String, group: String = "") -> Self:
        return self._agg("min", column, group)

    def max(self, column: String, group: String = "") -> Self:
        return self._agg("max", column, group)

    def median(self, column: String, group: String = "") -> Self:
        return self._agg("median", column, group)

    def stddev(self, column: String, group: String = "") -> Self:
        return self._agg("stddev", column, group)

    def var(self, column: String, group: String = "") -> Self:
        """`var_samp(column)`."""
        return self._agg("var_samp", column, group)

    def count(self, column: String = "*", group: String = "") -> Self:
        return self._agg("count", column, group)

    # ── Terminals (these execute) ──────────────────────────────────

    def execute(self) raises DuckDBError -> Result:
        """Execute and return the streaming `Result`."""
        return self._run(self._sql)

    def fetchall(self) raises DuckDBError -> MaterializedResult:
        """Execute and read all rows into memory."""
        return self._run(self._sql).fetchall()

    def get[T: Movable & Deinitable](self) raises DuckDBError -> List[T]:
        """Execute and read all rows as `T` (a struct, a `Tuple`, or a
        scalar for one column)."""
        return self._run(self._sql).get[T]()

    def show(self, max_rows: Int = 20, max_width: Int = 0, max_col_width: Int = 0) raises DuckDBError:
        """Execute and print a box table."""
        self._run(self._sql).show(max_rows, max_width, max_col_width)

    def render(self, max_rows: Int = 20, max_width: Int = 0, max_col_width: Int = 0) raises DuckDBError -> String:
        """Execute and return the box table `show` prints."""
        return self._run(self._sql).render(max_rows, max_width, max_col_width)

    def columns(self) raises DuckDBError -> List[Column]:
        """The result columns, without running the query."""
        return self._conn[].describe(self._sql).columns.copy()

    def shape(self) raises DuckDBError -> Tuple[Int, Int]:
        """`(row_count, column_count)`. Runs a `count(*)`, not the whole query."""
        var n = self.aggregate("count(*)::BIGINT AS n").fetchall().get[Int64](col=0, row=0)
        return (Int(n), len(self.columns()))

    def describe(self) -> Self:
        """The columns and types as a relation (`DESCRIBE`)."""
        return self._wrap(String("DESCRIBE ", self._sql))

    def explain(self) raises DuckDBError -> String:
        """The physical plan as text."""
        var plan = self._run(String("EXPLAIN ", self._sql)).fetchall()
        # The plan text is the `explain_value` column; look it up by name so a
        # DuckDB column change fails loudly instead of reading the wrong cell.
        var col = plan.column_index("explain_value")
        var out = String("")
        for i in range(len(plan)):
            if i > 0:
                out += "\n"
            out += plan.get[String](col=col, row=i)
        return out^

    def create(self, table: String) raises DuckDBError:
        """`CREATE TABLE table AS <query>`."""
        _ = self._run(String("CREATE TABLE ", _quote_qualified(table), " AS ", self._sql))

    def create_view(self, name: String, replace: Bool = True) raises DuckDBError -> Self:
        """Persist as a view and return a relation over it."""
        var or_replace = String("OR REPLACE ") if replace else String("")
        _ = self._run(String("CREATE ", or_replace, "VIEW ", _quote_qualified(name), " AS ", self._sql))
        return Self(self._conn, String("SELECT * FROM ", _quote_qualified(name)))

    def insert_into(self, table: String) raises DuckDBError:
        """Append the rows to an existing table."""
        _ = self._run(String("INSERT INTO ", _quote_qualified(table), " ", self._sql))

    def to_parquet(self, path: String) raises DuckDBError:
        _ = self._run(String("COPY (", self._sql, ") TO ", _quote_literal(path), " (FORMAT parquet)"))

    def to_csv(self, path: String, *, header: Bool = True) raises DuckDBError:
        var hdr = String("true") if header else String("false")
        _ = self._run(String("COPY (", self._sql, ") TO ", _quote_literal(path), " (FORMAT csv, HEADER ", hdr, ")"))
