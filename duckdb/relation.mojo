"""Lazy, composable relations (DuckDB's `DuckDBPyRelation` for Mojo).

A `Relation` is a symbolic query: it holds a borrowed reference to a connection
plus the SQL text built so far, and executes nothing until a terminal method is
called. Every transform (`filter`, `select`, `order`, ...) returns a *new*
relation that wraps the previous SQL as a subquery, so transforms compose
without mutating and without prematurely running anything:

```mojo
from duckdb import DuckDB, col, lit

var con = DuckDB.connect(":memory:")
var top = (
    con.sql("FROM read_csv('trips.csv')")
       .filter("fare > 0")
       .aggregate("payment, sum(fare) AS total", group="payment")
       .order("total DESC")
       .limit(10)
)
top.show()                      # executes here
var rows = top.fetchall()       # re-executes; relations are re-runnable
```

Design notes:

- A relation borrows its connection through an origin-tracked `Pointer`, so the
  connection is kept alive for as long as the relation (and anything derived
  from it) lives. The `origin` parameter threads through every transform.
- Values are interpolated through `lit` / `quote_literal`, never through bound
  parameters. Parameterized queries use `Connection.execute(...)` directly.
- Each transform nests in its own subquery (`SELECT ... FROM (<parent>)`), which
  keeps clause semantics correct (a `filter` after an `aggregate` becomes a
  `WHERE` over the grouped result, never a malformed fused clause). DuckDB's
  optimizer flattens the nesting.
"""

from duckdb._libduckdb import *
from duckdb.api import DuckDB
from duckdb.result import Result, MaterializedResult, ResultError, ErrorType
from duckdb.logical_type import LogicalType
from duckdb._sql_util import _quote_ident, _quote_literal, _quote_qualified


struct Relation[origin: ImmOrigin](Copyable, Movable, Writable):
    """A lazy, composable SQL relation. See module docstring.

    Parameters:
        origin: The origin of the borrowed connection. Threaded through
            transforms so the connection outlives the relation.
    """

    var _conn_ptr: Pointer[duckdb_connection, Self.origin]
    var _sql: String
    var _alias: String

    def __init__(
        out self,
        conn_ptr: Pointer[duckdb_connection, Self.origin],
        query: String,
        relation_alias: String = "",
    ):
        """Wrap ``query`` as a relation borrowing the connection at ``conn_ptr``."""
        self._conn_ptr = conn_ptr
        self._sql = query.copy()
        self._alias = relation_alias.copy()

    # ── Composition internals ──────────────────────────────────────

    def _conn(self) -> duckdb_connection:
        return self._conn_ptr[]

    def _from(self) -> String:
        """This relation as a FROM-clause subquery, with its alias if set."""
        if self._alias.byte_length() != 0:
            return String("(", self._sql, ") AS ", _quote_ident(self._alias))
        return String("(", self._sql, ")")

    def _wrap(self, query: String) -> Self:
        """A new relation wrapping ``query`` (alias resets)."""
        return Self(self._conn_ptr, query)

    def _run(self, query: String) raises ResultError -> Result:
        """Execute ``query`` on this relation's connection (mirrors `Connection.execute`)."""
        var result = duckdb_result()
        var result_ptr = Pointer(to=result)
        var _query = query.copy()
        ref libduckdb = DuckDB().libduckdb()
        var state = libduckdb.duckdb_query(
            self._conn_ptr[], _query.as_c_string_slice().unsafe_ptr(), result_ptr
        )
        if state == DuckDBError:
            var error_msg = String(
                unsafe_from_utf8_ptr=libduckdb.duckdb_result_error(result_ptr)
            )
            var error_type_value = libduckdb.duckdb_result_error_type(result_ptr)
            libduckdb.duckdb_destroy_result(result_ptr)
            raise ResultError(error_msg, ErrorType(error_type_value))
        return Result(result)

    # ── Introspection ──────────────────────────────────────────────

    def sql_query(self) -> String:
        """The composed SQL text (Python ``rel.sql_query()``)."""
        return self._sql.copy()

    def alias(self) -> String:
        """This relation's current alias (empty if unset)."""
        return self._alias.copy()

    def set_alias(self, relation_alias: String) -> Self:
        """Return a copy of this relation with the given subquery alias.

        Useful before `join` so a join condition can qualify columns
        (``a.id = b.id``).
        """
        return Self(self._conn_ptr, self._sql, relation_alias)

    # ── Transforms (each returns a new lazy relation) ──────────────

    def select(self, *cols: String) -> Self:
        """Project the given columns/expressions (Python ``select``/``project``).

        Each argument is a raw SQL fragment; build them with `col`, `lit`,
        `fn_`, or plain strings. Pass one comma-separated string or several
        fragments.
        """
        var proj = String("")
        for i in range(len(cols)):
            if i > 0:
                proj += ", "
            proj += cols[i]
        if len(cols) == 0:
            proj = "*"
        return self._wrap(String("SELECT ", proj, " FROM ", self._from()))

    def project(self, *cols: String) -> Self:
        """Alias for `select` (same projection semantics)."""
        var proj = String("")
        for i in range(len(cols)):
            if i > 0:
                proj += ", "
            proj += cols[i]
        if len(cols) == 0:
            proj = "*"
        return self._wrap(String("SELECT ", proj, " FROM ", self._from()))

    def filter(self, predicate: String) -> Self:
        """Keep rows matching ``predicate`` (Python ``filter``)."""
        return self._wrap(
            String("SELECT * FROM ", self._from(), " WHERE ", predicate)
        )

    def order(self, expr: String) -> Self:
        """Order by ``expr`` (Python ``order``)."""
        return self._wrap(
            String("SELECT * FROM ", self._from(), " ORDER BY ", expr)
        )

    def sort(self, expr: String) -> Self:
        """Alias for `order`."""
        return self.order(expr)

    def limit(self, n: Int, offset: Int = 0) -> Self:
        """Keep at most ``n`` rows, skipping ``offset`` (Python ``limit``)."""
        var s = String("SELECT * FROM ", self._from(), " LIMIT ", n)
        if offset > 0:
            s += String(" OFFSET ", offset)
        return self._wrap(s)

    def distinct(self) -> Self:
        """Remove duplicate rows (Python ``distinct``)."""
        return self._wrap(String("SELECT DISTINCT * FROM ", self._from()))

    def aggregate(self, aggr: String, group: String = "") -> Self:
        """Aggregate with ``aggr`` projection, optionally grouped by ``group``.

        ``rel.aggregate("k, sum(v) AS s", group="k")`` →
        ``SELECT k, sum(v) AS s FROM (...) GROUP BY k``.
        """
        var s = String("SELECT ", aggr, " FROM ", self._from())
        if group.byte_length() != 0:
            s += String(" GROUP BY ", group)
        return self._wrap(s)

    # ── Joins / set operations ─────────────────────────────────────

    def join[
        o2: ImmOrigin
    ](self, other: Relation[o2], on: String, how: String = "inner") -> Self:
        """Join with ``other`` on ``on`` (Python ``join``).

        ``how`` is ``inner``/``left``/``right``/``outer``/``semi``/``anti``.
        Set aliases via `set_alias` first so ``on`` can qualify columns.
        """
        return self._wrap(
            String(
                "SELECT * FROM ",
                self._from(),
                " ",
                how,
                " JOIN ",
                other._from(),
                " ON ",
                on,
            )
        )

    def cross[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        """Cartesian product with ``other`` (Python ``cross``)."""
        return self._wrap(
            String(
                "SELECT * FROM ", self._from(), " CROSS JOIN ", other._from()
            )
        )

    def union[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        """``UNION`` (deduplicated) with ``other`` (Python ``union``)."""
        return Self(
            self._conn_ptr, String("(", self._sql, ") UNION (", other._sql, ")")
        )

    def union_all[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        """``UNION ALL`` (keeps duplicates) with ``other``."""
        return Self(
            self._conn_ptr,
            String("(", self._sql, ") UNION ALL (", other._sql, ")"),
        )

    def except_[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        """Rows in this relation but not ``other`` (Python ``except_``)."""
        return Self(
            self._conn_ptr, String("(", self._sql, ") EXCEPT (", other._sql, ")")
        )

    def intersect[o2: ImmOrigin](self, other: Relation[o2]) -> Self:
        """Rows in both this relation and ``other`` (Python ``intersect``)."""
        return Self(
            self._conn_ptr,
            String("(", self._sql, ") INTERSECT (", other._sql, ")"),
        )

    # ── Aggregate shorthands ───────────────────────────────────────

    def _agg(self, fn_name: String, column: String, group: String) -> Self:
        var proj = String(fn_name, "(", column, ")")
        if group.byte_length() != 0:
            return self._wrap(
                String(
                    "SELECT ",
                    group,
                    ", ",
                    proj,
                    " FROM ",
                    self._from(),
                    " GROUP BY ",
                    group,
                )
            )
        return self._wrap(String("SELECT ", proj, " FROM ", self._from()))

    def sum(self, column: String, group: String = "") -> Self:
        """``sum(column)`` (Python ``sum``)."""
        return self._agg("sum", column, group)

    def avg(self, column: String, group: String = "") -> Self:
        """``avg(column)`` (Python ``avg``/``mean``)."""
        return self._agg("avg", column, group)

    def mean(self, column: String, group: String = "") -> Self:
        """Alias for `avg`."""
        return self._agg("avg", column, group)

    def min(self, column: String, group: String = "") -> Self:
        """``min(column)`` (Python ``min``)."""
        return self._agg("min", column, group)

    def max(self, column: String, group: String = "") -> Self:
        """``max(column)`` (Python ``max``)."""
        return self._agg("max", column, group)

    def median(self, column: String, group: String = "") -> Self:
        """``median(column)`` (Python ``median``)."""
        return self._agg("median", column, group)

    def stddev(self, column: String, group: String = "") -> Self:
        """``stddev(column)`` (Python ``std``/``stddev``)."""
        return self._agg("stddev", column, group)

    def var(self, column: String, group: String = "") -> Self:
        """``var_samp(column)`` (Python ``var``/``variance``)."""
        return self._agg("var_samp", column, group)

    def count(self, column: String = "*", group: String = "") -> Self:
        """``count(column)`` (default ``count(*)``) (Python ``count``)."""
        return self._agg("count", column, group)

    # ── Terminals (execute) ────────────────────────────────────────

    def to_result(self) raises ResultError -> Result:
        """Execute and return a streaming `Result` (also `execute`).

        Iterate the returned result for rows:
        ``for row in rel.to_result(): ...``
        """
        return self._run(self._sql)

    def execute(self) raises ResultError -> Result:
        """Execute and return a `Result` (alias for `to_result`)."""
        return self._run(self._sql)

    def fetchall(self) raises -> MaterializedResult:
        """Execute and fetch all rows into memory (Python ``fetchall``)."""
        return self._run(self._sql).fetchall()

    def fetchone[
        *Ts: Copyable & Deinitable
    ](self) raises -> Optional[Tuple[*Ts]]:
        """Execute and fetch the first row as a typed tuple, or ``None``."""
        var r = self._run(self._sql)
        return r.fetchone[*Ts]()

    def fetchmany[
        *Ts: Copyable & Deinitable
    ](self, size: Int = 1) raises -> List[Tuple[*Ts]]:
        """Execute and fetch up to ``size`` rows as typed tuples."""
        var r = self._run(self._sql)
        return r.fetchmany[*Ts](size)

    def get[
        T: Copyable & Deinitable
    ](self) raises -> List[T]:
        """Execute and decode all rows into ``List[T]`` (struct/scalar)."""
        return self._run(self._sql).fetchall().get[T]()

    def show(self, *, max_rows: Int = 40, max_col_width: Int = 32) raises:
        """Execute and print a formatted table (Python ``show``)."""
        self._run(self._sql).show(max_rows=max_rows, max_col_width=max_col_width)

    def columns(self) raises -> List[String]:
        """Column names without fetching rows (Python ``columns``)."""
        var q = String("SELECT * FROM ", self._from(), " LIMIT 0")
        return self._run(q).columns()

    def types(
        self,
    ) raises -> List[LogicalType[is_owned=True, origin=MutUntrackedOrigin]]:
        """Column logical types without fetching rows (Python ``dtypes``)."""
        var q = String("SELECT * FROM ", self._from(), " LIMIT 0")
        return self._run(q).column_types()

    def shape(self) raises -> Tuple[Int, Int]:
        """``(row_count, column_count)`` (Python ``shape``).

        Runs a ``count(*)`` rather than materializing all rows.
        """
        var n = self.aggregate("count(*) AS n").fetchone[Int64]()
        var rows = Int(n.value()[0]) if n else 0
        return (rows, len(self.columns()))

    def describe(self) -> Self:
        """Summary statistics per column as a relation (Python ``describe``)."""
        return self._wrap(String("DESCRIBE ", self._sql))

    def explain(self) raises -> String:
        """The query plan as text (Python ``explain``)."""
        var query = String("EXPLAIN ", self._sql)
        var res = self._run(query)
        var out = String("")
        var first = True
        for row in res:
            if not first:
                out += "\n"
            out += row.get[String](col=1)
            first = False
        return out^

    def create(self, table: String) raises:
        """Persist as a new table ``CREATE TABLE table AS <sql>`` (Python ``create``)."""
        var q = String("CREATE TABLE ", _quote_qualified(table), " AS ", self._sql)
        _ = self._run(q)

    def to_table(self, table: String) raises:
        """Alias for `create`."""
        self.create(table)

    def create_view(self, name: String, replace: Bool = True) raises -> Self:
        """Persist as a view and return a relation over it (Python ``create_view``)."""
        var or_replace = String("OR REPLACE ") if replace else String("")
        var q = String(
            "CREATE ", or_replace, "VIEW ", _quote_qualified(name), " AS ", self._sql
        )
        _ = self._run(q)
        return Self(
            self._conn_ptr, String("SELECT * FROM ", _quote_qualified(name))
        )

    def insert_into(self, table: String) raises:
        """Append rows into an existing table (Python ``insert_into``)."""
        var q = String("INSERT INTO ", _quote_qualified(table), " ", self._sql)
        _ = self._run(q)

    def to_parquet(self, path: String) raises:
        """Write to a Parquet file (Python ``to_parquet``/``write_parquet``)."""
        var q = String(
            "COPY (", self._sql, ") TO ", _quote_literal(path), " (FORMAT parquet)"
        )
        _ = self._run(q)

    def to_csv(self, path: String, *, header: Bool = True) raises:
        """Write to a CSV file (Python ``to_csv``/``write_csv``)."""
        var hdr = String("true") if header else String("false")
        var q = String(
            "COPY (", self._sql, ") TO ", _quote_literal(path),
            " (FORMAT csv, HEADER ", hdr, ")",
        )
        _ = self._run(q)

    # ── Display ────────────────────────────────────────────────────

    def write_to[W: Writer](self, mut writer: W):
        """Render as a formatted table for ``print`` / ``String(...)``.

        Executes the query; on error the message is written into the output
        (``write_to`` cannot raise).
        """
        try:
            var rendered = (
                self._run(self._sql)
                .fetchall()
                ._render_table(max_rows=40, max_col_width=32)
            )
            writer.write(rendered)
        except e:
            writer.write("<relation error: ", String(e), ">")
