"""Prepared statements and parameter binding for DuckDB.

A `PreparedStatement` wraps a parsed-and-planned SQL statement that can be
executed repeatedly with different parameter bindings.  This mirrors DuckDB's
Python ``con.execute(sql, params)`` / ``con.executemany(sql, seq)`` ergonomics,
but with statically-typed Mojo values.

Parameters can be bound positionally (``?`` or ``$1``) or by name (``$name``).

Example — positional binding via the connection convenience API:
```mojo
var con = DuckDB.connect(":memory:")
_ = con.execute("CREATE TABLE t (id INTEGER, name VARCHAR)")
_ = con.execute("INSERT INTO t VALUES (?, ?)", 1, String("Mark"))
var result = con.execute("SELECT name FROM t WHERE id = ?", 1)
```

Example — preparing once and binding manually:
```mojo
var stmt = con.prepare("SELECT $1 + $2")
stmt.bind(1, Int32(40))
stmt.bind(2, Int32(2))
var result = stmt.execute()
```

Example — named parameters:
```mojo
var result = con.execute_named(
    "SELECT $x + $y", {"x": Int32(40), "y": Int32(2)}
)
```
"""

from std.builtin.rebind import trait_downcast, downcast
from std.collections import Optional
from duckdb._libduckdb import *
from duckdb.api import DuckDB
from duckdb.typed_api import _to_duckdb_value
from duckdb.value import DuckDBValue
from duckdb.result import Result, ResultError, ErrorType, StatementType


trait Bindable:
    """Trait for types needing custom prepared-statement binding logic.

    Most scalar types are bound generically via `_to_duckdb_value`.  This trait
    exists for types that need special handling — currently `Optional[T]`, which
    binds NULL for `None` and its inner value otherwise.
    """

    def bind_to(ref self, mut stmt: PreparedStatement, index: Int) raises ResultError:
        """Bind this value to ``stmt`` at the given 1-based ``index``."""
        ...


struct PreparedStatement(Movable):
    """A prepared SQL statement with bindable parameters.

    Create one via `Connection.prepare`.  The statement borrows the connection's
    handle and must not outlive the connection (same contract as `Appender`).

    Parameters are 1-based, matching DuckDB's C API and SQL ``$1``/``?`` indices.
    """

    var _stmt: duckdb_prepared_statement

    # ── Construction / Destruction ────────────────────────────────

    def __init__(out self, conn: duckdb_connection, query: String) raises ResultError:
        """Prepare ``query`` against the given raw connection handle.

        Prefer `Connection.prepare`, which supplies the handle for you.
        """
        self._stmt = duckdb_prepared_statement.unsafe_dangling()
        var _query = query.copy()
        ref libduckdb = DuckDB().libduckdb()
        var state = libduckdb.duckdb_prepare(
            conn,
            _query.as_c_string_slice().unsafe_ptr(),
            UnsafePointer(to=self._stmt),
        )
        if state == DuckDBError:
            var err = self._error_str()
            # Per the C API, the statement must be destroyed even on failure.
            libduckdb.duckdb_destroy_prepare(UnsafePointer(to=self._stmt))
            raise ResultError(err^, ErrorType.INVALID)

    def __init__(out self, *, deinit take: Self):
        self._stmt = take._stmt

    def __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_prepare(UnsafePointer(to=self._stmt))

    # ── Error handling ────────────────────────────────────────────

    def _error_str(self) -> String:
        """Return the current prepare error message, or empty string."""
        ref libduckdb = DuckDB().libduckdb()
        var p = libduckdb.duckdb_prepare_error(self._stmt)
        if Int(p) == 0:
            return String("")
        return String(unsafe_from_utf8_ptr=p)

    def _check(self, state: duckdb_state, context: String) raises ResultError:
        """Raise a `ResultError` if ``state`` indicates failure."""
        if state == DuckDBError:
            var err = self._error_str()
            if err.byte_length() == 0:
                err = context.copy()
            raise ResultError(err^, ErrorType.INVALID)

    # ── Introspection ─────────────────────────────────────────────

    def parameter_count(self) -> Int:
        """The number of parameters expected by this statement."""
        ref libduckdb = DuckDB().libduckdb()
        return Int(libduckdb.duckdb_nparams(self._stmt))

    def parameter_name(self, index: Int) raises ResultError -> String:
        """The name of the parameter at the given 1-based ``index``."""
        ref libduckdb = DuckDB().libduckdb()
        var p = libduckdb.duckdb_parameter_name(self._stmt, idx_t(index))
        if Int(p) == 0:
            raise ResultError(
                String("Parameter index out of range: ", index),
                ErrorType.OUT_OF_RANGE,
            )
        var name = String(unsafe_from_utf8_ptr=p)
        libduckdb.duckdb_free(
            UnsafePointer[NoneType, MutAnyOrigin](unsafe_from_address=Int(p))
        )
        return name^

    def parameter_index(self, name: String) raises ResultError -> Int:
        """Resolve a parameter ``name`` (e.g. ``"x"`` for ``$x``) to its 1-based index."""
        ref libduckdb = DuckDB().libduckdb()
        var out_idx = idx_t(0)
        var _name = name.copy()
        var state = libduckdb.duckdb_bind_parameter_index(
            self._stmt,
            UnsafePointer(to=out_idx),
            _name.as_c_string_slice().unsafe_ptr(),
        )
        if state == DuckDBError:
            raise ResultError(
                String("Unknown parameter name: ", name), ErrorType.INVALID
            )
        return Int(out_idx)

    def statement_type(self) -> StatementType:
        """The kind of statement (SELECT, INSERT, ...)."""
        ref libduckdb = DuckDB().libduckdb()
        return StatementType(libduckdb.duckdb_prepared_statement_type(self._stmt))

    # ── Binding ───────────────────────────────────────────────────

    def clear_bindings(mut self) raises ResultError:
        """Clear all currently bound parameters."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(
            libduckdb.duckdb_clear_bindings(self._stmt), String("clear_bindings failed")
        )

    def bind_null(mut self, index: Int) raises ResultError:
        """Bind SQL NULL at the given 1-based ``index``."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(
            libduckdb.duckdb_bind_null(self._stmt, idx_t(index)),
            String("bind_null failed"),
        )

    def bind_value(mut self, index: Int, ref value: DuckDBValue) raises ResultError:
        """Bind a pre-built `DuckDBValue` at the given 1-based ``index``."""
        ref libduckdb = DuckDB().libduckdb()
        self._check(
            libduckdb.duckdb_bind_value(self._stmt, idx_t(index), value._value),
            String("bind_value failed"),
        )

    def bind[T: Copyable & Movable](mut self, index: Int, value: T) raises ResultError:
        """Bind a typed Mojo ``value`` at the given 1-based ``index``.

        Scalars (Bool, integers, floats, String, temporal types, ...) are bound
        directly.  `Optional[T]` binds NULL for `None`.

        Parameters:
            T: The Mojo type of the value.

        Args:
            index: The 1-based parameter index.
            value: The value to bind.
        """
        comptime if conforms_to(T, Bindable):
            trait_downcast[Bindable](value).bind_to(self, index)
        else:
            ref libduckdb = DuckDB().libduckdb()
            var raw: duckdb_value
            try:
                raw = _to_duckdb_value(value)
            except e:
                raise ResultError(String(e), ErrorType.INVALID)
            var state = libduckdb.duckdb_bind_value(self._stmt, idx_t(index), raw)
            libduckdb.duckdb_destroy_value(UnsafePointer(to=raw))
            self._check(state, String("bind failed"))

    # ── Execution ─────────────────────────────────────────────────

    def execute(mut self) raises ResultError -> Result:
        """Execute the statement with the currently bound parameters."""
        var result = duckdb_result()
        var result_ptr = UnsafePointer(to=result)
        ref libduckdb = DuckDB().libduckdb()
        var state = libduckdb.duckdb_execute_prepared(self._stmt, result_ptr)
        if state == DuckDBError:
            var error_msg = String(
                unsafe_from_utf8_ptr=libduckdb.duckdb_result_error(result_ptr)
            )
            var error_type_value = libduckdb.duckdb_result_error_type(result_ptr)
            libduckdb.duckdb_destroy_result(result_ptr)
            raise ResultError(error_msg^, ErrorType(error_type_value))
        return Result(result)


# ──────────────────────────────────────────────────────────────────
# Bindable extensions
# ──────────────────────────────────────────────────────────────────


__extension Optional(Bindable):
    def bind_to(ref self, mut stmt: PreparedStatement, index: Int) raises ResultError:
        if self:
            # Refine Self.T to Copyable & Movable so the inner value can be
            # bound — only Copyable values can be converted to a duckdb_value.
            comptime CT = downcast[Self.T, Copyable & Movable]
            var vp = UnsafePointer(to=self).bitcast[Optional[CT]]()
            stmt.bind(index, vp[].value())
        else:
            stmt.bind_null(index)
