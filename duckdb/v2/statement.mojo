"""Statement parameters, prepared statements and statement descriptions."""

from std.memory import ArcPointer
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb.v2._runtime import api_error, check, lib
from duckdb.v2.chunk import _read_schema
from duckdb.v2.error import DuckDBError
from duckdb.v2.mapping import mapped_to_value
from duckdb.v2.result import Column, Result, _columns
from duckdb.v2.value import Value


struct Params(Movable):
    """Named parameters for `execute(sql, params=...)`.

    ```mojo
    var p = Params()
    p.set("min_age", Int32(30))
    p.set("city", String("Amsterdam"))
    var r = con.execute("SELECT * FROM users WHERE age >= $min_age AND city = $city", params=p)
    ```

    Names match `$name` placeholders case-insensitively.
    """

    var names: List[String]
    var values: List[Value]

    def __init__(out self):
        self.names = List[String]()
        self.values = List[Value]()

    def set[T: Movable & Deinitable](mut self, name: String, value: T) raises DuckDBError:
        self.names.append(name)
        self.values.append(Value(value))

    def set_value(mut self, name: String, var value: Value):
        """A parameter from a ready-made `Value` (a typed NULL, for example)."""
        self.names.append(name)
        self.values.append(value^)

    def __len__(self) -> Int:
        return len(self.values)

    def _into_arguments(deinit self) -> _Arguments:
        return _Arguments(self.values^, self.names^)


struct _Arguments(Movable):
    """Parameter values and (optional) names as the C arrays V2 functions take.

    Owns everything the arrays point into, so pass it by reference and keep
    it alive until the call has returned.
    """

    var values: List[Value]
    var names: List[StrArg]
    var _value_handles: List[duckdb_v2_value_handle]
    var _name_views: List[duckdb_v2_identifier_t]

    def __init__(out self, var values: List[Value], var names: List[String]):
        self.values = values^
        self.names = List[StrArg]()
        for n in names:
            self.names.append(StrArg(n))
        self._value_handles = List[duckdb_v2_value_handle]()
        for i in range(len(self.values)):
            self._value_handles.append(self.values[i].handle())
        self._name_views = List[duckdb_v2_identifier_t]()
        for i in range(len(self.names)):
            self._name_views.append(self.names[i].ptr()[].copy())

    def count(self) -> idx_t:
        return idx_t(len(self._value_handles))

    def values_ptr(ref self) -> Pointer[duckdb_v2_value_handle, ImmUntrackedOrigin]:
        if len(self._value_handles) == 0:
            return null[Pointer[duckdb_v2_value_handle, ImmUntrackedOrigin]]()
        return self._value_handles.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]()

    def names_ptr(ref self) -> Pointer[duckdb_v2_identifier_t, ImmUntrackedOrigin]:
        if len(self._name_views) == 0:
            return null[Pointer[duckdb_v2_identifier_t, ImmUntrackedOrigin]]()
        return self._name_views.unsafe_ptr().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]()


def _positional[*Ts: Movable & Deinitable](factory: duckdb_v2_factory_handle, *args: *Ts) raises DuckDBError -> _Arguments:
    var values = List[Value]()
    comptime for i in range(args.__len__()):
        values.append(Value(_owned=mapped_to_value(args[i], factory)))
    return _Arguments(values^, List[String]())


def _named(var params: Params) -> _Arguments:
    return params^._into_arguments()


struct PreparedStatement(Movable):
    """A statement planned once and executed many times.

    ```mojo
    var stmt = con.prepare("SELECT name FROM users WHERE id = $1")
    var a = stmt.execute(Int64(1)).fetchall()
    var b = stmt.execute(Int64(2)).fetchall()
    ```

    It stays usable after its connection is gone (DuckDB keeps the session
    alive), but like any statement it cannot run while another result of the
    connection is still being read.
    """

    var _handle: duckdb_v2_prepared_statement_handle
    var _factory: duckdb_v2_factory_handle

    def __init__(out self, *, var _owned: duckdb_v2_prepared_statement_handle, factory: duckdb_v2_factory_handle):
        self._handle = _owned
        self._factory = factory

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle
        self._factory = move._factory

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_prepared_statement_destroy(Pointer(to=self._handle))

    def execute[*Ts: Movable & Deinitable](mut self, *args: *Ts) raises DuckDBError -> Result:
        """Execute with positional parameters (`$1`, `$2`, ... or `?`)."""
        var arguments = _positional(self._factory, *args)
        return self._execute(arguments)

    def execute(mut self, *, var params: Params) raises DuckDBError -> Result:
        """Execute with named parameters (`$name`)."""
        var arguments = _named(params^)
        return self._execute(arguments)

    def _execute(mut self, ref arguments: _Arguments) raises DuckDBError -> Result:
        var result = null[duckdb_v2_result_handle]()
        var err = null_error_info()
        check(
            lib().duckdb_v2_prepared_statement_execute(
                self._handle, arguments.names_ptr(), arguments.values_ptr(), arguments.count(), Pointer(to=result), Pointer(to=err)
            ),
            err,
        )
        return Result(_owned=result)

    def reuses_plan(self) raises DuckDBError -> Bool:
        """Whether executions reuse the plan built by `prepare`.

        False for statements that read base tables (they re-bind to see
        catalog changes) and for parameters whose types are only known at
        execution (`SELECT $1 + $2`).
        """
        var out = False
        var err = null_error_info()
        check(lib().duckdb_v2_prepared_statement_reuses_plan(self._handle, Pointer(to=out), Pointer(to=err)), err)
        return out


@fieldwise_init
struct Description(Movable):
    """What a statement returns and takes, from `Connection.describe`."""

    var columns: List[Column]
    """The result columns. Statements without rows report one status column."""
    var parameters: List[Column]
    """The parameters, in binding order. Names are `"1"`, `"2"`, ... for positional ones."""
