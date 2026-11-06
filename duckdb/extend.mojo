"""More ways to extend DuckDB: casts, replacement scans, macros, pragmas and
custom options. Register them like functions, on a `Connection` or an
`Extension`.

```mojo
def celsius_to_text(c: Float64) -> String:
    return String(c, " °C")

con.register(CastFunction.from_function[celsius_to_text]())

def files(name: String) raises -> Optional[Replacement]:
    if name.endswith(".log"):
        return Replacement.function("read_csv", [Value(name)])
    return None

con.register(ReplacementScan.from_function[files]())     # SELECT * FROM 'app.log'
con.register(MacroFunction.scalar("add_one", ["x"], "x + 1"))
con.register(CustomOption.of("my_ext_level", Int64(1), "how verbose my_ext is"))
```
"""

from duckdb._libduckdb_v2 import *
from duckdb._sql_util import _quote_ident, _quote_literal
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb._runtime import api_error, check, lib
from duckdb.error import DuckDBError
from duckdb.functions import BindArgs, _Args, _identity_user_data, _report, _required, _valid
from duckdb.serde import VectorWriter, logical_type_of
from duckdb.signature import ParamKind, Signature
from duckdb.types import LogicalType, TypeId
from duckdb.value import Value
from duckdb.settings import SettingScope


# ===--------------------------------------------------------------------===#
# Casts
# ===--------------------------------------------------------------------===#


struct CastFunction(Movable):
    """A cast from one type to another, written as a Mojo function.

    In `TRY_CAST` a value the function raises for becomes NULL. In `CAST` the
    error fails the query. NULL casts to NULL without calling the function.
    """

    var source: LogicalType
    var target: LogicalType
    var implicit_cost: Int
    var _exec: duckdb_v2_cast_function_exec_callback_fn

    @staticmethod
    def from_function[
        S: Movable & Deinitable, T: Movable & Deinitable, //, f: def(S) raises thin -> T
    ](
        *,
        implicit_cost: Int = -1,
        source: Optional[LogicalType] = None,
        target: Optional[LogicalType] = None,
    ) raises DuckDBError -> CastFunction:
        """A cast from the DuckDB type of `S` to that of `T` (or the given types).

        `implicit_cost` >= 0 lets DuckDB apply the cast implicitly, lower is
        preferred (built-in widening casts cost 0 to 20). -1 means only
        explicit `CAST`/`TRY_CAST`.
        """
        return CastFunction(
            source.value().copy() if source else logical_type_of[S](),
            target.value().copy() if target else logical_type_of[T](),
            implicit_cost,
            _cast_cb[S, T, f],
        )

    def __init__(
        out self, var source: LogicalType, var target: LogicalType, implicit_cost: Int, exec: duckdb_v2_cast_function_exec_callback_fn
    ):
        self.source = source^
        self.target = target^
        self.implicit_cost = implicit_cost
        self._exec = exec

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_cast_function_handle:
        var err = null_error_info()
        var function = null[duckdb_v2_cast_function_handle]()
        check(lib().duckdb_v2_cast_function_create(factory, Pointer(to=function), Pointer(to=err)), err)
        try:
            check(lib().duckdb_v2_cast_function_set_source_type(function, self.source.handle(), Pointer(to=err)), err)
            check(lib().duckdb_v2_cast_function_set_target_type(function, self.target.handle(), Pointer(to=err)), err)
            check(lib().duckdb_v2_cast_function_set_implicit_cast_cost(function, Int64(self.implicit_cost), Pointer(to=err)), err)
            check(lib().duckdb_v2_cast_function_set_exec_callback(function, self._exec, Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_cast_function_destroy(Pointer(to=function))
            raise e^
        return function


def _cast_cb[S: Movable & Deinitable, T: Movable & Deinitable, f: def(S) raises thin -> T](
    info: duckdb_v2_cast_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_cast[S, T, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_TYPE_CONVERSION)


def _run_cast[S: Movable & Deinitable, T: Movable & Deinitable, f: def(S) raises thin -> T](
    info: duckdb_v2_cast_function_exec_info_handle,
) raises:
    var e = null_error_info()
    var count: idx_t = 0
    check(lib().duckdb_v2_cast_function_exec_get_row_count(info, Pointer(to=count), Pointer(to=e)), e)
    var input = null[duckdb_v2_vector_handle]()
    check(lib().duckdb_v2_cast_function_exec_get_input(info, Pointer(to=input), Pointer(to=e)), e)
    var output = null[duckdb_v2_vector_handle]()
    check(lib().duckdb_v2_cast_function_exec_get_output(info, Pointer(to=output), Pointer(to=e)), e)
    var mode: DUCKDB_V2_CAST_MODE = DUCKDB_V2_CAST_MODE_NORMAL
    check(lib().duckdb_v2_cast_function_exec_get_mode(info, Pointer(to=mode), Pointer(to=e)), e)
    var n = Int(count)
    var args = _Args([input], n)
    var out = VectorWriter(output, n)
    var v = args.view(0)
    for row in range(n):
        if not _valid(v, row):
            out.write_null(row)
            continue
        if mode == DUCKDB_V2_CAST_MODE_TRY:
            var result = _try_call[S, T, f](args.get[S](0, v, row))
            if result:
                out.write(row, result.take())
            else:
                out.write_null(row)
        else:
            out.write(row, f(args.get[S](0, v, row)))
    out.finish()


def _try_call[S: Movable & Deinitable, T: Movable & Deinitable, f: def(S) raises thin -> T](x: S) -> Optional[T]:
    # Its own function: one `try` cannot mix the untyped error of `f` with typed ones.
    try:
        return f(x)
    except:
        return None


# ===--------------------------------------------------------------------===#
# Replacement scans
# ===--------------------------------------------------------------------===#


struct Replacement(Movable):
    """What a replacement scan reads instead of a table name it claims."""

    var _function: String
    var args: List[Value]
    var named_args: List[Tuple[String, Value]]
    var subquery: String
    var alias: String

    def __init__(out self):
        self._function = String()
        self.args = List[Value]()
        self.named_args = List[Tuple[String, Value]]()
        self.subquery = String()
        self.alias = String()

    @staticmethod
    def function(name: String, var args: List[Value] = []) -> Self:
        """Read table function `name` with positional arguments (`read_csv('x.log')`)."""
        var r = Self()
        r._function = name
        r.args = args^
        return r^

    @staticmethod
    def query(sql: String) -> Self:
        """Read the result of a SELECT statement."""
        var r = Self()
        r.subquery = sql
        return r^

    def named(var self, name: String, var value: Value) -> Self:
        """Add a named argument to the table function call."""
        self.named_args.append((name, value^))
        return self^

    def with_alias(var self, alias: String) -> Self:
        """The name the replaced table has in the query (default: the name it replaced)."""
        self.alias = alias
        return self^


struct ReplacementScan(Movable):
    """Resolves table names DuckDB cannot find in the catalog
    (`SELECT * FROM 'data.xyz'`).

    The function gets the name as written (the path, for a quoted file name,
    `schema.table` for a qualified name) and returns a `Replacement`, or
    `None` to leave the name to other scans and the usual error.
    """

    var _callback: duckdb_v2_replacement_scan_callback_fn

    @staticmethod
    def from_function[f: def(name: String) raises thin -> Optional[Replacement]]() -> ReplacementScan:
        return ReplacementScan(_replacement_cb[f])

    def __init__(out self, callback: duckdb_v2_replacement_scan_callback_fn):
        self._callback = callback

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_replacement_scan_handle:
        var err = null_error_info()
        var scan = null[duckdb_v2_replacement_scan_handle]()
        check(lib().duckdb_v2_replacement_scan_create(factory, Pointer(to=scan), Pointer(to=err)), err)
        var code = lib().duckdb_v2_replacement_scan_set_callback(scan, self._callback, Pointer(to=err))
        if code != DUCKDB_V2_ERROR_NONE:
            _ = lib().duckdb_v2_replacement_scan_destroy(Pointer(to=scan))
            check(code, err)
        return scan


def _qname_text(name: duckdb_v2_qname_handle) raises DuckDBError -> String:
    var count: idx_t = 0
    var err = null_error_info()
    check(lib().duckdb_v2_qname_get_part_count(name, Pointer(to=count), Pointer(to=err)), err)
    var parts = List[String]()
    for i in range(Int(count)):
        var part = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        check(lib().duckdb_v2_qname_get_part(name, idx_t(i), Pointer(to=part), Pointer(to=err)), err)
        parts.append(str_to_string(part))
    return String(".").join(parts)


def _qname(text: String) raises DuckDBError -> duckdb_v2_qname_handle:
    """A qualified name from dot-separated parts (`schema.function`)."""
    var args = List[StrArg]()
    for part in text.split("."):
        args.append(StrArg(part))
    var views = List[duckdb_v2_identifier_t]()
    for i in range(len(args)):
        views.append(args[i].ptr()[].copy())
    var name = null[duckdb_v2_qname_handle]()
    var err = null_error_info()
    var code = lib().duckdb_v2_qname_create(views.unsafe_ptr().as_imm(), idx_t(len(views)), Pointer(to=name), Pointer(to=err))
    _ = views^
    _ = args^
    check(code, err)
    return name


def _replacement_cb[f: def(name: String) raises thin -> Optional[Replacement]](
    info: duckdb_v2_replacement_scan_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_replacement[f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _run_replacement[f: def(name: String) raises thin -> Optional[Replacement]](
    info: duckdb_v2_replacement_scan_info_handle,
) raises:
    var e = null_error_info()
    var qname = null[duckdb_v2_qname_handle]()
    check(lib().duckdb_v2_replacement_scan_get_name(info, Pointer(to=qname), Pointer(to=e)), e)
    var name: String
    try:
        name = _qname_text(qname)
    finally:
        _ = lib().duckdb_v2_qname_destroy(Pointer(to=qname))
    var replacement = f(name)
    if not replacement:
        return
    ref r = replacement.value()
    if r.subquery:
        var sql = StrArg(r.subquery)
        check(lib().duckdb_v2_replacement_scan_set_subquery(info, sql.ptr(), Pointer(to=e)), e)
    else:
        var fname = _qname(r._function)
        try:
            check(lib().duckdb_v2_replacement_scan_set_function_name(info, fname, Pointer(to=e)), e)
        finally:
            _ = lib().duckdb_v2_qname_destroy(Pointer(to=fname))
        for i in range(len(r.args)):
            check(lib().duckdb_v2_replacement_scan_add_argument(info, r.args[i].handle(), Pointer(to=e)), e)
        for i in range(len(r.named_args)):
            var n = StrArg(r.named_args[i][0])
            check(
                lib().duckdb_v2_replacement_scan_add_named_argument(info, n.ptr(), r.named_args[i][1].handle(), Pointer(to=e)), e
            )
    if r.alias:
        var a = StrArg(r.alias)
        check(lib().duckdb_v2_replacement_scan_set_alias(info, a.ptr(), Pointer(to=e)), e)


# ===--------------------------------------------------------------------===#
# Macros
# ===--------------------------------------------------------------------===#


struct MacroFunction(Movable):
    """A SQL macro: a function whose body is SQL.

    In an extension it is registered through the V2 API. V2 cannot register
    macros on a connection, so there it is created with
    `CREATE TEMPORARY MACRO`, which lives as long as the connection.
    """

    var name: String
    var table: Bool
    var params: List[String]
    var defaults: List[Tuple[String, Value]]
    var body: String

    @staticmethod
    def scalar(name: String, params: List[String], body: String) -> MacroFunction:
        """A scalar macro: `body` is one SQL expression over the parameters (`x + 1`)."""
        return MacroFunction(name, False, params.copy(), body)

    @staticmethod
    def table_macro(name: String, params: List[String], body: String) -> MacroFunction:
        """A table macro: `body` is one SELECT query over the parameters."""
        return MacroFunction(name, True, params.copy(), body)

    def __init__(out self, name: String, table: Bool, var params: List[String], body: String):
        self.name = name
        self.table = table
        self.params = params^
        self.defaults = List[Tuple[String, Value]]()
        self.body = body

    def default[T: Movable & Deinitable](var self, name: String, value: T) raises DuckDBError -> Self:
        """A default for parameter `name`, which callers may then omit."""
        self.defaults.append((name, Value(value)))
        return self^

    def _default_of(self, name: String) -> Int:
        for i in range(len(self.defaults)):
            if self.defaults[i][0].lower() == name.lower():
                return i
        return -1

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_macro_function_handle:
        var err = null_error_info()
        var function = null[duckdb_v2_macro_function_handle]()
        var kind = DUCKDB_V2_MACRO_FUNCTION_TYPE_TABLE if self.table else DUCKDB_V2_MACRO_FUNCTION_TYPE_SCALAR
        check(lib().duckdb_v2_macro_function_create(factory, kind, Pointer(to=function), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_macro_function_set_name(function, name.ptr(), Pointer(to=err)), err)
            var body = StrArg(self.body)
            check(lib().duckdb_v2_macro_function_set_body(function, body.ptr(), Pointer(to=err)), err)
            var signature = null[duckdb_v2_function_signature_handle]()
            check(lib().duckdb_v2_macro_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
            var any = LogicalType(TypeId.ANY)
            for i in range(len(self.params)):
                var p = StrArg(self.params[i])
                var d = self._default_of(self.params[i])
                var default = null[duckdb_v2_value_handle]()
                if d >= 0:
                    default = self.defaults[d][1].handle()
                check(
                    lib().duckdb_v2_function_signature_add_parameter(
                        signature, p.ptr(), any.handle(), default, DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD, Pointer(to=err)
                    ),
                    err,
                )
        except e:
            _ = lib().duckdb_v2_macro_function_destroy(Pointer(to=function))
            raise e^
        return function

    def _create_sql(self) raises DuckDBError -> String:
        """The `CREATE TEMPORARY MACRO` statement for connections."""
        var params = List[String]()
        for i in range(len(self.params)):
            var p = _quote_ident(self.params[i])
            var d = self._default_of(self.params[i])
            if d >= 0:
                ref v = self.defaults[d][1]
                if v.is_null():
                    # `to_string` spells NULL as the word. Quoting it would cast a
                    # string. Emit a typed NULL instead (the V2 path sends the
                    # NULL value itself).
                    p += String(" := CAST(NULL AS ", v.type().to_text(), ")")
                else:
                    p += String(" := CAST(", _quote_literal(v.to_string()), " AS ", v.type().to_text(), ")")
            params.append(p^)
        var kind = "TABLE " if self.table else ""
        return String(
            "CREATE TEMPORARY MACRO ", _quote_ident(self.name), "(", String(", ").join(params), ") AS ", kind, self.body
        )


# ===--------------------------------------------------------------------===#
# Pragmas
# ===--------------------------------------------------------------------===#


struct PragmaFunction(Movable):
    """A `PRAGMA` statement (`PRAGMA name(args)` or `PRAGMA name`).

    A query pragma returns SQL that runs in place of the pragma (its result
    is the pragma's result); an exec pragma runs for its side effect.
    """

    var name: String
    var signature: Signature
    var _query: Optional[duckdb_v2_pragma_function_query_callback_fn]
    var _exec: Optional[duckdb_v2_pragma_function_exec_callback_fn]

    @staticmethod
    def query[f: def(args: BindArgs) raises thin -> String](name: String, params: List[LogicalType]) -> PragmaFunction:
        """A pragma that runs the SQL `f` returns."""
        var p = PragmaFunction(name, params)
        var cb: duckdb_v2_pragma_function_query_callback_fn = _pragma_query_cb[f]
        p._query = cb
        return p^

    @staticmethod
    def exec[f: def(args: BindArgs) raises thin -> None](name: String, params: List[LogicalType]) -> PragmaFunction:
        """A pragma that calls `f` and returns nothing."""
        var p = PragmaFunction(name, params)
        var cb: duckdb_v2_pragma_function_exec_callback_fn = _pragma_exec_cb[f]
        p._exec = cb
        return p^

    def __init__(out self, name: String, params: List[LogicalType]):
        self.name = name
        self.signature = Signature(params, fixed=False)
        self._query = None
        self._exec = None

    def named(var self, *names: String) raises DuckDBError -> Self:
        var list = List[String]()
        for n in names:
            list.append(n)
        self.signature.name(list^)
        return self^

    def default[T: Movable & Deinitable](var self, name: StringSlice, value: T) raises DuckDBError -> Self:
        self.signature.set_default(name, Value(value))
        return self^

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_pragma_function_handle:
        var err = null_error_info()
        var function = null[duckdb_v2_pragma_function_handle]()
        check(lib().duckdb_v2_pragma_function_create(factory, Pointer(to=function), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_pragma_function_set_name(function, name.ptr(), Pointer(to=err)), err)
            var signature = null[duckdb_v2_function_signature_handle]()
            check(lib().duckdb_v2_pragma_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
            self.signature._add_to(signature)
            if self._query:
                check(lib().duckdb_v2_pragma_function_set_query_callback(function, self._query.value(), Pointer(to=err)), err)
            if self._exec:
                check(lib().duckdb_v2_pragma_function_set_exec_callback(function, self._exec.value(), Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_pragma_function_destroy(Pointer(to=function))
            raise e^
        return function


def _pragma_query_cb[f: def(args: BindArgs) raises thin -> String](
    info: duckdb_v2_function_bind_info_handle,
    result: duckdb_v2_pragma_function_query_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        var sql = StrArg(f(BindArgs(info, context)))
        var e = null_error_info()
        check(lib().duckdb_v2_pragma_function_query_set_query(result, sql.ptr(), Pointer(to=e)), e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _pragma_exec_cb[f: def(args: BindArgs) raises thin -> None](
    info: duckdb_v2_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        f(BindArgs(info, context))
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


# ===--------------------------------------------------------------------===#
# Custom options
# ===--------------------------------------------------------------------===#


struct CustomOption(Movable):
    """A setting users change with `SET name = value` and read with
    `current_setting('name')`."""

    var name: String
    var type: LogicalType
    var default: Value
    var description: String
    var scope: Optional[SettingScope]
    var session: Bool
    var _update: Optional[duckdb_v2_custom_option_update_callback_fn]

    @staticmethod
    def of[T: Movable & Deinitable](name: String, default: T, description: String = "") raises DuckDBError -> CustomOption:
        """An option of the DuckDB type of `T`, with a default value."""
        return CustomOption(name, logical_type_of[T](), Value(default), description)

    def __init__(out self, name: String, var type: LogicalType, var default: Value, description: String):
        self.name = name
        self.type = type^
        self.default = default^
        self.description = description
        self.scope = None
        self.session = True
        self._update = None

    def global_by_default(var self) -> Self:
        """`SET name = value` changes the value for every connection (not only this session)."""
        self.scope = SettingScope.GLOBAL
        return self^

    def global_only(var self) -> Self:
        """The option has one value for the whole database, so `SET SESSION` fails."""
        self.session = False
        return self^

    def validate[f: def(value: Value) raises thin -> Optional[Value]](var self) -> Self:
        """Check every new value: `f` raises to reject it (the `SET` fails
        with the message), returns a replacement to normalize it, or `None`
        to keep it. Not called for `RESET`."""
        var cb: duckdb_v2_custom_option_update_callback_fn = _option_update_cb[f]
        self._update = cb
        return self^

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_custom_option_handle:
        var err = null_error_info()
        var option = null[duckdb_v2_custom_option_handle]()
        check(lib().duckdb_v2_custom_option_create(factory, Pointer(to=option), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_custom_option_set_name(option, name.ptr(), Pointer(to=err)), err)
            check(lib().duckdb_v2_custom_option_set_type(option, self.type.handle(), Pointer(to=err)), err)
            check(lib().duckdb_v2_custom_option_set_default_value(option, self.default.handle(), Pointer(to=err)), err)
            if self.description:
                var d = StrArg(self.description)
                check(lib().duckdb_v2_custom_option_set_description(option, d.ptr(), Pointer(to=err)), err)
            if not self.session:
                check(
                    lib().duckdb_v2_custom_option_set_supports_scope(option, DUCKDB_V2_SETTING_SCOPE_SESSION, False, Pointer(to=err)),
                    err,
                )
            if self.scope:
                check(lib().duckdb_v2_custom_option_set_default_scope(option, self.scope.value().value, Pointer(to=err)), err)
            if self._update:
                check(lib().duckdb_v2_custom_option_set_update_callback(option, self._update.value(), Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_custom_option_destroy(Pointer(to=option))
            raise e^
        return option


def _option_update_cb[f: def(value: Value) raises thin -> Optional[Value]](
    info: duckdb_v2_custom_option_update_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_option_update[f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_INPUT_INVALID)


def _run_option_update[f: def(value: Value) raises thin -> Optional[Value]](info: duckdb_v2_custom_option_update_info_handle) raises:
    var e = null_error_info()
    var reset = False
    check(lib().duckdb_v2_custom_option_update_is_reset(info, Pointer(to=reset), Pointer(to=e)), e)
    if reset:
        return
    var v = null[duckdb_v2_value_handle]()
    check(lib().duckdb_v2_custom_option_update_get_value(info, Pointer(to=v), Pointer(to=e)), e)
    var replacement = f(Value(_owned=v))
    if replacement:
        var code = lib().duckdb_v2_custom_option_update_set_value(info, replacement.value().handle(), Pointer(to=e))
        _ = replacement^  # keep the value alive until DuckDB has copied it
        check(code, e)


# ===--------------------------------------------------------------------===#
# Custom types
# ===--------------------------------------------------------------------===#


struct CustomType(Movable):
    """A named type stored as a base type (`CREATE TYPE name AS base`).

    Values keep the base type's storage, so Mojo reads and writes them as the
    base type, but DuckDB treats the type as distinct: give it its own
    functions and casts (`CastFunction` with `source=`/`target=` set to
    `LogicalType.parse("name", con.factory())`).
    """

    var name: String
    var base: LogicalType

    def __init__(out self, name: String, var base: LogicalType):
        self.name = name
        self.base = base^

    @staticmethod
    def of[T: Movable & Deinitable](name: String) raises DuckDBError -> CustomType:
        """A custom type stored as the DuckDB type of `T`."""
        return CustomType(name, logical_type_of[T]())

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_custom_type_handle:
        var err = null_error_info()
        var t = null[duckdb_v2_custom_type_handle]()
        check(lib().duckdb_v2_custom_type_create(factory, Pointer(to=t), Pointer(to=err)), err)
        try:
            var n = StrArg(self.name)
            check(lib().duckdb_v2_custom_type_set_name(t, n.ptr(), Pointer(to=err)), err)
            check(lib().duckdb_v2_custom_type_set_base_type(t, self.base.handle(), Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_custom_type_destroy(Pointer(to=t))
            raise e^
        return t
