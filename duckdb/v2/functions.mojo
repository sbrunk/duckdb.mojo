"""Scalar functions written in Mojo, registered on a `Connection` or an `Extension`.

```mojo
def add(a: Int64, b: Int64) -> Int64:
    return a + b

con.register(ScalarFunction.from_function["mojo_add", add]())
```

`from_function` reads the SQL signature from the Mojo function (1 to 8
arguments). Arguments and the result use the type mapping, so strings,
lists, structs, maps and unions work as well as numbers.

NULLs: by default a NULL argument gives a NULL result without calling the
function. An `Optional` argument receives NULLs as `None` instead; an
`Optional` result returns NULL for `None`. A function that raises fails the
query with its message.

Bind data: a first parameter whose type conforms to `BindData` is not a SQL
argument. Its `bind` runs once per call site when the query is planned (with
access to the argument types and constant arguments), and every execution
gets the result, read-only:

```mojo
@fieldwise_init
struct Scale(BindData, Copyable):
    var factor: Float64

    @staticmethod
    def bind(args: BindArgs) raises -> Self:
        var f = args.constant[Float64](1)
        if not f:
            raise Error("the factor must be a constant")
        return Self(f.value())

def scale(s: Scale, x: Float64, factor: Float64) -> Float64:
    return x * s.factor
```
"""

from std.builtin.rebind import downcast, rebind_var
from std.reflection import reflect
from std.memory.alloc import unsafe_alloc
from std.sys import simd_width_of
from emberserde.field_meta import __is_optional
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb.v2._runtime import check, lib
from duckdb.v2.aggregate import AggregateFunction
from duckdb.v2.table import TableFunction
from duckdb.v2.chunk import ColumnReader, column_reader
from duckdb.v2.error import DuckDBError, ErrorCode
from duckdb.v2.mapping import is_mapped, mapped_read
from duckdb.v2.serde import VectorWriter, _ReadTree, _RNode, from_value, logical_type_of, read_tree_of, read_value
from duckdb.v2.types import LogicalType


# ===--------------------------------------------------------------------===#
# Registration
# ===--------------------------------------------------------------------===#


trait Registrar:
    """Where functions are registered: a `Connection` or an `Extension`."""

    def factory(self) raises DuckDBError -> duckdb_v2_factory_handle:
        """The factory functions are created with."""
        ...

    def _register_scalar_handle(self, function: duckdb_v2_scalar_function_handle) raises DuckDBError:
        ...

    def register(self, function: ScalarFunction) raises DuckDBError:
        """Register a scalar function. Registering another function under the
        same name with other argument types adds an overload."""
        function._register(self)

    def _register_aggregate_handle(self, function: duckdb_v2_aggregate_function_handle) raises DuckDBError:
        ...

    def register(self, function: AggregateFunction) raises DuckDBError:
        """Register an aggregate function (each name once: DuckDB 2.0 cannot overload aggregates)."""
        function._register(self)

    def _register_table_handle(self, function: duckdb_v2_table_function_handle) raises DuckDBError:
        ...

    def register(self, function: TableFunction) raises DuckDBError:
        """Register a table function."""
        function._register(self)


comptime _ExecFn = duckdb_v2_scalar_function_exec_callback_fn
comptime _BindFn = duckdb_v2_scalar_function_bind_callback_fn
comptime _InitFn = duckdb_v2_scalar_function_init_callback_fn


struct ScalarFunction(Movable):
    """A scalar function: its name, signature, properties and callbacks.

    It is a description; the DuckDB function is created when it is registered,
    with the registrar's factory, so one definition works for connections and
    extensions.
    """

    var name: String
    var params: List[LogicalType]
    var result: LogicalType
    var _exec: _ExecFn
    var _bind: Optional[_BindFn]
    var _init: Optional[_InitFn]
    var volatile: Bool
    """Whether the function can return different results for the same
    arguments (a random number, the current time). DuckDB then calls it for
    every row and never folds it to a constant."""
    var special_nulls: Bool
    """Whether the function is called for NULL arguments (set when it takes
    an `Optional` argument)."""

    def __init__(
        out self,
        var name: String,
        var params: List[LogicalType],
        var result: LogicalType,
        exec: _ExecFn,
        bind: Optional[_BindFn] = None,
        volatile: Bool = False,
        special_nulls: Bool = False,
        init: Optional[_InitFn] = None,
    ):
        """A function from its parts, for hand-written exec callbacks."""
        self.name = name^
        self.params = params^
        self.result = result^
        self._exec = exec
        self._bind = bind
        self._init = init
        self.volatile = volatile
        self.special_nulls = special_nulls

    def _register[R: Registrar](self, registrar: R) raises DuckDBError:
        var err = null_error_info()
        var function = null[duckdb_v2_scalar_function_handle]()
        check(lib().duckdb_v2_scalar_function_create(registrar.factory(), Pointer(to=function), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_scalar_function_set_name(function, name.ptr(), Pointer(to=err)), err)
            var signature = null[duckdb_v2_function_signature_handle]()
            check(lib().duckdb_v2_scalar_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
            for i in range(len(self.params)):
                var param = StrArg(String("arg", i + 1))
                check(
                    lib().duckdb_v2_function_signature_add_parameter(
                        signature,
                        param.ptr(),
                        self.params[i].handle(),
                        null[duckdb_v2_value_handle](),
                        DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY,
                        Pointer(to=err),
                    ),
                    err,
                )
            check(lib().duckdb_v2_function_signature_set_return_type(signature, self.result.handle(), Pointer(to=err)), err)
            if self.volatile:
                check(
                    lib().duckdb_v2_scalar_function_set_property(
                        function,
                        DUCKDB_V2_FUNCTION_PROPERTY_STABILITY,
                        DUCKDB_V2_FUNCTION_PROPERTY_STABILITY_VOLATILE,
                        Pointer(to=err),
                    ),
                    err,
                )
            if self.special_nulls:
                check(
                    lib().duckdb_v2_scalar_function_set_property(
                        function,
                        DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
                        DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL,
                        Pointer(to=err),
                    ),
                    err,
                )
            var identity = _identity_user_data(self.name)
            check(lib().duckdb_v2_scalar_function_set_user_data(function, Pointer(to=identity), Pointer(to=err)), err)
            check(lib().duckdb_v2_scalar_function_set_exec_callback(function, self._exec, Pointer(to=err)), err)
            if self._bind:
                check(lib().duckdb_v2_scalar_function_set_bind_callback(function, self._bind.value(), Pointer(to=err)), err)
            if self._init:
                check(lib().duckdb_v2_scalar_function_set_init_callback(function, self._init.value(), Pointer(to=err)), err)
            registrar._register_scalar_handle(function)
        finally:
            _ = lib().duckdb_v2_scalar_function_destroy(Pointer(to=function))

    @staticmethod
    def from_chunk_function[
        f: def(mut args: ScalarArgs, mut out: VectorWriter) raises thin -> None,
    ](
        var name: String,
        var params: List[LogicalType],
        var result: LogicalType,
        *,
        bind: Optional[_BindFn] = None,
        init: Optional[_InitFn] = None,
        volatile: Bool = False,
        special_nulls: Bool = False,
    ) -> ScalarFunction:
        """A scalar function that processes a whole chunk at a time.

        `f` reads its arguments through `args.column[T](i)` and writes every
        row of `out` (`args.size()` rows). DuckDB passes NULL rows to it, so
        it writes NULLs itself (`out.write_null(row)`). For bind data, pass
        `bind=bind_callback[MyBindData]()` and read it with
        `args.bind_data[MyBindData]()`. For state per thread (scratch
        buffers, ...), pass `init=init_callback[MyState]()` and use
        `args.local_state[MyState]()`.

        ```mojo
        def double_it(mut args: ScalarArgs, mut out: VectorWriter) raises:
            var x = args.column[Int64](0)
            for row in range(args.size()):
                var v = x.get(row)
                if v:
                    out.write(row, v.value() * 2)
                else:
                    out.write_null(row)
        ```
        """
        return ScalarFunction(name^, params^, result^, _chunk_exec[f], bind, volatile, special_nulls, init)

    @staticmethod
    def from_simd_function[
        name: StringLiteral,
        In: DType,
        Out: DType,
        func: def[width: SIMDLength](SIMD[In, width]) thin -> SIMD[Out, width],
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        """A scalar function from a SIMD function of one argument.

        Flat inputs are computed in SIMD registers; constant and dictionary
        inputs one row at a time. NULL in gives NULL out.
        """
        return ScalarFunction(
            String(name), [logical_type_of[Scalar[In]]()], logical_type_of[Scalar[Out]](), _simd1[In, Out, func], None, volatile
        )

    @staticmethod
    def from_simd_function[
        name: StringLiteral,
        In1: DType,
        In2: DType,
        Out: DType,
        func: def[width: SIMDLength](SIMD[In1, width], SIMD[In2, width]) thin -> SIMD[Out, width],
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        """A scalar function from a SIMD function of two arguments (see the one-argument form)."""
        return ScalarFunction(
            String(name),
            [logical_type_of[Scalar[In1]](), logical_type_of[Scalar[In2]]()],
            logical_type_of[Scalar[Out]](),
            _simd2[In1, In2, Out, func],
            None,
            volatile,
        )

    @staticmethod
    def from_simd_function[
        name: StringLiteral,
        D: DType,
        func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point(),
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction where D.is_floating_point():
        """A scalar function from a standard-library style SIMD function such
        as `math.sqrt`, for one `D` argument and a `D` result."""
        return ScalarFunction(
            String(name), [logical_type_of[Scalar[D]]()], logical_type_of[Scalar[D]](), _simd1g[D, func], None, volatile
        )

    @staticmethod
    def from_simd_function[
        name: StringLiteral,
        D: DType,
        func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width], SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point(),
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction where D.is_floating_point():
        """A scalar function from a standard-library style SIMD function of
        two `D` arguments (`math.atan2`, ...)."""
        return ScalarFunction(
            String(name),
            [logical_type_of[Scalar[D]](), logical_type_of[Scalar[D]]()],
            logical_type_of[Scalar[D]](),
            _simd2g[D, func],
            None,
            volatile,
        )

    # BEGIN GENERATED from_function (scripts/generate_scalar_functions.py)
    @staticmethod
    def from_function[
        A1: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        """A scalar function from a Mojo function (see the module docs)."""
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec1[A1, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec2[A1, A2, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec3[A1, A2, A3, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3, A4) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec4[A1, A2, A3, A4, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3, A4, A5) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec5[A1, A2, A3, A4, A5, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3, A4, A5, A6) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec6[A1, A2, A3, A4, A5, A6, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3, A4, A5, A6, A7) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        _param[A7](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec7[A1, A2, A3, A4, A5, A6, A7, R, f], bind, volatile, special_nulls
        )

    @staticmethod
    def from_function[
        A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, R: Movable & Deinitable, //,
        name: StringLiteral,
        f: def(A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> R,
    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        var bind = Optional[_BindFn](None)
        comptime if is_bind_data[A1]():
            var callback: _BindFn = _bind_callback[A1]
            bind = callback
        else:
            _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        _param[A7](params, special_nulls)
        _param[A8](params, special_nulls)
        return ScalarFunction(
            String(name), params^, logical_type_of[R](), _exec8[A1, A2, A3, A4, A5, A6, A7, A8, R, f], bind, volatile, special_nulls
        )
    # END GENERATED from_function


# ===--------------------------------------------------------------------===#
# Bind data
# ===--------------------------------------------------------------------===#


trait BindData(Deinitable, Movable):
    """Data a scalar function computes once per call site, when the query is
    planned. Take it as the function's first parameter."""

    @staticmethod
    def bind(args: BindArgs) raises -> Self:
        ...


struct BindArgs:
    """The arguments of the call site being bound: their types, and the
    values of constant ones. Indexes count SQL arguments (the bind data
    parameter is not one)."""

    var _info: duckdb_v2_function_bind_info_handle

    def __init__(out self, info: duckdb_v2_function_bind_info_handle):
        self._info = info

    def count(self) raises DuckDBError -> Int:
        var fixed: idx_t = 0
        var variadic: idx_t = 0
        var named: idx_t = 0
        var named_variadic: idx_t = 0
        var err = null_error_info()
        check(
            lib().duckdb_v2_function_bind_get_arg_count(
                self._info, Pointer(to=fixed), Pointer(to=variadic), Pointer(to=named), Pointer(to=named_variadic), Pointer(to=err)
            ),
            err,
        )
        return Int(fixed + variadic + named + named_variadic)

    def type(self, i: Int) raises DuckDBError -> LogicalType:
        var t = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_function_bind_get_arg_type(self._info, idx_t(i), Pointer(to=t), Pointer(to=err)), err)
        var owned = null[duckdb_v2_logical_type_handle]()
        check(lib().duckdb_v2_logical_type_copy(t, Pointer(to=owned), Pointer(to=err)), err)
        return LogicalType(_owned=owned)

    def _value(self, i: Int) -> Optional[duckdb_v2_value_handle]:
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        if lib().duckdb_v2_function_bind_get_arg_value(self._info, idx_t(i), Pointer(to=v), Pointer(to=err)) != DUCKDB_V2_ERROR_NONE:
            _ = lib().duckdb_v2_error_info_destroy(Pointer(to=err))
            return None
        return v

    def is_constant(self, i: Int) -> Bool:
        """Whether argument `i` is a constant (folds to a value at planning time)."""
        var v = self._value(i)
        if not v:
            return False
        var h = v.value()
        _ = lib().duckdb_v2_value_destroy(Pointer(to=h))
        return True

    def constant[T: Movable & Deinitable](self, i: Int) raises DuckDBError -> Optional[T]:
        """The value of constant argument `i`, or `None` for NULL. Raises if
        the argument is not a constant (a column, ...)."""
        var v = self._value(i)
        if not v:
            raise DuckDBError(ErrorCode.INPUT_INVALID, String("argument ", i + 1, " is not a constant"))
        var h = v.value()
        try:
            var is_null = False
            var err = null_error_info()
            check(lib().duckdb_v2_value_is_null(h, Pointer(to=is_null), Pointer(to=err)), err)
            if is_null:
                return None
            return Optional[T](from_value[T](h))
        finally:
            _ = lib().duckdb_v2_value_destroy(Pointer(to=h))


def _bind_callback[T: AnyType](
    info: duckdb_v2_function_bind_info_handle,
    result: duckdb_v2_scalar_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    comptime B = downcast[T, BindData]
    try:
        var data = B.bind(BindArgs(info))
        var ptr = unsafe_alloc[B](1)
        ptr.unsafe_write(data^)
        var opaque = duckdb_v2_opaque(ptr.unsafe_bitcast[NoneType](), _destroy_bind[B], _equal_bind[B])
        var e2 = null_error_info()
        var code = lib().duckdb_v2_scalar_function_bind_set_bind_data(result, Pointer(to=opaque), Pointer(to=e2))
        if code != DUCKDB_V2_ERROR_NONE:
            _destroy_bind[B](ptr.unsafe_bitcast[NoneType]())
            check(code, e2)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _destroy_bind[T: Movable & Deinitable](ptr: Pointer[NoneType, MutUntrackedOrigin]) abi("C"):
    var p = ptr.unsafe_bitcast[T]()
    p.unsafe_deinit_pointee()
    p.unsafe_free()


def _equal_bind[T: Movable & Deinitable](
    a: Pointer[NoneType, MutUntrackedOrigin], b: Pointer[NoneType, MutUntrackedOrigin]
) abi("C") -> Bool:
    # Equal bind data lets DuckDB treat two calls as one expression.
    comptime if conforms_to(T, Equatable):
        comptime E = downcast[T, Equatable]
        return a.unsafe_bitcast[E]()[] == b.unsafe_bitcast[E]()[]
    else:
        return Int(a) == Int(b)


trait LocalState(Deinitable, Movable):
    """State of one thread running a chunk-level scalar function, created
    once per thread per query (see `from_chunk_function`)."""

    @staticmethod
    def init() raises -> Self:
        ...


def init_callback[T: LocalState]() -> _InitFn:
    """The init callback for thread state `T`, for `from_chunk_function(..., init=...)`."""
    return _init_callback[T]


def _init_callback[T: LocalState](
    info: duckdb_v2_scalar_function_init_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        var ptr = unsafe_alloc[T](1)
        ptr.unsafe_write(T.init())
        var opaque = duckdb_v2_opaque(ptr.unsafe_bitcast[NoneType](), _destroy_bind[T], _equal_bind[T])
        var e = null_error_info()
        var code = lib().duckdb_v2_scalar_function_init_set_init_data(info, Pointer(to=opaque), Pointer(to=e))
        if code != DUCKDB_V2_ERROR_NONE:
            _destroy_bind[T](ptr.unsafe_bitcast[NoneType]())
            check(code, e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def bind_callback[T: BindData]() -> _BindFn:
    """The bind callback for bind data `T`, for `from_chunk_function(..., bind=...)`."""
    return _bind_callback[T]


def _identity_user_data(name: String) -> duckdb_v2_opaque:
    """User data that tells functions apart by name.

    DuckDB compares functions when it merges identical expressions, and every
    C API function shares the same C++ wrapper: without user data, two
    different aggregates over the same column (`SELECT f(x), g(x)`) are
    computed as one.
    """
    var p = unsafe_alloc[String](1)
    p.unsafe_write(name.copy())
    return duckdb_v2_opaque(p.unsafe_bitcast[NoneType](), _destroy_bind[String], _equal_bind[String])


def is_bind_data[T: AnyType]() -> Bool:
    return conforms_to(T, BindData)


# ===--------------------------------------------------------------------===#
# Execution
# ===--------------------------------------------------------------------===#


def _report(err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin], message: String, code: DUCKDB_V2_ERROR):
    """Fail the callback. The code is what fails it; the text alone does not."""
    var text = StrArg(message)
    _ = lib().duckdb_v2_error_info_set_code(err[], code)
    _ = lib().duckdb_v2_error_info_set_text(err[], text.ptr())


struct _Args(Movable):
    """The argument vectors of a callback as read trees (each vector is node 1 of its tree)."""

    var size: Int
    var args: List[_ReadTree]

    def __init__(out self, vectors: List[duckdb_v2_vector_handle], size: Int) raises DuckDBError:
        self.size = size
        self.args = List[_ReadTree]()
        for i in range(len(vectors)):
            self.args.append(read_tree_of(vectors[i]))

    def all_flat(self) -> Bool:
        """Whether every argument is flat or constant (no dictionary) and
        has no NULLs, so every row can be computed from the raw arrays."""
        for i in range(len(self.args)):
            ref n = self.args[i].nodes[1]
            if Int(n.sel) != 0 and not n.constant:
                return False
            if Int(n.validity) != 0:
                var rows = 1 if n.constant else self.size
                for row in range(rows):
                    if not self.args[i].is_valid(1, row):
                        return False
        return True

    def step(self, arg: Int) -> Int:
        """1 for a flat argument, 0 for a constant one (all rows read index 0)."""
        return 0 if self.args[arg].nodes[1].constant else 1

    def numbers[T: AnyType](self, arg: Int) -> Pointer[downcast[T, TrivialRegisterPassable], MutUntrackedOrigin]:
        """The storage of a flat numeric argument."""
        return self.args[arg].nodes[1].data.unsafe_bitcast[downcast[T, TrivialRegisterPassable]]().unsafe_mut_cast[True]().unsafe_origin_cast[MutUntrackedOrigin]()

    def view(mut self, arg: Int) -> _NodePtr:
        """The vector node of argument `arg` (stable while the call lives)."""
        return Pointer(to=self.args[arg].nodes[1]).unsafe_origin_cast[MutUntrackedOrigin]()

    @always_inline
    def get[T: AnyType](mut self, arg: Int, node: _NodePtr, row: Int) raises DuckDBError -> T:
        comptime if is_mapped[T]():
            # The signature fixes the type and NULL rows were skipped: read
            # the storage directly.
            var idx = Int(node[].sel[unsafe_offset=row]) if Int(node[].sel) != 0 else row
            return mapped_read[T](node[].meta, node[].data, idx)
        else:
            return read_value[T](self.args[arg], 1, row)


struct _Call(Movable):
    """One exec callback: the argument trees and the result writer."""

    var info: duckdb_v2_scalar_function_exec_info_handle
    var size: Int
    var a: _Args
    var out: VectorWriter

    def __init__(out self, info: duckdb_v2_scalar_function_exec_info_handle) raises DuckDBError:
        self.info = info
        var err = null_error_info()
        var count: idx_t = 0
        check(lib().duckdb_v2_scalar_function_exec_get_row_count(info, Pointer(to=count), Pointer(to=err)), err)
        self.size = Int(count)
        var fixed: idx_t = 0
        var variadic: idx_t = 0
        var named: idx_t = 0
        var named_variadic: idx_t = 0
        check(
            lib().duckdb_v2_scalar_function_exec_get_arg_count(
                info, Pointer(to=fixed), Pointer(to=variadic), Pointer(to=named), Pointer(to=named_variadic), Pointer(to=err)
            ),
            err,
        )
        var vectors = List[duckdb_v2_vector_handle]()
        for i in range(Int(fixed + variadic + named + named_variadic)):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_scalar_function_exec_get_arg(info, idx_t(i), Pointer(to=vec), Pointer(to=err)), err)
            vectors.append(vec)
        self.a = _Args(vectors, self.size)
        var result = null[duckdb_v2_vector_handle]()
        check(lib().duckdb_v2_scalar_function_exec_get_result(info, Pointer(to=result), Pointer(to=err)), err)
        self.out = VectorWriter(result, self.size)

    def all_flat(self) -> Bool:
        return self.a.all_flat()

    def step(self, arg: Int) -> Int:
        return self.a.step(arg)

    def numbers[T: AnyType](self, arg: Int) -> Pointer[downcast[T, TrivialRegisterPassable], MutUntrackedOrigin]:
        return self.a.numbers[T](arg)

    def view(mut self, arg: Int) -> _NodePtr:
        return self.a.view(arg)

    @always_inline
    def get[T: AnyType](mut self, arg: Int, node: _NodePtr, row: Int) raises DuckDBError -> T:
        return self.a.get[T](arg, node, row)

    def bind_data[T: AnyType](self) raises DuckDBError -> Pointer[T, MutUntrackedOrigin]:
        var p = null[Pointer[NoneType, MutUntrackedOrigin]]()
        var err = null_error_info()
        check(lib().duckdb_v2_scalar_function_exec_get_bind_data(self.info, Pointer(to=p), Pointer(to=err)), err)
        return p.unsafe_bitcast[T]()


struct ScalarArgs(Movable):
    """The arguments of one chunk-level call: `size()` rows, one column per argument."""

    var _info: duckdb_v2_scalar_function_exec_info_handle
    var _size: Int
    var _vectors: List[duckdb_v2_vector_handle]

    def __init__(out self, info: duckdb_v2_scalar_function_exec_info_handle) raises DuckDBError:
        self._info = info
        var err = null_error_info()
        var count: idx_t = 0
        check(lib().duckdb_v2_scalar_function_exec_get_row_count(info, Pointer(to=count), Pointer(to=err)), err)
        self._size = Int(count)
        var fixed: idx_t = 0
        var variadic: idx_t = 0
        var named: idx_t = 0
        var named_variadic: idx_t = 0
        check(
            lib().duckdb_v2_scalar_function_exec_get_arg_count(
                info, Pointer(to=fixed), Pointer(to=variadic), Pointer(to=named), Pointer(to=named_variadic), Pointer(to=err)
            ),
            err,
        )
        self._vectors = List[duckdb_v2_vector_handle]()
        for i in range(Int(fixed + variadic + named + named_variadic)):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_scalar_function_exec_get_arg(info, idx_t(i), Pointer(to=vec), Pointer(to=err)), err)
            self._vectors.append(vec)

    def size(self) -> Int:
        """The number of rows."""
        return self._size

    def __len__(self) -> Int:
        """The number of arguments."""
        return len(self._vectors)

    def column[T: Movable & Deinitable](ref self, i: Int) raises DuckDBError -> ColumnReader[T, origin_of(self)]:
        """A reader over argument `i`. It may be a constant or dictionary
        vector: read it by row (`reader[row]`, `reader.get(row)`), or check
        `is_flat()` / `span()` for direct access."""
        if i < 0 or i >= len(self._vectors):
            raise DuckDBError(ErrorCode.API, String("argument index ", i, " out of range (", len(self._vectors), " arguments)"))
        var t = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_vector_get_logical_type(self._vectors[i], Pointer(to=t), Pointer(to=err)), err)
        return column_reader[T, origin_of(self)](self._vectors[i], String("argument ", i + 1), LogicalType(_owned=t), self._size)

    def bind_data[T: BindData](self) raises DuckDBError -> ref[MutUntrackedOrigin] T:
        """The bind data of the call site (read it, do not change it: all threads share it)."""
        var p = null[Pointer[NoneType, MutUntrackedOrigin]]()
        var err = null_error_info()
        check(lib().duckdb_v2_scalar_function_exec_get_bind_data(self._info, Pointer(to=p), Pointer(to=err)), err)
        return p.unsafe_bitcast[T]()[]

    def local_state[T: LocalState](self) raises DuckDBError -> ref[MutUntrackedOrigin] T:
        """This thread's state (needs `init=init_callback[T]()`)."""
        var p = null[Pointer[NoneType, MutUntrackedOrigin]]()
        var err = null_error_info()
        check(lib().duckdb_v2_scalar_function_exec_get_init_data(self._info, Pointer(to=p), Pointer(to=err)), err)
        if Int(p) == 0:
            raise DuckDBError(ErrorCode.API, "the function has no thread state: register it with init=init_callback[...]()")
        return p.unsafe_bitcast[T]()[]

    def result(self) raises DuckDBError -> VectorWriter:
        var result = null[duckdb_v2_vector_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_scalar_function_exec_get_result(self._info, Pointer(to=result), Pointer(to=err)), err)
        return VectorWriter(result, self._size)


def _chunk_exec[f: def(mut args: ScalarArgs, mut out: VectorWriter) raises thin -> None](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_chunk[f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_chunk[f: def(mut args: ScalarArgs, mut out: VectorWriter) raises thin -> None](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var args = ScalarArgs(info)
    var out = args.result()
    f(args, out)
    out.finish()


def _simd1[In: DType, Out: DType, func: def[width: SIMDLength](SIMD[In, width]) thin -> SIMD[Out, width]](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_simd1[In, Out, func](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_simd1[In: DType, Out: DType, func: def[width: SIMDLength](SIMD[In, width]) thin -> SIMD[Out, width]](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var args = ScalarArgs(info)
    var out = args.result()
    var x = args.column[Scalar[In]](0)
    var dst = out.data[Scalar[Out]]()
    var n = args.size()
    if x.is_flat():
        # Every row, NULL rows included (their values are ignored), then the NULLs.
        comptime w = simd_width_of[In]()
        var src = x.unsafe_data().unsafe_bitcast[Scalar[In]]()
        var i = 0
        while i + w <= n:
            dst.unsafe_offset(i).unsafe_store(0, func[w](src.unsafe_offset(i).unsafe_load[width=w]()))
            i += w
        while i < n:
            dst[unsafe_offset=i] = func[1](src[unsafe_offset=i])
            i += 1
        if x.has_nulls():
            for row in range(n):
                if not x.is_valid(row):
                    out.write_null(row)
    else:
        for row in range(n):
            var v = x.get(row)
            if v:
                dst[unsafe_offset=row] = func[1](v.value())
            else:
                out.write_null(row)


def _simd2[
    In1: DType,
    In2: DType,
    Out: DType,
    func: def[width: SIMDLength](SIMD[In1, width], SIMD[In2, width]) thin -> SIMD[Out, width],
](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_simd2[In1, In2, Out, func](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_simd2[
    In1: DType,
    In2: DType,
    Out: DType,
    func: def[width: SIMDLength](SIMD[In1, width], SIMD[In2, width]) thin -> SIMD[Out, width],
](info: duckdb_v2_scalar_function_exec_info_handle) raises:
    var args = ScalarArgs(info)
    var out = args.result()
    var x = args.column[Scalar[In1]](0)
    var y = args.column[Scalar[In2]](1)
    var dst = out.data[Scalar[Out]]()
    var n = args.size()
    if x.is_flat() and y.is_flat():
        comptime w = simd_width_of[Out]()
        var a = x.unsafe_data().unsafe_bitcast[Scalar[In1]]()
        var b = y.unsafe_data().unsafe_bitcast[Scalar[In2]]()
        var i = 0
        while i + w <= n:
            dst.unsafe_offset(i).unsafe_store(
                0, func[w](a.unsafe_offset(i).unsafe_load[width=w](), b.unsafe_offset(i).unsafe_load[width=w]())
            )
            i += w
        while i < n:
            dst[unsafe_offset=i] = func[1](a[unsafe_offset=i], b[unsafe_offset=i])
            i += 1
        if x.has_nulls() or y.has_nulls():
            for row in range(n):
                if not x.is_valid(row) or not y.is_valid(row):
                    out.write_null(row)
    else:
        for row in range(n):
            var u = x.get(row)
            var v = y.get(row)
            if u and v:
                dst[unsafe_offset=row] = func[1](u.value(), v.value())
            else:
                out.write_null(row)


def _simd1g[D: DType, func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point()](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C") where D.is_floating_point():
    try:
        _run_simd1g[D, func](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_simd1g[D: DType, func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point()](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises where D.is_floating_point():
    var args = ScalarArgs(info)
    var out = args.result()
    var x = args.column[Scalar[D]](0)
    var dst = out.data[Scalar[D]]()
    var n = args.size()
    if x.is_flat():
        # Every row, NULL rows included (their values are ignored), then the NULLs.
        comptime w = simd_width_of[D]()
        var src = x.unsafe_data().unsafe_bitcast[Scalar[D]]()
        var i = 0
        while i + w <= n:
            dst.unsafe_offset(i).unsafe_store(0, func(src.unsafe_offset(i).unsafe_load[width=w]()))
            i += w
        while i < n:
            dst[unsafe_offset=i] = func(src[unsafe_offset=i])
            i += 1
        if x.has_nulls():
            for row in range(n):
                if not x.is_valid(row):
                    out.write_null(row)
    else:
        for row in range(n):
            var v = x.get(row)
            if v:
                dst[unsafe_offset=row] = func(v.value())
            else:
                out.write_null(row)


def _simd2g[
    D: DType,
    func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width], SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point(),
](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C") where D.is_floating_point():
    try:
        _run_simd2g[D, func](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_simd2g[
    D: DType,
    func: def[dtype: DType, width: SIMDLength, //](SIMD[dtype, width], SIMD[dtype, width]) thin -> SIMD[dtype, width] where dtype.is_floating_point(),
](info: duckdb_v2_scalar_function_exec_info_handle) raises where D.is_floating_point():
    var args = ScalarArgs(info)
    var out = args.result()
    var x = args.column[Scalar[D]](0)
    var y = args.column[Scalar[D]](1)
    var dst = out.data[Scalar[D]]()
    var n = args.size()
    if x.is_flat() and y.is_flat():
        comptime w = simd_width_of[D]()
        var a = x.unsafe_data().unsafe_bitcast[Scalar[D]]()
        var b = y.unsafe_data().unsafe_bitcast[Scalar[D]]()
        var i = 0
        while i + w <= n:
            dst.unsafe_offset(i).unsafe_store(
                0, func(a.unsafe_offset(i).unsafe_load[width=w](), b.unsafe_offset(i).unsafe_load[width=w]())
            )
            i += w
        while i < n:
            dst[unsafe_offset=i] = func(a[unsafe_offset=i], b[unsafe_offset=i])
            i += 1
        if x.has_nulls() or y.has_nulls():
            for row in range(n):
                if not x.is_valid(row) or not y.is_valid(row):
                    out.write_null(row)
    else:
        for row in range(n):
            var u = x.get(row)
            var v = y.get(row)
            if u and v:
                dst[unsafe_offset=row] = func(u.value(), v.value())
            else:
                out.write_null(row)


comptime _NodePtr = Pointer[_RNode, MutUntrackedOrigin]


@always_inline
def _valid(node: _NodePtr, row: Int) -> Bool:
    ref n = node[]
    if Int(n.validity) == 0:
        return True
    var idx = Int(n.sel[unsafe_offset=row]) if Int(n.sel) != 0 else row
    return (n.validity[unsafe_offset=idx >> 6] >> UInt64(idx & 63)) & 1 == 1


def _is_number[T: AnyType]() -> Bool:
    return reflect[T].base_name() == "SIMD" and conforms_to(T, TrivialRegisterPassable)


def _all_numbers[*Ts: AnyType]() -> Bool:
    """Whether every type is a plain number (`Int64`, `Float64`, ...)."""
    comptime for i in range(Ts.length):
        comptime if not _is_number[Ts[i]]():
            return False
    return True


def _required[T: AnyType]() -> Bool:
    return not __is_optional[T]()


def _param[T: AnyType](mut params: List[LogicalType], mut special_nulls: Bool) raises DuckDBError:
    params.append(logical_type_of[T]())
    special_nulls = special_nulls or __is_optional[T]()


# BEGIN GENERATED exec (scripts/generate_scalar_functions.py)


def _exec1[A1: Movable & Deinitable, R: Movable & Deinitable, f: def(A1) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run1[A1, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run1[A1: Movable & Deinitable, R: Movable & Deinitable, f: def(A1) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        for row in range(call.size):
            call.out.write(row, f(b[]))
    else:
        comptime if _all_numbers[A1, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var s0 = call.step(0)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0]))
                call.out.finish()
                return
        var v0 = call.view(0)
        comptime r0 = _required[A1]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row)))
    call.out.finish()

def _exec2[A1: Movable & Deinitable, A2: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run2[A1, A2, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run2[A1: Movable & Deinitable, A2: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        comptime r0 = _required[A2]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row)))
    else:
        comptime if _all_numbers[A1, A2, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row)))
    call.out.finish()

def _exec3[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run3[A1, A2, A3, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run3[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row)))
    call.out.finish()

def _exec4[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run4[A1, A2, A3, A4, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run4[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        comptime r2 = _required[A4]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row), call.get[A4](2, v2, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, A4, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var p3 = call.numbers[A4](3)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var s3 = call.step(3)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2], p3[unsafe_offset=row * s3]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        comptime r3 = _required[A4]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row), call.get[A4](3, v3, row)))
    call.out.finish()

def _exec5[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run5[A1, A2, A3, A4, A5, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run5[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        comptime r2 = _required[A4]()
        comptime r3 = _required[A5]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row), call.get[A4](2, v2, row), call.get[A5](3, v3, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, A4, A5, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var p3 = call.numbers[A4](3)
                var p4 = call.numbers[A5](4)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var s3 = call.step(3)
                var s4 = call.step(4)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2], p3[unsafe_offset=row * s3], p4[unsafe_offset=row * s4]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        comptime r3 = _required[A4]()
        comptime r4 = _required[A5]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row), call.get[A4](3, v3, row), call.get[A5](4, v4, row)))
    call.out.finish()

def _exec6[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run6[A1, A2, A3, A4, A5, A6, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run6[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        comptime r2 = _required[A4]()
        comptime r3 = _required[A5]()
        comptime r4 = _required[A6]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row), call.get[A4](2, v2, row), call.get[A5](3, v3, row), call.get[A6](4, v4, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, A4, A5, A6, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var p3 = call.numbers[A4](3)
                var p4 = call.numbers[A5](4)
                var p5 = call.numbers[A6](5)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var s3 = call.step(3)
                var s4 = call.step(4)
                var s5 = call.step(5)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2], p3[unsafe_offset=row * s3], p4[unsafe_offset=row * s4], p5[unsafe_offset=row * s5]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        var v5 = call.view(5)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        comptime r3 = _required[A4]()
        comptime r4 = _required[A5]()
        comptime r5 = _required[A6]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row), call.get[A4](3, v3, row), call.get[A5](4, v4, row), call.get[A6](5, v5, row)))
    call.out.finish()

def _exec7[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6, A7) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run7[A1, A2, A3, A4, A5, A6, A7, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run7[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6, A7) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        var v5 = call.view(5)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        comptime r2 = _required[A4]()
        comptime r3 = _required[A5]()
        comptime r4 = _required[A6]()
        comptime r5 = _required[A7]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row), call.get[A4](2, v2, row), call.get[A5](3, v3, row), call.get[A6](4, v4, row), call.get[A7](5, v5, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, A4, A5, A6, A7, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var p3 = call.numbers[A4](3)
                var p4 = call.numbers[A5](4)
                var p5 = call.numbers[A6](5)
                var p6 = call.numbers[A7](6)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var s3 = call.step(3)
                var s4 = call.step(4)
                var s5 = call.step(5)
                var s6 = call.step(6)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2], p3[unsafe_offset=row * s3], p4[unsafe_offset=row * s4], p5[unsafe_offset=row * s5], p6[unsafe_offset=row * s6]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        var v5 = call.view(5)
        var v6 = call.view(6)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        comptime r3 = _required[A4]()
        comptime r4 = _required[A5]()
        comptime r5 = _required[A6]()
        comptime r6 = _required[A7]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)) or (r6 and not _valid(v6, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row), call.get[A4](3, v3, row), call.get[A5](4, v4, row), call.get[A6](5, v5, row), call.get[A7](6, v6, row)))
    call.out.finish()

def _exec8[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run8[A1, A2, A3, A4, A5, A6, A7, A8, R, f](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run8[A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, R: Movable & Deinitable, f: def(A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> R](
    info: duckdb_v2_scalar_function_exec_info_handle,
) raises:
    var call = _Call(info)
    comptime if is_bind_data[A1]():
        var b = call.bind_data[A1]()
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        var v5 = call.view(5)
        var v6 = call.view(6)
        comptime r0 = _required[A2]()
        comptime r1 = _required[A3]()
        comptime r2 = _required[A4]()
        comptime r3 = _required[A5]()
        comptime r4 = _required[A6]()
        comptime r5 = _required[A7]()
        comptime r6 = _required[A8]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)) or (r6 and not _valid(v6, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(b[], call.get[A2](0, v0, row), call.get[A3](1, v1, row), call.get[A4](2, v2, row), call.get[A5](3, v3, row), call.get[A6](4, v4, row), call.get[A7](5, v5, row), call.get[A8](6, v6, row)))
    else:
        comptime if _all_numbers[A1, A2, A3, A4, A5, A6, A7, A8, R]():
            if call.all_flat():
                var p0 = call.numbers[A1](0)
                var p1 = call.numbers[A2](1)
                var p2 = call.numbers[A3](2)
                var p3 = call.numbers[A4](3)
                var p4 = call.numbers[A5](4)
                var p5 = call.numbers[A6](5)
                var p6 = call.numbers[A7](6)
                var p7 = call.numbers[A8](7)
                var s0 = call.step(0)
                var s1 = call.step(1)
                var s2 = call.step(2)
                var s3 = call.step(3)
                var s4 = call.step(4)
                var s5 = call.step(5)
                var s6 = call.step(6)
                var s7 = call.step(7)
                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()
                for row in range(call.size):
                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f(p0[unsafe_offset=row * s0], p1[unsafe_offset=row * s1], p2[unsafe_offset=row * s2], p3[unsafe_offset=row * s3], p4[unsafe_offset=row * s4], p5[unsafe_offset=row * s5], p6[unsafe_offset=row * s6], p7[unsafe_offset=row * s7]))
                call.out.finish()
                return
        var v0 = call.view(0)
        var v1 = call.view(1)
        var v2 = call.view(2)
        var v3 = call.view(3)
        var v4 = call.view(4)
        var v5 = call.view(5)
        var v6 = call.view(6)
        var v7 = call.view(7)
        comptime r0 = _required[A1]()
        comptime r1 = _required[A2]()
        comptime r2 = _required[A3]()
        comptime r3 = _required[A4]()
        comptime r4 = _required[A5]()
        comptime r5 = _required[A6]()
        comptime r6 = _required[A7]()
        comptime r7 = _required[A8]()
        for row in range(call.size):
            if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)) or (r6 and not _valid(v6, row)) or (r7 and not _valid(v7, row)):
                call.out.write_null(row)
                continue
            call.out.write(row, f(call.get[A1](0, v0, row), call.get[A2](1, v1, row), call.get[A3](2, v2, row), call.get[A4](3, v3, row), call.get[A5](4, v4, row), call.get[A6](5, v5, row), call.get[A7](6, v6, row), call.get[A8](7, v7, row)))
    call.out.finish()


# END GENERATED exec
