"""Aggregate functions written in Mojo.

An aggregate is a state struct plus an `update` method:

```mojo
struct Avg(AggregateState):
    comptime Result = Float64
    var sum: Float64
    var n: Int

    def __init__(out self):
        self.sum = 0
        self.n = 0

    def update(mut self, x: Float64):
        self.sum += x
        self.n += 1

    def combine(mut self, ref other: Self):
        self.sum += other.sum
        self.n += other.n

    def finalize(self) -> Float64:
        return self.sum / Float64(self.n)

con.register(AggregateFunction.from_state["my_avg", Avg.update]())
```

`from_state` reads the SQL signature from `update` (1 to 8 arguments after
`self`) and the return type from `Result`. Every group starts from
`__init__`; `combine` merges another state into this one (starting from a
fresh state, so it must treat `__init__`'s state as empty). `combine` borrows
`other`: it stays alive after the call and DuckDB destroys it. It works with
`GROUP BY`.

NULLs: rows with a NULL argument are skipped, unless the argument is an
`Optional`. A group without any aggregated row finalizes to NULL.

Speed: a state that also conforms to `BatchUpdate` gets whole arrays of
values at once when every row of a chunk goes to the same state (no
`GROUP BY`) and the input has no NULLs. The ready-made `from_sum`,
`from_min`, `from_max`, `from_mean`, `from_product` and `from_reduce` use
SIMD reductions this way.

DuckDB 2.0 cannot overload aggregates: register each name once.
"""

from std.builtin.rebind import downcast, rebind_var
from std.reflection import reflect
from std.memory.alloc import unsafe_alloc
from std.sys import align_of, bit_width_of, simd_width_of, size_of
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb._runtime import api_error, check, lib
from duckdb.error import DuckDBError
from duckdb.functions import BindArgs, BindData, Registrar, _Args, _NodePtr, _identity_user_data, _param, _report, _required, _valid
from duckdb.serde import VectorWriter, logical_type_of
from duckdb.signature import Signature
from duckdb.types import LogicalType
from duckdb.value import Value


trait AggregateState(Defaultable, Deinitable, Movable):
    """The state of one group of an aggregate (see the module docs)."""

    comptime Result: Movable & Deinitable
    """What `finalize` returns: any mapped type."""

    def combine(mut self, ref other: Self) raises:
        """Merge `other` into this state (`other` is borrowed and stays alive)."""
        ...

    def finalize(self) raises -> Self.Result:
        """The aggregate's value for this group."""
        ...


trait AggregateBind:
    """An aggregate state that uses bind data: values computed once per call
    when the query is planned, e.g. from constant arguments.

    ```mojo
    @fieldwise_init
    struct Sep(BindData, Copyable):
        var text: String

        @staticmethod
        def bind(args: BindArgs) raises -> Self:
            return Self(args.constant[String](1).or_else(","))

    struct Join(AggregateBind, AggregateState):
        comptime Bind = Sep
        ...
        def init_from(mut self, ref bind: Sep):
            self.sep = bind.text
    ```
    """

    comptime Bind: BindData

    def init_from(mut self, ref bind: Self.Bind):
        """Called on every new state, right after `__init__`."""
        ...


trait BatchUpdate:
    """An aggregate state that can take a whole array of values at once."""

    comptime Input: DType

    def update_batch(mut self, values: Span[Scalar[Self.Input], ImmUntrackedOrigin]):
        """Aggregate every value (none of them is NULL)."""
        ...


struct _Slot[S: AggregateState](Movable):
    """What DuckDB stores per group: the state and whether it saw a row."""

    var seen: Bool
    var state: Self.S

    def __init__(out self):
        self.seen = False
        self.state = Self.S()


struct AggregateFunction(Movable):
    """An aggregate function: name, signature, properties and callbacks."""

    var name: String
    var signature: Signature
    var result: LogicalType
    var special_nulls: Bool
    var order_dependent: Bool
    """Whether the result depends on the order of the rows (`string_agg`, ...)."""
    var _bind: duckdb_v2_aggregate_function_bind_callback_fn
    var _size: duckdb_v2_aggregate_function_size_callback_fn
    var _init: duckdb_v2_aggregate_function_init_callback_fn
    var _update: duckdb_v2_aggregate_function_update_callback_fn
    var _combine: duckdb_v2_aggregate_function_combine_callback_fn
    var _finalize: duckdb_v2_aggregate_function_finalize_callback_fn
    var _destroy: duckdb_v2_aggregate_function_destroy_callback_fn

    def __init__[S: AggregateState](
        out self,
        var name: String,
        var params: List[LogicalType],
        update: duckdb_v2_aggregate_function_update_callback_fn,
        special_nulls: Bool,
        order_dependent: Bool,
    ) raises DuckDBError:
        self.name = name^
        self.signature = Signature(params, fixed=True)
        self.result = logical_type_of[S.Result]()
        self.special_nulls = special_nulls
        self.order_dependent = order_dependent
        comptime if conforms_to(S, AggregateBind):
            self._bind = _agg_bind_cb[S]
        else:
            self._bind = _identity_bind
        self._size = _size_cb[S]
        self._init = _init_cb[S]
        self._update = update
        self._combine = _combine_cb[S]
        self._finalize = _finalize_cb[S]
        self._destroy = _destroy_cb[S]

    def _register[R: Registrar](self, registrar: R) raises DuckDBError:
        var err = null_error_info()
        var function = null[duckdb_v2_aggregate_function_handle]()
        check(lib().duckdb_v2_aggregate_function_create(registrar.factory(), Pointer(to=function), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_aggregate_function_set_name(function, name.ptr(), Pointer(to=err)), err)
            var signature = null[duckdb_v2_function_signature_handle]()
            check(lib().duckdb_v2_aggregate_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
            self.signature._add_to(signature)
            check(lib().duckdb_v2_function_signature_set_return_type(signature, self.result.handle(), Pointer(to=err)), err)
            if self.special_nulls:
                check(
                    lib().duckdb_v2_aggregate_function_set_property(
                        function,
                        DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING,
                        DUCKDB_V2_FUNCTION_PROPERTY_NULL_HANDLING_SPECIAL,
                        Pointer(to=err),
                    ),
                    err,
                )
            if self.order_dependent:
                check(
                    lib().duckdb_v2_aggregate_function_set_property(
                        function,
                        DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT,
                        DUCKDB_V2_FUNCTION_PROPERTY_AGG_ORDER_DEPENDENT_YES,
                        Pointer(to=err),
                    ),
                    err,
                )
            var identity = _identity_user_data(self.name)
            check(lib().duckdb_v2_aggregate_function_set_user_data(function, Pointer(to=identity), Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_bind_callback(function, self._bind, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_size_callback(function, self._size, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_init_callback(function, self._init, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_update_callback(function, self._update, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_combine_callback(function, self._combine, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_finalize_callback(function, self._finalize, Pointer(to=err)), err)
            check(lib().duckdb_v2_aggregate_function_set_destroy_callback(function, self._destroy, Pointer(to=err)), err)
            registrar._register_aggregate_handle(function)
        finally:
            _ = lib().duckdb_v2_aggregate_function_destroy(Pointer(to=function))


    # ── Signature ─────────────────────────────────────────────────

    def named(var self, *names: String) raises DuckDBError -> Self:
        """Name the parameters in order, so callers can pass them by name."""
        var list = List[String]()
        for n in names:
            list.append(n)
        self.signature.name(list^)
        return self^

    def default[T: Movable & Deinitable](var self, name: StringSlice, value: T) raises DuckDBError -> Self:
        """Make parameter `name` optional, with `value` when the call omits it."""
        self.signature.set_default(name, Value(value))
        return self^

    def named_only(var self, *names: String) raises DuckDBError -> Self:
        """Parameters that can only be passed by name (`f(1, scale := 2)`)."""
        for n in names:
            self.signature.set_named_only(n)
        return self^

    # ── Ready-made SIMD reductions ────────────────────────────────

    @staticmethod
    def from_reduce[
        name: StringLiteral,
        D: DType,
        reduce_fn: def[width: SIMDLength](SIMD[D, width], SIMD[D, width]) thin -> SIMD[D, width],
        init_fn: def() thin -> Scalar[D],
    ]() raises DuckDBError -> AggregateFunction:
        """An aggregate that folds its `D` values with `reduce_fn`, starting
        from `init_fn()`, vectorized. `reduce_fn` must be associative and
        commutative (rows arrive in any order and groups are merged)."""
        return Self.from_state[name, _Reduce[D, reduce_fn, init_fn].update]()

    @staticmethod
    def from_sum[name: StringLiteral, D: DType]() raises DuckDBError -> AggregateFunction:
        """Sum of `D` values.

        Integral `D` up to 64 bits accumulates in `Int128` and raises on
        overflow instead of wrapping: DuckDB's own `sum` promotes `BIGINT` to
        `HUGEINT` and errors on overflow, and a silently wrapping sum would
        return a wrong value where `sum` errors. Wider integral `D` (128-bit)
        has no wider native accumulator. DuckDB's `sum` wraps there too.
        Floating-point `D` sums directly.
        """
        comptime if D.is_integral() and bit_width_of[D]() <= 64:
            return Self.from_state[name, _Sum[D].update]()
        else:
            return Self.from_state[name, _Builtin[D, _SUM].update]()

    @staticmethod
    def from_product[name: StringLiteral, D: DType]() raises DuckDBError -> AggregateFunction:
        return Self.from_state[name, _Builtin[D, _PRODUCT].update]()

    @staticmethod
    def from_min[name: StringLiteral, D: DType]() raises DuckDBError -> AggregateFunction:
        return Self.from_state[name, _Builtin[D, _MIN].update]()

    @staticmethod
    def from_max[name: StringLiteral, D: DType]() raises DuckDBError -> AggregateFunction:
        return Self.from_state[name, _Builtin[D, _MAX].update]()

    @staticmethod
    def from_mean[name: StringLiteral, D: DType]() raises DuckDBError -> AggregateFunction where D.is_floating_point():
        """The arithmetic mean of `D` values (a `D` result)."""
        return Self.from_state[name, _Mean[D].update]()

    # BEGIN GENERATED from_state (scripts/generate_scalar_functions.py)
    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        """An aggregate from a state type and its `update` method (see the module docs)."""
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update1[S, A1, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update2[S, A1, A2, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update3[S, A1, A2, A3, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3, A4) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update4[S, A1, A2, A3, A4, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3, A4, A5) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update5[S, A1, A2, A3, A4, A5, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3, A4, A5, A6) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update6[S, A1, A2, A3, A4, A5, A6, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3, A4, A5, A6, A7) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        _param[A7](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update7[S, A1, A2, A3, A4, A5, A6, A7, update], special_nulls, order_dependent)

    @staticmethod
    def from_state[
        S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, //,
        name: StringLiteral,
        update: def(mut S, A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> None,
    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:
        var params = List[LogicalType]()
        var special_nulls = False
        _param[A1](params, special_nulls)
        _param[A2](params, special_nulls)
        _param[A3](params, special_nulls)
        _param[A4](params, special_nulls)
        _param[A5](params, special_nulls)
        _param[A6](params, special_nulls)
        _param[A7](params, special_nulls)
        _param[A8](params, special_nulls)
        return AggregateFunction.__init__[S](String(name), params^, _update8[S, A1, A2, A3, A4, A5, A6, A7, A8, update], special_nulls, order_dependent)
    # END GENERATED from_state


# ===--------------------------------------------------------------------===#
# Callbacks shared by every arity
# ===--------------------------------------------------------------------===#


def _identity_bind(
    info: duckdb_v2_function_bind_info_handle,
    result: duckdb_v2_aggregate_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    """Give every call bind data holding the function's name.

    DuckDB decides whether two aggregate calls are the same from their C++
    callbacks, argument types, return type and bind data. Every C API
    aggregate shares the same C++ callbacks, so without bind data two
    different aggregates over the same column (`SELECT f(x), g(x)`) would be
    computed once and both return one result.
    """
    var e = null_error_info()
    var user = null[Pointer[NoneType, MutUntrackedOrigin]]()
    if lib().duckdb_v2_function_bind_get_user_data(info, Pointer(to=user), Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))
        _report(err, "could not read the aggregate's user data", DUCKDB_V2_ERROR_QUERY_BINDER)
        return
    var data = _identity_user_data(user.unsafe_bitcast[String]()[].copy())
    if lib().duckdb_v2_function_bind_set_bind_data(info, Pointer(to=data), Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))
        data.destroy(data.ptr)
        _report(err, "could not set the aggregate's bind data", DUCKDB_V2_ERROR_QUERY_BINDER)


struct _AggBind[B: BindData](Movable):
    """Bind data of an aggregate with `AggregateBind`: its name (see
    `_identity_bind`) and the user's bind data."""

    var name: String
    var data: Self.B

    def __init__(out self, var name: String, var data: Self.B):
        self.name = name^
        self.data = data^


def _destroy_agg_bind[B: BindData](ptr: Pointer[NoneType, MutUntrackedOrigin]) abi("C"):
    var p = ptr.unsafe_bitcast[_AggBind[B]]()
    p.unsafe_deinit_pointee()
    p.unsafe_free()


def _equal_agg_bind[B: BindData](a: Pointer[NoneType, MutUntrackedOrigin], b: Pointer[NoneType, MutUntrackedOrigin]) abi("C") -> Bool:
    ref x = a.unsafe_bitcast[_AggBind[B]]()[]
    ref y = b.unsafe_bitcast[_AggBind[B]]()[]
    if x.name != y.name:
        return False
    comptime if conforms_to(B, Equatable):
        comptime E = downcast[B, Equatable]
        return Pointer(to=x.data).unsafe_bitcast[E]()[] == Pointer(to=y.data).unsafe_bitcast[E]()[]
    else:
        return Int(a) == Int(b)


def _agg_bind_cb[S: AggregateState](
    info: duckdb_v2_function_bind_info_handle,
    result: duckdb_v2_aggregate_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_agg_bind[S](info, context)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _run_agg_bind[S: AggregateState](info: duckdb_v2_function_bind_info_handle, context: duckdb_v2_context_handle) raises:
    comptime B = downcast[S, AggregateBind].Bind
    var e = null_error_info()
    var user = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_function_bind_get_user_data(info, Pointer(to=user), Pointer(to=e)), e)
    var bound = B.bind(BindArgs(info, context))
    var ptr = unsafe_alloc[_AggBind[B]](1)
    ptr.unsafe_write(_AggBind[B](user.unsafe_bitcast[String]()[].copy(), bound^))
    var data = duckdb_v2_opaque(ptr.unsafe_bitcast[NoneType](), _destroy_agg_bind[B], _equal_agg_bind[B])
    var code = lib().duckdb_v2_function_bind_set_bind_data(info, Pointer(to=data), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        _destroy_agg_bind[B](data.ptr)
        check(code, e)


@always_inline
def _slot[S: AggregateState](p: Pointer[NoneType, MutUntrackedOrigin]) -> ref[MutUntrackedOrigin] _Slot[S]:
    return p.unsafe_bitcast[_Slot[S]]()[]


def _size_cb[S: AggregateState](
    info: duckdb_v2_aggregate_function_size_info_handle, err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]
) abi("C"):
    # DuckDB aligns states to 8 bytes.
    comptime assert align_of[_Slot[S]]() <= 8, "aggregate states may need at most 8-byte alignment"
    var e = null_error_info()
    if lib().duckdb_v2_aggregate_function_size_set_state_size(info, idx_t(size_of[_Slot[S]]()), Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))
        _report(err, "could not set the aggregate state size", DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _init_cb[S: AggregateState](
    info: duckdb_v2_aggregate_function_init_info_handle, err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]
) abi("C"):
    try:
        var e = null_error_info()
        var count: idx_t = 0
        check(lib().duckdb_v2_aggregate_function_init_get_state_count(info, Pointer(to=count), Pointer(to=e)), e)
        var states = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
        check(lib().duckdb_v2_aggregate_function_init_get_states(info, Pointer(to=states), Pointer(to=e)), e)
        for i in range(Int(count)):
            states[unsafe_offset=i].unsafe_bitcast[_Slot[S]]().unsafe_write(_Slot[S]())
        comptime if conforms_to(S, AggregateBind):
            comptime A = downcast[S, AggregateBind]
            var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
            check(lib().duckdb_v2_aggregate_function_init_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
            ref data = bind.unsafe_bitcast[_AggBind[A.Bind]]()[].data
            for i in range(Int(count)):
                ref slot = _slot[S](states[unsafe_offset=i])
                Pointer(to=slot.state).unsafe_bitcast[A]()[].init_from(data)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _destroy_cb[S: AggregateState](
    info: duckdb_v2_aggregate_function_destroy_info_handle, err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]
) abi("C"):
    try:
        var e = null_error_info()
        var count: idx_t = 0
        check(lib().duckdb_v2_aggregate_function_destroy_get_state_count(info, Pointer(to=count), Pointer(to=e)), e)
        var states = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
        check(lib().duckdb_v2_aggregate_function_destroy_get_states(info, Pointer(to=states), Pointer(to=e)), e)
        for i in range(Int(count)):
            states[unsafe_offset=i].unsafe_bitcast[_Slot[S]]().unsafe_deinit_pointee()
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _combine_cb[S: AggregateState](
    info: duckdb_v2_aggregate_function_combine_info_handle, err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]
) abi("C"):
    try:
        _run_combine[S](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_combine[S: AggregateState](info: duckdb_v2_aggregate_function_combine_info_handle) raises:
    var e = null_error_info()
    var count: idx_t = 0
    check(lib().duckdb_v2_aggregate_function_combine_get_state_count(info, Pointer(to=count), Pointer(to=e)), e)
    var sources = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
    var targets = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
    check(lib().duckdb_v2_aggregate_function_combine_get_sources(info, Pointer(to=sources), Pointer(to=e)), e)
    check(lib().duckdb_v2_aggregate_function_combine_get_targets(info, Pointer(to=targets), Pointer(to=e)), e)
    for i in range(Int(count)):
        ref src = _slot[S](sources[unsafe_offset=i])
        if not src.seen:
            continue
        ref dst = _slot[S](targets[unsafe_offset=i])
        dst.state.combine(src.state)
        dst.seen = True


def _finalize_cb[S: AggregateState](
    info: duckdb_v2_aggregate_function_finalize_info_handle, err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin]
) abi("C"):
    try:
        _run_finalize[S](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_finalize[S: AggregateState](info: duckdb_v2_aggregate_function_finalize_info_handle) raises:
    var e = null_error_info()
    var count: idx_t = 0
    check(lib().duckdb_v2_aggregate_function_finalize_get_state_count(info, Pointer(to=count), Pointer(to=e)), e)
    var states = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
    check(lib().duckdb_v2_aggregate_function_finalize_get_states(info, Pointer(to=states), Pointer(to=e)), e)
    var offset: idx_t = 0
    check(lib().duckdb_v2_aggregate_function_finalize_get_result_offset(info, Pointer(to=offset), Pointer(to=e)), e)
    var result = null[duckdb_v2_vector_handle]()
    check(lib().duckdb_v2_aggregate_function_finalize_get_result(info, Pointer(to=result), Pointer(to=e)), e)
    var out = VectorWriter(result, Int(offset + count), grow_only=True)
    for i in range(Int(count)):
        ref slot = _slot[S](states[unsafe_offset=i])
        var row = Int(offset) + i
        if slot.seen:
            out.write(row, slot.state.finalize())
        else:
            out.write_null(row)
    out.finish()


struct _UpdateCall(Movable):
    """One update callback: the argument trees and the state of each row."""

    var size: Int
    var a: _Args
    var states: Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]

    def __init__(out self, info: duckdb_v2_aggregate_function_update_info_handle) raises DuckDBError:
        var e = null_error_info()
        var count: idx_t = 0
        check(lib().duckdb_v2_aggregate_function_update_get_row_count(info, Pointer(to=count), Pointer(to=e)), e)
        self.size = Int(count)
        var fixed: idx_t = 0
        var variadic: idx_t = 0
        var named: idx_t = 0
        var named_variadic: idx_t = 0
        check(
            lib().duckdb_v2_aggregate_function_update_get_arg_count(
                info, Pointer(to=fixed), Pointer(to=variadic), Pointer(to=named), Pointer(to=named_variadic), Pointer(to=e)
            ),
            e,
        )
        var vectors = List[duckdb_v2_vector_handle]()
        for i in range(Int(fixed + variadic + named + named_variadic)):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_aggregate_function_update_get_arg(info, idx_t(i), Pointer(to=vec), Pointer(to=e)), e)
            vectors.append(vec)
        self.a = _Args(vectors, self.size)
        self.states = null[Pointer[Pointer[NoneType, MutUntrackedOrigin], MutUntrackedOrigin]]()
        check(lib().duckdb_v2_aggregate_function_update_get_states(info, Pointer(to=self.states), Pointer(to=e)), e)

    def one_state(self) -> Bool:
        """Whether every row goes to the same state (an aggregate without GROUP BY)."""
        if self.size == 0:
            return False
        var first = Int(self.states[unsafe_offset=0])
        for i in range(1, self.size):
            if Int(self.states[unsafe_offset=i]) != first:
                return False
        return True

    def flat_without_nulls(self, arg: Int) -> Bool:
        ref n = self.a.args[arg].nodes[1]
        if Int(n.sel) != 0:
            return False
        if Int(n.validity) != 0:
            for row in range(self.size):
                if not self.a.args[arg].is_valid(1, row):
                    return False
        return True


def _batchable[S: AggregateState, A: AnyType]() -> Bool:
    comptime if conforms_to(S, BatchUpdate):
        return reflect[A].name() == reflect[Scalar[downcast[S, BatchUpdate].Input]].name()
    else:
        return False


def _batch[S: AggregateState](mut call: _UpdateCall) raises:
    """Feed the whole (flat, NULL-free) argument to the one state."""
    comptime B = downcast[S, BatchUpdate]
    ref slot = _slot[S](call.states[unsafe_offset=0])
    var data = call.a.args[0].nodes[1].data.unsafe_bitcast[Scalar[B.Input]]()
    var values = Span[Scalar[B.Input], ImmUntrackedOrigin](unsafe_ptr=data, length=call.size)
    Pointer(to=slot.state).unsafe_bitcast[B]()[].update_batch(values)
    slot.seen = True


# ===--------------------------------------------------------------------===#
# Ready-made states
# ===--------------------------------------------------------------------===#


comptime _SUM = 0
comptime _PRODUCT = 1
comptime _MIN = 2
comptime _MAX = 3


@always_inline
def _apply[D: DType, op: Int, width: SIMDLength](a: SIMD[D, width], b: SIMD[D, width]) -> SIMD[D, width]:
    comptime if op == _SUM:
        return a + b
    elif op == _PRODUCT:
        return a * b
    elif op == _MIN:
        return min(a, b)
    else:
        return max(a, b)


def _identity[D: DType, op: Int]() -> Scalar[D]:
    comptime if op == _SUM:
        return 0
    elif op == _PRODUCT:
        return 1
    elif op == _MIN:
        return Scalar[D].MAX
    else:
        return Scalar[D].MIN


struct _Builtin[D: DType, op: Int](AggregateState, BatchUpdate):
    """sum, product, min and max, selected by `op`."""

    comptime Result = Scalar[Self.D]
    comptime Input = Self.D
    var acc: Scalar[Self.D]

    def __init__(out self):
        self.acc = _identity[Self.D, Self.op]()

    def update(mut self, x: Scalar[Self.D]):
        self.acc = _apply[Self.D, Self.op](self.acc, x)

    def update_batch(mut self, values: Span[Scalar[Self.D], ImmUntrackedOrigin]):
        # Loads/stores keep the default (scalar) alignment: DuckDB does not
        # guarantee a vector buffer is aligned to the SIMD width, and a
        # misaligned wider access would fault. LLVM may still merge the lanes.
        comptime w = simd_width_of[Self.D]()
        var n = len(values)
        var p = values.unsafe_ptr()
        var i = 0
        if n >= w:
            var acc = SIMD[Self.D, w](_identity[Self.D, Self.op]())
            while i + w <= n:
                acc = _apply[Self.D, Self.op](acc, p.unsafe_offset(i).unsafe_load[width=w]())
                i += w
            comptime for k in range(w):
                self.acc = _apply[Self.D, Self.op](self.acc, acc[k])
        while i < n:
            self.acc = _apply[Self.D, Self.op](self.acc, p[unsafe_offset=i])
            i += 1

    def combine(mut self, ref other: Self):
        self.acc = _apply[Self.D, Self.op](self.acc, other.acc)

    def finalize(self) -> Scalar[Self.D]:
        return self.acc


struct _Sum[D: DType](AggregateState):
    """Integer sum: accumulates in `Int128`, raises on overflow (see `from_sum`).

    Not a `BatchUpdate`: 64-bit integer sums are memory-bound, and an Int128
    accumulator has no useful SIMD form here. The accumulator is stored as
    two 64-bit halves: aggregate states must not exceed 8-byte alignment (the
    size callback asserts it), and `Int128` is 16-byte aligned.
    """

    comptime Result = Scalar[Self.D]
    var lo: UInt64
    var hi: Int64

    def __init__(out self):
        self.lo = 0
        self.hi = 0

    def _acc(self) -> Int128:
        return (Int128(self.hi) << 64) | Int128(self.lo)

    def _store(mut self, var acc: Int128):
        self.lo = acc.cast[DType.uint64]()
        self.hi = (acc >> 64).cast[DType.int64]()

    def update(mut self, x: Scalar[Self.D]):
        # A 64-bit-or-narrower value widened to Int128 cannot come close to
        # overflowing the accumulator for any realistic number of rows.
        self._store(self._acc() + x.cast[DType.int128]())

    def combine(mut self, ref other: Self):
        self._store(self._acc() + other._acc())

    # Matches `AggregateState.finalize` (untyped `raises`): a trait method
    # must keep the trait's raise type.
    def finalize(self) raises -> Scalar[Self.D]:
        var acc = self._acc()
        if acc < Scalar[Self.D].MIN.cast[DType.int128]() or acc > Scalar[Self.D].MAX.cast[DType.int128]():
            raise api_error(String("sum overflow: the result does not fit in ", String(Self.D)))
        return Scalar[Self.D](acc.cast[Self.D]())


struct _Reduce[
    D: DType,
    reduce_fn: def[width: SIMDLength](SIMD[D, width], SIMD[D, width]) thin -> SIMD[D, width],
    init_fn: def() thin -> Scalar[D],
](AggregateState, BatchUpdate):
    comptime Result = Scalar[Self.D]
    comptime Input = Self.D
    var acc: Scalar[Self.D]

    def __init__(out self):
        self.acc = Self.init_fn()

    def update(mut self, x: Scalar[Self.D]):
        self.acc = Self.reduce_fn(self.acc, x)

    def update_batch(mut self, values: Span[Scalar[Self.D], ImmUntrackedOrigin]):
        # Default (scalar) load/store alignment on purpose; see `_Builtin`.
        comptime w = simd_width_of[Self.D]()
        var n = len(values)
        var p = values.unsafe_ptr()
        var i = 0
        if n >= w:
            var acc = SIMD[Self.D, w](Self.init_fn())
            while i + w <= n:
                acc = Self.reduce_fn(acc, p.unsafe_offset(i).unsafe_load[width=w]())
                i += w
            comptime for k in range(w):
                self.acc = Self.reduce_fn(self.acc, acc[k])
        while i < n:
            self.acc = Self.reduce_fn(self.acc, p[unsafe_offset=i])
            i += 1

    def combine(mut self, ref other: Self):
        self.acc = Self.reduce_fn(self.acc, other.acc)

    def finalize(self) -> Scalar[Self.D]:
        return self.acc


struct _Mean[D: DType](AggregateState, BatchUpdate):
    comptime Result = Scalar[Self.D]
    comptime Input = Self.D
    var sum: Scalar[Self.D]
    var count: Int

    def __init__(out self):
        self.sum = 0
        self.count = 0

    def update(mut self, x: Scalar[Self.D]):
        self.sum += x
        self.count += 1

    def update_batch(mut self, values: Span[Scalar[Self.D], ImmUntrackedOrigin]):
        # Default (scalar) load/store alignment on purpose; see `_Builtin`.
        comptime w = simd_width_of[Self.D]()
        var n = len(values)
        var p = values.unsafe_ptr()
        var acc = SIMD[Self.D, w](0)
        var i = 0
        while i + w <= n:
            acc += p.unsafe_offset(i).unsafe_load[width=w]()
            i += w
        var total = acc.reduce_add()
        while i < n:
            total += p[unsafe_offset=i]
            i += 1
        self.sum += total
        self.count += n

    def combine(mut self, ref other: Self):
        self.sum += other.sum
        self.count += other.count

    def finalize(self) -> Scalar[Self.D]:
        return self.sum / Scalar[Self.D](self.count)


# BEGIN GENERATED update (scripts/generate_scalar_functions.py)


def _update1[S: AggregateState, A1: Movable & Deinitable, update: def(mut S, A1) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update1[S, A1, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update1[S: AggregateState, A1: Movable & Deinitable, update: def(mut S, A1) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    comptime if _batchable[S, A1]():
        if call.one_state() and call.flat_without_nulls(0):
            _batch[S](call)
            return
    var v0 = call.a.view(0)
    comptime r0 = _required[A1]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row))
        slot.seen = True


def _update2[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, update: def(mut S, A1, A2) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update2[S, A1, A2, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update2[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, update: def(mut S, A1, A2) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row))
        slot.seen = True


def _update3[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, update: def(mut S, A1, A2, A3) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update3[S, A1, A2, A3, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update3[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, update: def(mut S, A1, A2, A3) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    comptime r2 = _required[A3]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row))
        slot.seen = True


def _update4[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update4[S, A1, A2, A3, A4, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update4[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    var v3 = call.a.view(3)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    comptime r2 = _required[A3]()
    comptime r3 = _required[A4]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row), call.a.get[A4](3, v3, row))
        slot.seen = True


def _update5[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update5[S, A1, A2, A3, A4, A5, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update5[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    var v3 = call.a.view(3)
    var v4 = call.a.view(4)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    comptime r2 = _required[A3]()
    comptime r3 = _required[A4]()
    comptime r4 = _required[A5]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row), call.a.get[A4](3, v3, row), call.a.get[A5](4, v4, row))
        slot.seen = True


def _update6[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update6[S, A1, A2, A3, A4, A5, A6, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update6[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    var v3 = call.a.view(3)
    var v4 = call.a.view(4)
    var v5 = call.a.view(5)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    comptime r2 = _required[A3]()
    comptime r3 = _required[A4]()
    comptime r4 = _required[A5]()
    comptime r5 = _required[A6]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row), call.a.get[A4](3, v3, row), call.a.get[A5](4, v4, row), call.a.get[A6](5, v5, row))
        slot.seen = True


def _update7[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6, A7) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update7[S, A1, A2, A3, A4, A5, A6, A7, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update7[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6, A7) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    var v3 = call.a.view(3)
    var v4 = call.a.view(4)
    var v5 = call.a.view(5)
    var v6 = call.a.view(6)
    comptime r0 = _required[A1]()
    comptime r1 = _required[A2]()
    comptime r2 = _required[A3]()
    comptime r3 = _required[A4]()
    comptime r4 = _required[A5]()
    comptime r5 = _required[A6]()
    comptime r6 = _required[A7]()
    for row in range(call.size):
        if (r0 and not _valid(v0, row)) or (r1 and not _valid(v1, row)) or (r2 and not _valid(v2, row)) or (r3 and not _valid(v3, row)) or (r4 and not _valid(v4, row)) or (r5 and not _valid(v5, row)) or (r6 and not _valid(v6, row)):
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row), call.a.get[A4](3, v3, row), call.a.get[A5](4, v4, row), call.a.get[A6](5, v5, row), call.a.get[A7](6, v6, row))
        slot.seen = True


def _update8[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_update8[S, A1, A2, A3, A4, A5, A6, A7, A8, update](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_update8[S: AggregateState, A1: Movable & Deinitable, A2: Movable & Deinitable, A3: Movable & Deinitable, A4: Movable & Deinitable, A5: Movable & Deinitable, A6: Movable & Deinitable, A7: Movable & Deinitable, A8: Movable & Deinitable, update: def(mut S, A1, A2, A3, A4, A5, A6, A7, A8) raises thin -> None](
    info: duckdb_v2_aggregate_function_update_info_handle,
) raises:
    var call = _UpdateCall(info)
    var v0 = call.a.view(0)
    var v1 = call.a.view(1)
    var v2 = call.a.view(2)
    var v3 = call.a.view(3)
    var v4 = call.a.view(4)
    var v5 = call.a.view(5)
    var v6 = call.a.view(6)
    var v7 = call.a.view(7)
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
            continue
        ref slot = _slot[S](call.states[unsafe_offset=row])
        update(slot.state, call.a.get[A1](0, v0, row), call.a.get[A2](1, v1, row), call.a.get[A3](2, v2, row), call.a.get[A4](3, v3, row), call.a.get[A5](4, v4, row), call.a.get[A6](5, v5, row), call.a.get[A7](6, v6, row), call.a.get[A8](7, v7, row))
        slot.seen = True


# END GENERATED update
