"""Table functions written in Mojo.

A table function is a struct conforming to `TableFunctionImpl`. Its `bind`
reads the arguments, declares the output columns and returns the struct,
which all scans share read-only. Each scan thread gets its own `State` from
`init`, and `scan` fills one batch of rows at a time:

```mojo
@fieldwise_init
struct Squares(TableFunctionImpl):
    comptime State = Counter
    var n: Int64

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        columns.add[Int64]("i")
        columns.add[Int64]("square")
        return Self(args.constant[Int64](0).value())

    def init(ref self, shared: ScanShared) raises -> Counter:
        return Counter(0)

    def scan(ref self, mut state: Counter, mut out: TableOutput) raises -> Int:
        var rows = 0
        while rows < out.capacity() and state.next < self.n:
            out.write(rows, 0, state.next)
            out.write(rows, 1, state.next * state.next)
            state.next += 1
            rows += 1
        return rows

con.register(TableFunction.from_impl[Squares]("squares", param_types[Int64]()))
# SELECT * FROM squares(10)
```

Output: columns are declared in `bind` (`columns.add[T](name)`, or
`columns.add_row[Row]()` for every field of a struct). `out.write(row, col,
value)` and `out.write_row(row, value)` write by declared column. Columns the
query does not use are skipped (projection pushdown), `out.wants(col)` tells
which those are.

Parallel scans: `max_threads` (default 1) lets DuckDB run several scans at
once, each with its own state. `ScanShared.claim()` hands out work units
(0, 1, 2, ...) across threads, so the scans can split the work. By default
DuckDB keeps the rows in scan order, which limits parallelism. Declare the
rows unordered (`columns.set_unordered()`) or number the batches
(`OrderedBatches`). Optional traits: `FilterPushdown`, `ScanProgress`,
`OrderedBatches`.
"""

from std.atomic import Atomic
from std.builtin.rebind import downcast
from std.memory.alloc import unsafe_alloc
from std.reflection import reflect
from emberserde.field_meta import is_skipped, static_wire_name
from duckdb._libduckdb_v2 import *
from duckdb._libduckdb_v2 import _duckdb_v2_table_function_filter_pushdown_info
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb._runtime import check, lib
from duckdb.error import DuckDBError, ErrorCode
from duckdb.expression import Expression
from duckdb.functions import BindArgs, Registrar, _destroy_bind, _identity_user_data, _report
from duckdb.mapping import ColumnMeta, MutRawData
from duckdb.serde import _WNode, _WriteTree, _Writer, _from_ser, _vector_type, _write, logical_type_of
from duckdb.signature import ParamKind, Signature
from duckdb.types import LogicalType, TypeId
from duckdb.value import Value


def param_types[*Ts: AnyType]() raises DuckDBError -> List[LogicalType]:
    """The DuckDB types of the given Mojo types, for function parameters."""
    var out = List[LogicalType]()
    comptime for i in range(Ts.length):
        out.append(logical_type_of[Ts[i]]())
    return out^


trait TableFunctionImpl(Deinitable, Movable):
    """A table function: its bind data, and how it scans (see the module docs)."""

    comptime State: Movable & Deinitable
    """The state of one scan thread."""

    @staticmethod
    def bind(args: BindArgs, mut columns: TableColumns) raises -> Self:
        """Read the arguments and declare the output columns."""
        ...

    def init(ref self, shared: ScanShared) raises -> Self.State:
        """The state of a new scan thread."""
        ...

    def scan(ref self, mut state: Self.State, mut out: TableOutput) raises -> Int:
        """Write up to `out.capacity()` rows, return how many. 0 ends this thread's scan."""
        ...

    def max_threads(ref self) -> Int:
        """How many threads may scan at once (default 1)."""
        return 1


trait FilterPushdown:
    """A table function that can apply some of a query's filters itself.

    `pushdown` is called while the query is planned, after `bind` and before
    any scan, once per filter the query applies to the function's rows. Return
    True to take the filter over: record what it selects in `self` (the
    scans read it) and scan only matching rows. DuckDB then stops applying
    it. Return False to leave it to DuckDB. A filter may be offered again in
    a later optimizer pass if it was not taken.

    A function may also use a filter only to narrow its scan to a superset
    of the matching rows and return False: DuckDB then still applies the
    exact filter. That is the safe way to use `BETWEEN` filters (see
    `Between`).

    ```mojo
    def pushdown(mut self, filter: Expression) raises -> Bool:
        var c = filter.comparison()
        if c and c.value().column == 0 and c.value().op == ExprKind.LESS:
            self.limit = c.value().value.get[Int64]()
            return True
        return False
    ```
    """

    def pushdown(mut self, filter: Expression) raises -> Bool:
        ...


trait ScanProgress:
    """A table function that reports how far its scan is (DuckDB's progress bar).

    `progress` returns a fraction from 0.0 to 1.0. It may run on another
    thread while scans are running, so read only what is safe to read
    concurrently (`shared.claimed()` is).
    """

    def progress(ref self, shared: ScanShared) -> Float64:
        ...


trait OrderedBatches:
    """A parallel table function whose rows have an order across threads.

    Each `scan` calls `out.set_batch(i)` with the position of the batch it
    produced: batch numbers grow on each thread and differ between threads
    (a work unit from `ScanShared.claim()` works). DuckDB then puts the
    batches back in order, so results, `INSERT` and `COPY` keep the order
    while scanning in parallel. Without it a parallel scan either runs in
    insertion order on one thread or, after `columns.set_unordered()`, in
    parallel without order.
    """

    pass


struct TableColumns:
    """The output columns of a table function, declared in `bind`."""

    var _info: duckdb_v2_table_function_bind_info_handle
    var _names: List[String]

    def __init__(out self, info: duckdb_v2_table_function_bind_info_handle):
        self._info = info
        self._names = List[String]()

    def add(mut self, name: StringSlice, type: LogicalType) raises DuckDBError:
        self._names.append(String(name))
        var n = StrArg(name)
        var err = null_error_info()
        check(lib().duckdb_v2_table_function_bind_add_result_column(self._info, n.ptr(), type.handle(), Pointer(to=err)), err)

    def add[T: AnyType](mut self, name: StringSlice) raises DuckDBError:
        """A column of the DuckDB type of `T`."""
        self.add(name, logical_type_of[T]())

    def add_row[T: AnyType](mut self) raises DuckDBError:
        """One column per field of struct `T` (by wire name), in field order."""
        comptime r = reflect[T]
        comptime for i in range(r.field_count()):
            comptime if not is_skipped[T, i]():
                comptime FT = r.field_types()[i]
                self.add(static_wire_name[T, i](), logical_type_of[FT]())

    def set_unordered(self) raises DuckDBError:
        """The rows have no order, so DuckDB may scan in parallel into results,
        `INSERT` and `COPY` (see `max_threads`)."""
        var err = null_error_info()
        check(
            lib().duckdb_v2_table_function_bind_set_order_preservation(
                self._info, DUCKDB_V2_ORDER_PRESERVATION_NO_ORDER, Pointer(to=err)
            ),
            err,
        )

    def set_cardinality(self, rows: Int, exact: Bool = False) raises DuckDBError:
        """How many rows the scan will produce (a hint for the optimizer)."""
        var err = null_error_info()
        check(lib().duckdb_v2_table_function_bind_set_cardinality(self._info, idx_t(rows), exact, Pointer(to=err)), err)


struct _Bound[T: TableFunctionImpl](Movable):
    """The bind data DuckDB keeps: the implementation and its column names."""

    var impl: Self.T
    var names: List[String]

    def __init__(out self, var impl: Self.T, var names: List[String]):
        self.impl = impl^
        self.names = names^


struct _Local[S: Movable & Deinitable](Movable):
    """What DuckDB keeps per scan thread: the user's state and the batch the
    last scan reported (`OrderedBatches`)."""

    var state: Self.S
    var batch: Int

    def __init__(out self, var state: Self.S):
        self.state = state^
        self.batch = 0


struct _Shared(Movable):
    var next: Int64

    def __init__(out self):
        self.next = 0


@fieldwise_init
struct ScanShared(ImplicitlyCopyable):
    """State shared by all threads of one scan."""

    var _ptr: Pointer[_Shared, MutUntrackedOrigin]

    def claim(self) -> Int:
        """The next work unit: 0, 1, 2, ... across all threads, each handed out once."""
        var p = Pointer(to=self._ptr[].next).unsafe_origin_cast[MutUntrackedOrigin]()
        return Int(Atomic.fetch_add(p, Int64(1)))

    def claimed(self) -> Int:
        """How many work units have been claimed so far (safe from any thread)."""
        var p = Pointer(to=self._ptr[].next).unsafe_origin_cast[MutUntrackedOrigin]()
        return Int(Atomic.fetch_add(p, Int64(0)))


struct TableOutput(Movable):
    """The batch a scan writes into. Columns are addressed by their declared index."""

    var _tree: _WriteTree
    var _capacity: Int
    var _position: List[Int]  # declared column -> tree node (0: not requested)
    var _vectors: List[duckdb_v2_vector_handle]
    var _nodes: List[Int]  # tree node of each vector
    var _vector_of: List[Int]  # declared column -> output vector (-1: not requested)
    var _batch: Pointer[Int, MutUntrackedOrigin]  # this thread's batch number

    def __init__(out self, info: duckdb_v2_table_function_exec_info_handle, names: List[String]) raises DuckDBError:
        var err = null_error_info()
        var chunk = null[duckdb_v2_data_chunk_handle]()
        check(lib().duckdb_v2_table_function_exec_get_output_chunk(info, Pointer(to=chunk), Pointer(to=err)), err)
        var count: idx_t = 0
        check(lib().duckdb_v2_table_function_exec_get_column_count(info, Pointer(to=count), Pointer(to=err)), err)
        var declared = List[Int]()
        for i in range(Int(count)):
            var d: idx_t = 0
            check(lib().duckdb_v2_table_function_exec_get_column_index(info, idx_t(i), Pointer(to=d), Pointer(to=err)), err)
            declared.append(Int(d))
        self = Self(chunk, names, declared)

    def __init__(out self, chunk: duckdb_v2_data_chunk_handle, names: List[String], declared: List[Int]) raises DuckDBError:
        """Over an output chunk whose vector i holds declared column `declared[i]`."""
        var err = null_error_info()
        var cap: idx_t = 0
        check(lib().duckdb_v2_data_chunk_get_capacity(chunk, Pointer(to=cap), Pointer(to=err)), err)
        self._capacity = Int(cap)
        self._tree = _WriteTree()
        self._tree.nodes.append(_WNode(null[duckdb_v2_vector_handle](), ColumnMeta(kind=TypeId.STRUCT)))
        self._tree.nodes[0].lenient = True
        self._position = List[Int]()
        self._vectors = List[duckdb_v2_vector_handle]()
        self._nodes = List[Int]()
        self._vector_of = List[Int]()
        self._batch = null[Pointer[Int, MutUntrackedOrigin]]()
        for i in range(len(declared)):
            var vec = null[duckdb_v2_vector_handle]()
            check(lib().duckdb_v2_data_chunk_get_vector(chunk, idx_t(i), Pointer(to=vec), Pointer(to=err)), err)
            # Sized to the capacity so NULL writes pass the bounds check; the
            # real row count is set at the end.
            check(lib().duckdb_v2_vector_set_size(vec, cap, Pointer(to=err)), err)
            var node = self._tree._add(vec, _vector_type(vec))
            self._vectors.append(vec)
            self._nodes.append(node)
            self._tree.nodes[0].children.append(node)
            self._tree.nodes[0].lower_names.append(names[declared[i]].lower())
            while len(self._position) <= declared[i]:
                self._position.append(0)
                self._vector_of.append(-1)
            self._position[declared[i]] = node
            self._vector_of[declared[i]] = i

    def capacity(self) -> Int:
        """The most rows one batch can hold."""
        return self._capacity

    def set_batch(mut self, index: Int):
        """The position of this batch in the output order (see `OrderedBatches`)."""
        self._batch[] = index

    def wants(self, col: Int) -> Bool:
        """Whether the query uses declared column `col` (else writes to it are skipped)."""
        return col < len(self._position) and self._position[col] != 0

    def write[T: AnyType](mut self, row: Int, col: Int, value: T) raises DuckDBError:
        """Write `value` to declared column `col` at `row` (skipped if the query does not use the column)."""
        if not self.wants(col):
            return
        var w = _Writer(Pointer(to=self._tree).unsafe_origin_cast[MutUntrackedOrigin](), self._position[col], row)
        try:
            _write(w, value)
        except e:
            raise _from_ser(e, String("row ", row, ", column ", col))

    def write_null(mut self, row: Int, col: Int) raises DuckDBError:
        if self.wants(col):
            self._tree.set_null(self._position[col], row)

    def data[T: TrivialRegisterPassable](mut self, col: Int) raises DuckDBError -> Optional[Pointer[T, MutUntrackedOrigin]]:
        """Direct access to a fixed-width column's values (`None` if the query does not use it)."""
        if not self.wants(col):
            return None
        return self._tree.data(self._position[col]).unsafe_bitcast[T]()

    def reference(mut self, col: Int, source: duckdb_v2_vector_handle) raises DuckDBError:
        """Make declared column `col` show `source`'s data without copying.
        `source` must have the column's type and stay alive until the next
        scan of this thread."""
        if not self.wants(col):
            return
        var err = null_error_info()
        check(lib().duckdb_v2_vector_reference(self._vectors[self._vector_of[col]], source, Pointer(to=err)), err)

    def write_row[T: AnyType](mut self, row: Int, value: T) raises DuckDBError:
        """Write struct `value` to the columns named like its fields (needs columns
        declared with names matching the fields, e.g. by `add_row[T]`)."""
        var w = _Writer(Pointer(to=self._tree).unsafe_origin_cast[MutUntrackedOrigin](), 0, row)
        try:
            _write(w, value)
        except e:
            raise _from_ser(e, String("row ", row))

    def _finish(mut self, rows: Int) raises DuckDBError:
        var err = null_error_info()
        for i in range(len(self._vectors)):
            check(lib().duckdb_v2_vector_set_size(self._vectors[i], idx_t(rows), Pointer(to=err)), err)
            self._tree.finish(self._nodes[i])


struct TableFunction(Movable):
    """A table function: name, parameters and callbacks."""

    var name: String
    var signature: Signature
    var _bind: duckdb_v2_table_function_bind_callback_fn
    var _init_global: duckdb_v2_table_function_init_global_callback_fn
    var _init_local: duckdb_v2_table_function_init_local_callback_fn
    var _exec: duckdb_v2_table_function_exec_callback_fn
    var _pushdown: Optional[duckdb_v2_table_function_filter_pushdown_callback_fn]
    var _progress: Optional[duckdb_v2_table_function_progress_callback_fn]
    var _partition: Optional[duckdb_v2_table_function_partition_data_callback_fn]

    @staticmethod
    def from_impl[T: TableFunctionImpl](var name: String, var params: List[LogicalType]) -> TableFunction:
        """A table function from its implementation struct and parameter types (`param_types[...]()`)."""
        var f = TableFunction(name^, params^, _bind_cb[T], _init_global_cb[T], _init_local_cb[T], _exec_cb[T])
        comptime if conforms_to(T, FilterPushdown):
            var callback: duckdb_v2_table_function_filter_pushdown_callback_fn = _pushdown_cb[T]
            f._pushdown = callback
        comptime if conforms_to(T, ScanProgress):
            var callback: duckdb_v2_table_function_progress_callback_fn = _progress_cb[T]
            f._progress = callback
        comptime if conforms_to(T, OrderedBatches):
            var callback: duckdb_v2_table_function_partition_data_callback_fn = _partition_cb[T]
            f._partition = callback
        return f^

    def __init__(
        out self,
        var name: String,
        var params: List[LogicalType],
        bind: duckdb_v2_table_function_bind_callback_fn,
        init_global: duckdb_v2_table_function_init_global_callback_fn,
        init_local: duckdb_v2_table_function_init_local_callback_fn,
        exec: duckdb_v2_table_function_exec_callback_fn,
    ):
        self.name = name^
        self.signature = Signature(params, fixed=False)
        self._bind = bind
        self._init_global = init_global
        self._init_local = init_local
        self._exec = exec
        self._pushdown = None
        self._progress = None
        self._partition = None


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

    def add_named[T: Movable & Deinitable](var self, name: String, default: T) raises DuckDBError -> Self:
        """A named-only parameter with a default value (`repeat('a', times := 5)`)."""
        self.signature.add(name, logical_type_of[T](), ParamKind.NAMED_ONLY)
        self.signature.set_default(name, Value(default))
        return self^

    def varargs(var self, name: String, type: LogicalType) raises DuckDBError -> Self:
        """`*args`: any number of further positional arguments of `type`
        (`TypeId.ANY` leaves their types as they are)."""
        self.signature.add(name, type.copy(), ParamKind.VARARGS)
        return self^

    def kwargs(var self, name: String, type: LogicalType) raises DuckDBError -> Self:
        """`**kwargs`: named arguments that match no parameter, of `type`."""
        self.signature.add(name, type.copy(), ParamKind.KWARGS)
        return self^

    def _register[R: Registrar](self, registrar: R) raises DuckDBError:
        var err = null_error_info()
        var function = null[duckdb_v2_table_function_handle]()
        check(lib().duckdb_v2_table_function_create(registrar.factory(), Pointer(to=function), Pointer(to=err)), err)
        try:
            var name = StrArg(self.name)
            check(lib().duckdb_v2_table_function_set_name(function, name.ptr(), Pointer(to=err)), err)
            var signature = null[duckdb_v2_function_signature_handle]()
            check(lib().duckdb_v2_table_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
            self.signature._add_to(signature)
            var identity = _identity_user_data(self.name)
            check(lib().duckdb_v2_table_function_set_user_data(function, Pointer(to=identity), Pointer(to=err)), err)
            check(lib().duckdb_v2_table_function_set_bind_callback(function, self._bind, Pointer(to=err)), err)
            check(lib().duckdb_v2_table_function_set_init_global_callback(function, self._init_global, Pointer(to=err)), err)
            check(lib().duckdb_v2_table_function_set_init_local_callback(function, self._init_local, Pointer(to=err)), err)
            check(lib().duckdb_v2_table_function_set_exec_callback(function, self._exec, Pointer(to=err)), err)
            check(lib().duckdb_v2_table_function_set_projection_pushdown(function, True, Pointer(to=err)), err)
            if self._progress:
                check(lib().duckdb_v2_table_function_set_progress_callback(function, self._progress.value(), Pointer(to=err)), err)
            if self._partition:
                check(
                    lib().duckdb_v2_table_function_set_partition_data_callback(function, self._partition.value(), Pointer(to=err)), err
                )
            if self._pushdown:
                check(
                    lib().duckdb_v2_table_function_set_filter_pushdown_callback(function, self._pushdown.value(), Pointer(to=err)),
                    err,
                )
            registrar._register_table_handle(function)
        finally:
            _ = lib().duckdb_v2_table_function_destroy(Pointer(to=function))


# ===--------------------------------------------------------------------===#
# Callbacks
# ===--------------------------------------------------------------------===#


def _opaque[T: Movable & Deinitable](var value: T) -> duckdb_v2_opaque:
    var ptr = unsafe_alloc[T](1)
    ptr.unsafe_write(value^)
    return duckdb_v2_opaque(ptr.unsafe_bitcast[NoneType](), _destroy_bind[T], _same)


def _same(a: Pointer[NoneType, MutUntrackedOrigin], b: Pointer[NoneType, MutUntrackedOrigin]) abi("C") -> Bool:
    return Int(a) == Int(b)


def _bind_cb[T: TableFunctionImpl](
    info: duckdb_v2_function_bind_info_handle,
    result: duckdb_v2_table_function_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        var columns = TableColumns(result)
        var impl = T.bind(BindArgs(info, context), columns)
        var data = _opaque(_Bound[T](impl^, columns._names.copy()))
        var e = null_error_info()
        var code = lib().duckdb_v2_function_bind_set_bind_data(info, Pointer(to=data), Pointer(to=e))
        if code != DUCKDB_V2_ERROR_NONE:
            data.destroy(data.ptr)
            check(code, e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _init_global_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_init_global_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        var e = null_error_info()
        var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
        check(lib().duckdb_v2_table_function_init_global_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
        var threads = bind.unsafe_bitcast[_Bound[T]]()[].impl.max_threads()
        var shared = _opaque(_Shared())
        var code = lib().duckdb_v2_table_function_init_global_set_global_state(info, Pointer(to=shared), Pointer(to=e))
        if code != DUCKDB_V2_ERROR_NONE:
            shared.destroy(shared.ptr)
            check(code, e)
        check(lib().duckdb_v2_table_function_init_global_set_max_threads(info, idx_t(max(threads, 1)), Pointer(to=e)), e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _init_local_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_init_local_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_init_local[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_init_local[T: TableFunctionImpl](info: duckdb_v2_table_function_init_local_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_table_function_init_local_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var shared = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_table_function_init_local_get_global_state(info, Pointer(to=shared), Pointer(to=e)), e)
    var state = _opaque(_Local[T.State](bind.unsafe_bitcast[_Bound[T]]()[].impl.init(ScanShared(shared.unsafe_bitcast[_Shared]()))))
    var code = lib().duckdb_v2_table_function_init_local_set_local_state(info, Pointer(to=state), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        state.destroy(state.ptr)
        check(code, e)


def _progress_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_progress_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        comptime P = downcast[T, ScanProgress]
        var e = null_error_info()
        var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
        check(lib().duckdb_v2_table_function_progress_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
        var shared = null[Pointer[NoneType, MutUntrackedOrigin]]()
        check(lib().duckdb_v2_table_function_progress_get_global_state(info, Pointer(to=shared), Pointer(to=e)), e)
        if Int(shared) == 0:
            return  # no scan started yet
        ref impl = bind.unsafe_bitcast[_Bound[T]]()[].impl
        var p = Pointer(to=impl).unsafe_bitcast[P]()[].progress(ScanShared(shared.unsafe_bitcast[_Shared]()))
        check(lib().duckdb_v2_table_function_progress_set_progress(info, p, Pointer(to=e)), e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _partition_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_partition_data_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        var e = null_error_info()
        var local = null[Pointer[NoneType, MutUntrackedOrigin]]()
        check(lib().duckdb_v2_table_function_partition_data_get_local_state(info, Pointer(to=local), Pointer(to=e)), e)
        var batch = local.unsafe_bitcast[_Local[T.State]]()[].batch
        check(lib().duckdb_v2_table_function_partition_data_set_batch_index(info, idx_t(batch), Pointer(to=e)), e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _resolve_filter_column(info: Pointer[NoneType, MutUntrackedOrigin], index: Int) raises -> Int:
    var declared: idx_t = 0
    var err = null_error_info()
    check(
        lib().duckdb_v2_table_function_filter_pushdown_get_column_index(
            info.unsafe_bitcast[_duckdb_v2_table_function_filter_pushdown_info](), idx_t(index), Pointer(to=declared), Pointer(to=err)
        ),
        err,
    )
    return Int(declared)


def _pushdown_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_filter_pushdown_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_pushdown[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_OPTIMIZER)


def _run_pushdown[T: TableFunctionImpl](info: duckdb_v2_table_function_filter_pushdown_info_handle) raises:
    comptime P = downcast[T, FilterPushdown]
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_table_function_filter_pushdown_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    ref impl = bind.unsafe_bitcast[_Bound[T]]()[].impl
    var count: idx_t = 0
    check(lib().duckdb_v2_table_function_filter_pushdown_get_filter_count(info, Pointer(to=count), Pointer(to=e)), e)
    for i in range(Int(count)):
        var handle = null[duckdb_v2_expression_handle]()
        check(lib().duckdb_v2_table_function_filter_pushdown_get_filter(info, idx_t(i), Pointer(to=handle), Pointer(to=e)), e)
        var filter = Expression(handle, info.unsafe_bitcast[NoneType](), _resolve_filter_column)
        # Bind data is only shared once scans start; planning runs on one thread.
        if Pointer(to=impl).unsafe_bitcast[P]()[].pushdown(filter):
            check(lib().duckdb_v2_table_function_filter_pushdown_accept(info, idx_t(i), Pointer(to=e)), e)


def _exec_cb[T: TableFunctionImpl](
    info: duckdb_v2_table_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_exec[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_exec[T: TableFunctionImpl](info: duckdb_v2_table_function_exec_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_table_function_exec_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var local = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_table_function_exec_get_local_state(info, Pointer(to=local), Pointer(to=e)), e)
    ref bound = bind.unsafe_bitcast[_Bound[T]]()[]
    ref l = local.unsafe_bitcast[_Local[T.State]]()[]
    var out = TableOutput(info, bound.names)
    out._batch = Pointer(to=l.batch).unsafe_origin_cast[MutUntrackedOrigin]()
    var rows = bound.impl.scan(l.state, out)
    if rows < 0 or rows > out.capacity():
        raise DuckDBError(ErrorCode.API, String("scan returned ", rows, " rows; it must be between 0 and ", out.capacity()))
    out._finish(rows)
