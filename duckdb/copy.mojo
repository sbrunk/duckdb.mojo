"""Custom file formats for `COPY`.

```mojo
con.register(CopyFunction("lines", extension="txt").writes[LinesWriter]().reads[LinesReader]())
# COPY t TO 'out.txt' (FORMAT lines, SEPARATOR '|')
# COPY t FROM 'out.txt' (FORMAT lines)
```

A writer (`CopyToFormat`) is set up once per statement (`bind`), opens each
output file (`open`), gets the rows one chunk at a time (`write`; DuckDB
never runs two writes of one file at once, and keeps their order) and
finishes each file (`close`). A statement can write several files
(`PARTITION_BY`, `PER_THREAD_OUTPUT`), each with its own `File` state.

A reader (`CopyFromFormat`) works like a table function: `bind` reads the
path, the options and the columns the target table expects, `init` creates
the scan state, and `scan` fills one batch of those columns at a time.
`CopyInfo.file_system()` gives DuckDB's file system, so paths work as in SQL.
"""

from std.memory import ArcPointer
from std.memory.alloc import unsafe_alloc
from duckdb._libduckdb_v2 import *
from duckdb._libduckdb_v2 import _duckdb_v2_column_data_collection
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb._runtime import check, lib
from duckdb.chunk import Chunk, _Schema
from duckdb.error import DuckDBError, ErrorCode
from duckdb.files import FileSystem, context_file_system
from duckdb.functions import _destroy_bind, _identity_user_data, _report
from duckdb.table import TableOutput
from duckdb.types import LogicalType
from duckdb.value import Value


struct CopyInfo(Movable):
    """What a `COPY` statement asks for: the path, the columns and the options."""

    var path: String
    var names: List[String]
    var types: List[LogicalType]
    var _option_names: List[String]
    var _option_values: List[Value]
    var _context: duckdb_v2_context_handle

    def __init__(out self, var path: String, context: duckdb_v2_context_handle):
        self.path = path^
        self.names = List[String]()
        self.types = List[LogicalType]()
        self._option_names = List[String]()
        self._option_values = List[Value]()
        self._context = context

    def column_count(self) -> Int:
        return len(self.names)

    def has_option(self, name: StringSlice) -> Bool:
        return self._find(name) >= 0

    def option[T: Movable & Deinitable](self, name: StringSlice) raises DuckDBError -> Optional[T]:
        """Option `name` of the statement (`(SEPARATOR '|')`, case-insensitive) as
        `T`, or `None` when it is not set. An option without a value (`HEADER`) is TRUE."""
        var i = self._find(name)
        if i < 0:
            return None
        return self._option_values[i].get_optional[T]()

    def option_names(self) -> List[String]:
        return self._option_names.copy()

    def file_system(self) raises DuckDBError -> FileSystem[ImmUntrackedOrigin]:
        """DuckDB's file system (valid during `bind`)."""
        return context_file_system(self._context)

    def _find(self, name: StringSlice) -> Int:
        var lower = String(name).lower()
        for i in range(len(self._option_names)):
            if self._option_names[i].lower() == lower:
                return i
        return -1


trait CopyToFormat(Deinitable, Movable):
    """Writes the rows of `COPY ... TO` (see the module docs)."""

    comptime File: Movable & Deinitable
    """The state of one output file."""

    @staticmethod
    def bind(info: CopyInfo) raises -> Self:
        ...

    def open(ref self, path: String) raises -> Self.File:
        ...

    def write(ref self, mut file: Self.File, rows: Chunk) raises:
        ...

    def close(ref self, mut file: Self.File) raises:
        ...


trait CopyFromFormat(Deinitable, Movable):
    """Reads the rows of `COPY ... FROM` (see the module docs)."""

    comptime State: Movable & Deinitable

    @staticmethod
    def bind(info: CopyInfo) raises -> Self:
        ...

    def init(ref self) raises -> Self.State:
        ...

    def scan(ref self, mut state: Self.State, mut out: TableOutput) raises -> Int:
        """Write up to `out.capacity()` rows of the expected columns. 0 ends the read."""
        ...


struct CopyFunction(Movable):
    """A `COPY` format: a writer, a reader, or both, under one `FORMAT` name."""

    var name: String
    var extension: String
    var _to_bind: Optional[duckdb_v2_copy_to_bind_callback_fn]
    var _to_init: Optional[duckdb_v2_copy_to_init_callback_fn]
    var _to_batch: Optional[duckdb_v2_copy_to_batch_callback_fn]
    var _to_flush: Optional[duckdb_v2_copy_to_flush_callback_fn]
    var _to_finalize: Optional[duckdb_v2_copy_to_finalize_callback_fn]
    var _from_bind: Optional[duckdb_v2_copy_from_bind_callback_fn]
    var _from_init_global: Optional[duckdb_v2_copy_from_init_global_callback_fn]
    var _from_init_local: Optional[duckdb_v2_copy_from_init_local_callback_fn]
    var _from_exec: Optional[duckdb_v2_copy_from_exec_callback_fn]

    def __init__(out self, name: String, *, extension: String = ""):
        """A format called `name` (`FORMAT name`); `extension` (without the dot)
        names the files DuckDB creates itself (`PARTITION_BY`, ...)."""
        self.name = name
        self.extension = extension
        self._to_bind = None
        self._to_init = None
        self._to_batch = None
        self._to_flush = None
        self._to_finalize = None
        self._from_bind = None
        self._from_init_global = None
        self._from_init_local = None
        self._from_exec = None

    def writes[T: CopyToFormat](var self) -> Self:
        """Implement `COPY ... TO` with `T`."""
        var bind: duckdb_v2_copy_to_bind_callback_fn = _to_bind_cb[T]
        var init: duckdb_v2_copy_to_init_callback_fn = _to_init_cb[T]
        var batch: duckdb_v2_copy_to_batch_callback_fn = _to_batch_cb
        var flush: duckdb_v2_copy_to_flush_callback_fn = _to_flush_cb[T]
        var finalize: duckdb_v2_copy_to_finalize_callback_fn = _to_finalize_cb[T]
        self._to_bind = bind
        self._to_init = init
        self._to_batch = batch
        self._to_flush = flush
        self._to_finalize = finalize
        return self^

    def reads[T: CopyFromFormat](var self) -> Self:
        """Implement `COPY ... FROM` with `T`."""
        var bind: duckdb_v2_copy_from_bind_callback_fn = _from_bind_cb[T]
        var init_global: duckdb_v2_copy_from_init_global_callback_fn = _from_init_global_cb
        var init_local: duckdb_v2_copy_from_init_local_callback_fn = _from_init_local_cb[T]
        var exec: duckdb_v2_copy_from_exec_callback_fn = _from_exec_cb[T]
        self._from_bind = bind
        self._from_init_global = init_global
        self._from_init_local = init_local
        self._from_exec = exec
        return self^

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_copy_function_handle:
        var err = null_error_info()
        var f = null[duckdb_v2_copy_function_handle]()
        check(lib().duckdb_v2_copy_function_create(factory, Pointer(to=f), Pointer(to=err)), err)
        try:
            var n = StrArg(self.name)
            check(lib().duckdb_v2_copy_function_set_name(f, n.ptr(), Pointer(to=err)), err)
            if self.extension:
                var e = StrArg(self.extension)
                check(lib().duckdb_v2_copy_function_set_file_extension(f, e.ptr(), Pointer(to=err)), err)
            if self._to_bind:
                check(lib().duckdb_v2_copy_to_set_bind_callback(f, self._to_bind.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_to_set_init_callback(f, self._to_init.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_to_set_batch_callback(f, self._to_batch.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_to_set_flush_callback(f, self._to_flush.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_to_set_finalize_callback(f, self._to_finalize.value(), Pointer(to=err)), err)
            if self._from_bind:
                check(lib().duckdb_v2_copy_from_set_bind_callback(f, self._from_bind.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_from_set_init_global_callback(f, self._from_init_global.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_from_set_init_local_callback(f, self._from_init_local.value(), Pointer(to=err)), err)
                check(lib().duckdb_v2_copy_from_set_exec_callback(f, self._from_exec.value(), Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_copy_function_destroy(Pointer(to=f))
            raise e^
        return f


# ===--------------------------------------------------------------------===#
# Helpers
# ===--------------------------------------------------------------------===#


def _opaque[T: Movable & Deinitable](var value: T) -> duckdb_v2_opaque:
    var ptr = unsafe_alloc[T](1)
    ptr.unsafe_write(value^)
    return duckdb_v2_opaque(ptr.unsafe_bitcast[NoneType](), _destroy_bind[T], _same)


def _same(a: Pointer[NoneType, MutUntrackedOrigin], b: Pointer[NoneType, MutUntrackedOrigin]) abi("C") -> Bool:
    return Int(a) == Int(b)


def _path(s: duckdb_v2_str) -> String:
    return str_to_string(s)


struct _Bound[T: Movable & Deinitable](Movable):
    """Bind data of a copy function: the user's format and the column schema."""

    var impl: Self.T
    var schema: ArcPointer[_Schema]

    def __init__(out self, var impl: Self.T, var schema: ArcPointer[_Schema]):
        self.impl = impl^
        self.schema = schema^


def _destroy_collection(ptr: Pointer[NoneType, MutUntrackedOrigin]) abi("C"):
    var c = ptr.unsafe_bitcast[_duckdb_v2_column_data_collection]()
    _ = lib().duckdb_v2_column_data_collection_destroy(Pointer(to=c))


# ===--------------------------------------------------------------------===#
# COPY TO callbacks
# ===--------------------------------------------------------------------===#


def _to_bind_cb[T: CopyToFormat](
    info: duckdb_v2_copy_to_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_to_bind[T](info, context)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _run_to_bind[T: CopyToFormat](info: duckdb_v2_copy_to_bind_info_handle, context: duckdb_v2_context_handle) raises:
    var e = null_error_info()
    var p = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
    check(lib().duckdb_v2_copy_to_bind_get_file_path(info, Pointer(to=p), Pointer(to=e)), e)
    var ci = CopyInfo(_path(p), context)
    var count: idx_t = 0
    check(lib().duckdb_v2_copy_to_bind_get_column_count(info, Pointer(to=count), Pointer(to=e)), e)
    for i in range(Int(count)):
        var n = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        check(lib().duckdb_v2_copy_to_bind_get_column_name(info, idx_t(i), Pointer(to=n), Pointer(to=e)), e)
        var t = null[duckdb_v2_logical_type_handle]()
        check(lib().duckdb_v2_copy_to_bind_get_column_type(info, idx_t(i), Pointer(to=t), Pointer(to=e)), e)
        ci.names.append(str_to_string(n))
        ci.types.append(LogicalType(_owned=t))
    var options: idx_t = 0
    check(lib().duckdb_v2_copy_to_bind_get_option_count(info, Pointer(to=options), Pointer(to=e)), e)
    for i in range(Int(options)):
        var n = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        check(lib().duckdb_v2_copy_to_bind_get_option_name(info, idx_t(i), Pointer(to=n), Pointer(to=e)), e)
        var v = null[duckdb_v2_value_handle]()
        check(lib().duckdb_v2_copy_to_bind_get_option_value(info, idx_t(i), Pointer(to=v), Pointer(to=e)), e)
        ci._option_names.append(str_to_string(n))
        ci._option_values.append(Value(_owned=v))
    var impl = T.bind(ci)
    var data = _opaque(_Bound[T](impl^, ArcPointer(_Schema(ci.names.copy(), ci.types.copy()))))
    var code = lib().duckdb_v2_copy_to_bind_set_bind_data(info, Pointer(to=data), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        data.destroy(data.ptr)
        check(code, e)


def _to_init_cb[T: CopyToFormat](
    info: duckdb_v2_copy_to_init_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_to_init[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_to_init[T: CopyToFormat](info: duckdb_v2_copy_to_init_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_init_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var p = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
    check(lib().duckdb_v2_copy_to_init_get_file_path(info, Pointer(to=p), Pointer(to=e)), e)
    var file = _opaque(bind.unsafe_bitcast[_Bound[T]]()[].impl.open(_path(p)))
    var code = lib().duckdb_v2_copy_to_init_set_init_data(info, Pointer(to=file), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        file.destroy(file.ptr)
        check(code, e)


def _to_batch_cb(
    info: duckdb_v2_copy_to_batch_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    """Keep the batch's rows for the flush, which hands them to `write` in order."""
    try:
        var e = null_error_info()
        var collection = null[duckdb_v2_column_data_collection_handle]()
        check(lib().duckdb_v2_copy_to_batch_take_input(info, Pointer(to=collection), Pointer(to=e)), e)
        var data = duckdb_v2_opaque(collection.unsafe_bitcast[NoneType](), _destroy_collection, _same)
        var code = lib().duckdb_v2_copy_to_batch_set_batch_data(info, Pointer(to=data), Pointer(to=e))
        if code != DUCKDB_V2_ERROR_NONE:
            _destroy_collection(data.ptr)
            check(code, e)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _to_flush_cb[T: CopyToFormat](
    info: duckdb_v2_copy_to_flush_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_to_flush[T](info, context)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_to_flush[T: CopyToFormat](info: duckdb_v2_copy_to_flush_info_handle, context: duckdb_v2_context_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_flush_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var file = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_flush_get_init_data(info, Pointer(to=file), Pointer(to=e)), e)
    var batch = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_flush_get_batch_data(info, Pointer(to=batch), Pointer(to=e)), e)
    ref bound = bind.unsafe_bitcast[_Bound[T]]()[]
    var collection = batch.unsafe_bitcast[_duckdb_v2_column_data_collection]()
    var factory = null[duckdb_v2_factory_handle]()
    check(lib().duckdb_v2_context_get_factory(context, Pointer(to=factory), Pointer(to=e)), e)
    var shared = null[duckdb_v2_column_data_collection_shared_scan_state_handle]()
    var worker = null[duckdb_v2_column_data_collection_worker_scan_state_handle]()
    check(lib().duckdb_v2_column_data_collection_shared_scan_state_create(collection, Pointer(to=shared), Pointer(to=e)), e)
    try:
        check(lib().duckdb_v2_column_data_collection_worker_scan_state_create(collection, Pointer(to=worker), Pointer(to=e)), e)
        var handles = List[duckdb_v2_logical_type_handle]()
        for i in range(len(bound.schema[].types)):
            handles.append(bound.schema[].types[i].handle())
        while True:
            var chunk = null[duckdb_v2_data_chunk_handle]()
            check(
                lib().duckdb_v2_data_chunk_create(factory, handles.unsafe_ptr().as_imm(), idx_t(len(handles)), Pointer(to=chunk), Pointer(to=e)),
                e,
            )
            var produced = False
            var code = lib().duckdb_v2_column_data_collection_scan(collection, shared, worker, chunk, Pointer(to=produced), Pointer(to=e))
            var rows = Chunk(_owned=chunk, schema=bound.schema)
            check(code, e)
            if not produced:
                break
            _write_rows[T](bound.impl, file.unsafe_bitcast[T.File]()[], rows)
        _ = handles^
    finally:
        _ = lib().duckdb_v2_column_data_collection_worker_scan_state_destroy(Pointer(to=worker))
        _ = lib().duckdb_v2_column_data_collection_shared_scan_state_destroy(Pointer(to=shared))


def _write_rows[T: CopyToFormat](ref impl: T, mut file: T.File, rows: Chunk) raises DuckDBError:
    # Its own function: one `try` cannot mix the user's untyped errors with DuckDBError.
    try:
        impl.write(file, rows)
    except e:
        raise DuckDBError(ErrorCode(DUCKDB_V2_ERROR_QUERY_EXECUTOR), String(e))


def _to_finalize_cb[T: CopyToFormat](
    info: duckdb_v2_copy_to_finalize_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_to_finalize[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_to_finalize[T: CopyToFormat](info: duckdb_v2_copy_to_finalize_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_finalize_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var file = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_to_finalize_get_init_data(info, Pointer(to=file), Pointer(to=e)), e)
    bind.unsafe_bitcast[_Bound[T]]()[].impl.close(file.unsafe_bitcast[T.File]()[])


# ===--------------------------------------------------------------------===#
# COPY FROM callbacks
# ===--------------------------------------------------------------------===#


def _from_bind_cb[T: CopyFromFormat](
    info: duckdb_v2_copy_from_bind_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_from_bind[T](info, context)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_BINDER)


def _run_from_bind[T: CopyFromFormat](info: duckdb_v2_copy_from_bind_info_handle, context: duckdb_v2_context_handle) raises:
    var e = null_error_info()
    var p = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
    check(lib().duckdb_v2_copy_from_bind_get_file_path(info, Pointer(to=p), Pointer(to=e)), e)
    var ci = CopyInfo(_path(p), context)
    var count: idx_t = 0
    check(lib().duckdb_v2_copy_from_bind_get_column_count(info, Pointer(to=count), Pointer(to=e)), e)
    for i in range(Int(count)):
        var n = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        check(lib().duckdb_v2_copy_from_bind_get_column_name(info, idx_t(i), Pointer(to=n), Pointer(to=e)), e)
        var t = null[duckdb_v2_logical_type_handle]()
        check(lib().duckdb_v2_copy_from_bind_get_column_type(info, idx_t(i), Pointer(to=t), Pointer(to=e)), e)
        ci.names.append(str_to_string(n))
        ci.types.append(LogicalType(_owned=t))
    var options: idx_t = 0
    check(lib().duckdb_v2_copy_from_bind_get_option_count(info, Pointer(to=options), Pointer(to=e)), e)
    for i in range(Int(options)):
        var n = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        check(lib().duckdb_v2_copy_from_bind_get_option_name(info, idx_t(i), Pointer(to=n), Pointer(to=e)), e)
        var v = null[duckdb_v2_value_handle]()
        check(lib().duckdb_v2_copy_from_bind_get_option_value(info, idx_t(i), Pointer(to=v), Pointer(to=e)), e)
        ci._option_names.append(str_to_string(n))
        ci._option_values.append(Value(_owned=v))
    var impl = T.bind(ci)
    var data = _opaque(_Bound[T](impl^, ArcPointer(_Schema(ci.names.copy(), ci.types.copy()))))
    var code = lib().duckdb_v2_copy_from_bind_set_bind_data(info, Pointer(to=data), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        data.destroy(data.ptr)
        check(code, e)


def _from_init_global_cb(
    info: duckdb_v2_copy_from_init_global_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    # One reading thread: reading one file in parallel needs the format's
    # cooperation, which this API does not ask for yet.
    var e = null_error_info()
    if lib().duckdb_v2_copy_from_init_global_set_max_threads(info, 1, Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))


def _from_init_local_cb[T: CopyFromFormat](
    info: duckdb_v2_copy_from_init_local_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_from_init_local[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_from_init_local[T: CopyFromFormat](info: duckdb_v2_copy_from_init_local_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_from_init_local_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var state = _opaque(bind.unsafe_bitcast[_Bound[T]]()[].impl.init())
    var code = lib().duckdb_v2_copy_from_init_local_set_local_state(info, Pointer(to=state), Pointer(to=e))
    if code != DUCKDB_V2_ERROR_NONE:
        state.destroy(state.ptr)
        check(code, e)


def _from_exec_cb[T: CopyFromFormat](
    info: duckdb_v2_copy_from_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _run_from_exec[T](info)
    except e:
        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)


def _run_from_exec[T: CopyFromFormat](info: duckdb_v2_copy_from_exec_info_handle) raises:
    var e = null_error_info()
    var bind = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_from_exec_get_bind_data(info, Pointer(to=bind), Pointer(to=e)), e)
    var local = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib().duckdb_v2_copy_from_exec_get_local_state(info, Pointer(to=local), Pointer(to=e)), e)
    var chunk = null[duckdb_v2_data_chunk_handle]()
    check(lib().duckdb_v2_copy_from_exec_get_output_chunk(info, Pointer(to=chunk), Pointer(to=e)), e)
    ref bound = bind.unsafe_bitcast[_Bound[T]]()[]
    var declared = List[Int]()
    for i in range(len(bound.schema[].names)):
        declared.append(i)
    var out = TableOutput(chunk, bound.schema[].names, declared)
    var rows = bound.impl.scan(local.unsafe_bitcast[T.State]()[], out)
    if rows < 0 or rows > out.capacity():
        raise Error(String("scan returned ", rows, " rows; it must be between 0 and ", out.capacity()))
    out._finish(rows)
