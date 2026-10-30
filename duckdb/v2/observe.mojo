"""Profiling and logging.

```mojo
con.enable_profiling()
_ = con.execute("SELECT sum(i) FROM range(1000000) t(i)").fetchall()
var m = con.query_metrics()
print(m.root().metric("query.total_time"))

con.log(LogLevel.INFO, "my_app", "loaded the data")

def print_entry(entry: LogEntry):
    print(entry.level, entry.type, entry.message)

con.register(LogSink.from_function[print_entry]("printer"))
_ = con.execute("SET enable_logging = true; SET logging_storage = 'printer'")
```

Which metrics exist and how the operator tree looks may change between
DuckDB versions: look metrics up by name and expect any to be missing.
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.duckdb_type import TimestampTZ
from duckdb.v2._runtime import check, lib
from duckdb.v2.error import DuckDBError
from duckdb.v2.value import Value


# ===--------------------------------------------------------------------===#
# Profiling
# ===--------------------------------------------------------------------===#


struct QueryMetrics(Movable):
    """A snapshot of the metrics of the last query a connection finished (owned)."""

    var _handle: duckdb_v2_query_metrics_handle

    def __init__(out self, *, var _owned: duckdb_v2_query_metrics_handle):
        self._handle = _owned

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_query_metrics_destroy(Pointer(to=self._handle))

    def root(ref self) raises DuckDBError -> MetricsNode[ImmOrigin(origin_of(self))]:
        """The node of the query as a whole; its children are the operators."""
        var n = null[duckdb_v2_query_metrics_node_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_get_root(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return MetricsNode[ImmOrigin(origin_of(self))](n, Pointer(to=self).unsafe_bitcast[NoneType]())


struct MetricsNode[origin: ImmOrigin](ImplicitlyCopyable):
    """One node of a metrics tree (borrowed from its `QueryMetrics`)."""

    var _handle: duckdb_v2_query_metrics_node_handle
    var _owner: Pointer[NoneType, Self.origin]

    def __init__(out self, handle: duckdb_v2_query_metrics_node_handle, owner: Pointer[NoneType, Self.origin]):
        self._handle = handle
        self._owner = owner

    def name(self) raises DuckDBError -> String:
        """`QUERY` for the root, else the operator (`TABLE_SCAN`, ...)."""
        var s = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_node_get_name(self._handle, Pointer(to=s), Pointer(to=err)), err)
        return str_to_string(s)

    def child_count(self) raises DuckDBError -> Int:
        var n: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_node_get_child_count(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return Int(n)

    def child(self, i: Int) raises DuckDBError -> Self:
        var c = null[duckdb_v2_query_metrics_node_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_node_get_child(self._handle, idx_t(i), Pointer(to=c), Pointer(to=err)), err)
        return Self(c, self._owner)

    def metric(self, name: StringSlice) raises DuckDBError -> Optional[Value]:
        """Metric `name` (`query.total_time`, `operator.timing`, ...), or `None`."""
        var n = StrArg(name)
        var v = null[duckdb_v2_value_handle]()
        var present = False
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_node_get_metric(self._handle, n.ptr(), Pointer(to=v), Pointer(to=present), Pointer(to=err)), err)
        if not present:
            return None
        return Value(_owned=v)

    def metrics(self) raises DuckDBError -> List[Tuple[String, Value]]:
        """Every metric of this node, by name."""
        var count: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_query_metrics_node_get_metric_count(self._handle, Pointer(to=count), Pointer(to=err)), err)
        var out = List[Tuple[String, Value]]()
        for i in range(Int(count)):
            var s = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
            check(lib().duckdb_v2_query_metrics_node_get_metric_name(self._handle, idx_t(i), Pointer(to=s), Pointer(to=err)), err)
            var v = null[duckdb_v2_value_handle]()
            check(lib().duckdb_v2_query_metrics_node_get_metric_value(self._handle, idx_t(i), Pointer(to=v), Pointer(to=err)), err)
            out.append((str_to_string(s), Value(_owned=v)))
        return out^


# ===--------------------------------------------------------------------===#
# Logging
# ===--------------------------------------------------------------------===#


@fieldwise_init
struct LogLevel(Comparable, ImplicitlyCopyable, Writable):
    var value: DUCKDB_V2_LOG_LEVEL

    comptime TRACE = Self(DUCKDB_V2_LOG_LEVEL_TRACE)
    comptime DEBUG = Self(DUCKDB_V2_LOG_LEVEL_DEBUG)
    comptime INFO = Self(DUCKDB_V2_LOG_LEVEL_INFO)
    comptime WARNING = Self(DUCKDB_V2_LOG_LEVEL_WARNING)
    comptime ERROR = Self(DUCKDB_V2_LOG_LEVEL_ERROR)
    comptime FATAL = Self(DUCKDB_V2_LOG_LEVEL_FATAL)

    def __lt__(self, other: Self) -> Bool:
        return self.value < other.value

    def __eq__(self, other: Self) -> Bool:
        return self.value == other.value

    def write_to(self, mut writer: Some[Writer]):
        if self == Self.TRACE:
            writer.write("TRACE")
        elif self == Self.DEBUG:
            writer.write("DEBUG")
        elif self == Self.INFO:
            writer.write("INFO")
        elif self == Self.WARNING:
            writer.write("WARNING")
        elif self == Self.ERROR:
            writer.write("ERROR")
        else:
            writer.write("FATAL")


@fieldwise_init
struct LogEntry(Copyable, Movable):
    """One entry DuckDB hands to a `LogSink`."""

    var timestamp: TimestampTZ
    var level: LogLevel
    var type: String
    """The log type, e.g. `QueryLog` or what `Connection.log` was given."""
    var message: String
    var connection_id: Optional[Int]
    var query_id: Optional[Int]


struct LogSink(Movable):
    """Receives DuckDB's log entries in Mojo.

    Registered under a name; DuckDB sends entries to it after
    `SET logging_storage = '<name>'` (and `SET enable_logging = true`). The
    function may run on any DuckDB thread, also concurrently, and must not
    log itself. A raised error is dropped (logging never fails the operation
    that logged). The sink lives as long as the database.
    """

    var name: String
    var _write: duckdb_v2_log_sink_write_callback_fn

    @staticmethod
    def from_function[f: def(entry: LogEntry) raises thin -> None](name: String) -> LogSink:
        return LogSink(name, _log_write_cb[f])

    def __init__(out self, name: String, write: duckdb_v2_log_sink_write_callback_fn):
        self.name = name
        self._write = write

    def _create(self, factory: duckdb_v2_factory_handle) raises DuckDBError -> duckdb_v2_log_sink_handle:
        var err = null_error_info()
        var sink = null[duckdb_v2_log_sink_handle]()
        check(lib().duckdb_v2_log_sink_create(factory, Pointer(to=sink), Pointer(to=err)), err)
        try:
            var n = StrArg(self.name)
            check(lib().duckdb_v2_log_sink_set_name(sink, n.ptr(), Pointer(to=err)), err)
            check(lib().duckdb_v2_log_sink_set_write_callback(sink, self._write, Pointer(to=err)), err)
        except e:
            _ = lib().duckdb_v2_log_sink_destroy(Pointer(to=sink))
            raise e^
        return sink


def _optional_id(found: Bool, id: idx_t) -> Optional[Int]:
    if not found:
        return None
    return Int(id)


def _log_write_cb[f: def(entry: LogEntry) raises thin -> None](
    info: duckdb_v2_log_sink_write_info_handle,
    user_data: Pointer[NoneType, MutUntrackedOrigin],
    timestamp: Int64,
    level: DUCKDB_V2_LOG_LEVEL,
    scope: DUCKDB_V2_LOG_SCOPE,
    type: Pointer[duckdb_v2_str, ImmUntrackedOrigin],
    message: Pointer[duckdb_v2_str, ImmUntrackedOrigin],
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    var e = null_error_info()
    var conn: idx_t = 0
    var query: idx_t = 0
    var has_conn = False
    var has_query = False
    if lib().duckdb_v2_log_sink_write_info_get_connection_id(info, Pointer(to=conn), Pointer(to=has_conn), Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))
    if lib().duckdb_v2_log_sink_write_info_get_query_id(info, Pointer(to=query), Pointer(to=has_query), Pointer(to=e)) != DUCKDB_V2_ERROR_NONE:
        _ = lib().duckdb_v2_error_info_destroy(Pointer(to=e))
    var entry = LogEntry(
        TimestampTZ(timestamp),
        LogLevel(level),
        str_to_string(type[]),
        str_to_string(message[]),
        _optional_id(has_conn, conn),
        _optional_id(has_query, query),
    )
    try:
        f(entry)
    except:
        pass


def _log(target: duckdb_v2_connection_handle, level: LogLevel, type: StringSlice, message: StringSlice) raises DuckDBError:
    var t = StrArg(type)
    var m = StrArg(message)
    var err = null_error_info()
    check(lib().duckdb_v2_connection_log(target, level.value, t.ptr(), m.ptr(), Pointer(to=err)), err)
