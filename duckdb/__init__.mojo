"""Mojo bindings for DuckDB, built on DuckDB's V2 C API.

```mojo
import duckdb

duckdb.sql("SELECT 42 AS answer").show()
var con = duckdb.connect()
var rows = con.execute("SELECT range AS i FROM range(3)").get[Int64]()
```

See docs/design/v2-high-level-api.md for the design.
"""

from duckdb.error import DuckDBError, ErrorCode
from duckdb.types import LogicalType, TypeId
from duckdb.mapping import Blob, DuckDBMapped
from duckdb.value import Value
from duckdb.database import Connection, Database
from duckdb.chunk import Chunk, ColumnReader
from duckdb.result import ChunkResult, Step, StepStatus, Column, MaterializedResult, Result, ResultType, Row, StatementType
from duckdb.statement import Description, Params, PreparedStatement
from duckdb.relation import Relation
from duckdb.serde import ChunkWriter, logical_type_of
from duckdb.appender import Appender, OnConflict
from duckdb.functions import BindArgs, BindData, LocalState, Registrar, ScalarArgs, ScalarFunction, bind_callback, init_callback
from duckdb.serde import VectorWriter
from duckdb.simd_math import register_simd_math
from duckdb.extension import Extension
from duckdb.aggregate import AggregateBind, AggregateFunction, AggregateState, BatchUpdate
from duckdb.table import FilterPushdown, OrderedBatches, ScanProgress, ScanShared, TableColumns, TableFunction, TableFunctionImpl, TableOutput, param_types
from duckdb.expr import col, fn_, lit, sql_null, star
from duckdb.signature import ParamKind
from duckdb.expression import Between, Comparison, ExprKind, Expression
from duckdb.extend import CastFunction, CustomOption, CustomType, MacroFunction, PragmaFunction, Replacement, ReplacementScan
from duckdb.settings import SettingScope, Settings
from duckdb.files import File, FileSystem
from duckdb.observe import LogEntry, LogLevel, LogSink, MetricsNode, QueryMetrics
from duckdb.arrow import ArrowResult, OwnedArrowArray, OwnedArrowSchema, OwnedArrowStream
from duckdb.copy import CopyFromFormat, CopyFunction, CopyInfo, CopyToFormat
from duckdb.scalar_types import (
    Bit,
    Date,
    Decimal,
    Interval,
    Time,
    TimeNS,
    TimeTZ,
    Timestamp,
    TimestampMS,
    TimestampNS,
    TimestampS,
    TimestampTZ,
    TimestampTZNS,
    UUID,
)
from duckdb.default import (
    connect,
    default_connection,
    execute,
    read_csv,
    read_json,
    read_parquet,
    sql,
    table,
)
from duckdb.kernels.simd import ksqrt, ksin, kcos, kln, kexp, klog10
