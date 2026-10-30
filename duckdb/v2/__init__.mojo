"""DuckDB's high-level Mojo API on the V2 C API (work in progress).

This package is built next to the existing (V1-based) API and replaces it once
it is complete; see docs/design/v2-high-level-api.md.
"""

from duckdb.v2.error import DuckDBError, ErrorCode
from duckdb.v2.types import LogicalType, TypeId
from duckdb.v2.mapping import Blob, DuckDBMapped
from duckdb.v2.value import Value
from duckdb.v2.database import Connection, Database
from duckdb.v2.chunk import Chunk, ColumnReader
from duckdb.v2.result import ChunkResult, Column, MaterializedResult, Result, ResultType, Row, StatementType
from duckdb.v2.statement import Description, Params, PreparedStatement
from duckdb.v2.relation import Relation
from duckdb.v2.serde import ChunkWriter, logical_type_of
from duckdb.v2.appender import Appender, OnConflict
from duckdb.v2.functions import BindArgs, BindData, LocalState, Registrar, ScalarArgs, ScalarFunction, bind_callback, init_callback
from duckdb.v2.serde import VectorWriter
from duckdb.v2.simd_math import register_simd_math
from duckdb.v2.extension import Extension
from duckdb.v2.aggregate import AggregateBind, AggregateFunction, AggregateState, BatchUpdate
from duckdb.v2.table import FilterPushdown, ScanShared, TableColumns, TableFunction, TableFunctionImpl, TableOutput, param_types
from duckdb.expr import col, fn_, lit, sql_null, star
from duckdb.v2.signature import ParamKind
from duckdb.v2.expression import Between, Comparison, ExprKind, Expression
from duckdb.v2.extend import CastFunction, CustomOption, MacroFunction, PragmaFunction, Replacement, ReplacementScan, SettingScope
