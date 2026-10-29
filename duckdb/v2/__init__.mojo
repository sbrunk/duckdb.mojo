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
