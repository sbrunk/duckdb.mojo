"""Read-only bound expressions, as DuckDB offers them to table functions
for filter pushdown (see `FilterPushdown` in `table.mojo`).

An expression is a tree of nodes: comparisons, `AND`/`OR`, column references,
constants, casts, function calls. Each node has a `kind()`, a `return_type()`
and children. `comparison()` recognizes the common shape `column <op>
constant` (either way round), `between()` a range.

DuckDB combines filters before offering them: `i >= 5 AND i < 10` arrives
as one `BETWEEN` node, whose bounds may be exclusive without the API saying
so (see `Between`).
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info, str_to_string
from duckdb.v2._runtime import check, lib
from duckdb.v2.error import DuckDBError
from duckdb.v2.serde import from_value
from duckdb.v2.types import LogicalType
from duckdb.v2.value import Value


@fieldwise_init
struct ExprKind(Equatable, ImplicitlyCopyable, Writable):
    """The kind of an expression node (V2 `EXPRESSION_TYPE`). Node types the
    API does not model are `OTHER`: leave such filters to DuckDB."""

    var value: DUCKDB_V2_EXPRESSION_TYPE

    comptime OTHER = Self(DUCKDB_V2_EXPRESSION_TYPE_INVALID)
    comptime CAST = Self(DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_CAST)
    comptime NOT = Self(DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_NOT)
    comptime IS_NULL = Self(DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NULL)
    comptime IS_NOT_NULL = Self(DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_IS_NOT_NULL)
    comptime EQUAL = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_EQUAL)
    comptime NOT_EQUAL = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOTEQUAL)
    comptime LESS = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHAN)
    comptime GREATER = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHAN)
    comptime LESS_EQUAL = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_LESSTHANOREQUALTO)
    comptime GREATER_EQUAL = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_GREATERTHANOREQUALTO)
    comptime IN = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_IN)
    comptime NOT_IN = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_IN)
    comptime DISTINCT_FROM = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_DISTINCT_FROM)
    comptime BETWEEN = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_BETWEEN)
    comptime NOT_DISTINCT_FROM = Self(DUCKDB_V2_EXPRESSION_TYPE_COMPARE_NOT_DISTINCT_FROM)
    comptime AND = Self(DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_AND)
    comptime OR = Self(DUCKDB_V2_EXPRESSION_TYPE_CONJUNCTION_OR)
    comptime CONSTANT = Self(DUCKDB_V2_EXPRESSION_TYPE_VALUE_CONSTANT)
    comptime PARAMETER = Self(DUCKDB_V2_EXPRESSION_TYPE_VALUE_PARAMETER)
    comptime FUNCTION = Self(DUCKDB_V2_EXPRESSION_TYPE_BOUND_FUNCTION)
    comptime CASE = Self(DUCKDB_V2_EXPRESSION_TYPE_CASE_EXPR)
    comptime COALESCE = Self(DUCKDB_V2_EXPRESSION_TYPE_OPERATOR_COALESCE)
    comptime COLUMN = Self(DUCKDB_V2_EXPRESSION_TYPE_BOUND_COLUMN_REF)

    def is_comparison(self) -> Bool:
        """`=`, `<>`, `<`, `>`, `<=` or `>=`."""
        return (
            self == Self.EQUAL
            or self == Self.NOT_EQUAL
            or self == Self.LESS
            or self == Self.GREATER
            or self == Self.LESS_EQUAL
            or self == Self.GREATER_EQUAL
        )

    def flipped(self) -> Self:
        """The comparison with its sides swapped (`a < b` is `b > a`)."""
        if self == Self.LESS:
            return Self.GREATER
        if self == Self.GREATER:
            return Self.LESS
        if self == Self.LESS_EQUAL:
            return Self.GREATER_EQUAL
        if self == Self.GREATER_EQUAL:
            return Self.LESS_EQUAL
        return self

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ExprKind(", self.value, ")")


comptime _ColumnResolver = def(Pointer[NoneType, MutUntrackedOrigin], Int) raises thin -> Int


@fieldwise_init
struct Comparison(Movable):
    """`column <op> value`, with the column on the left."""

    var column: Int
    """The declared column index."""
    var op: ExprKind
    var value: Value


@fieldwise_init
struct Between(Movable):
    """`column BETWEEN low AND high`.

    DuckDB also builds this node from `column >= low AND column < high`, and
    the V2 API does not say whether each bound is inclusive. Use it to narrow
    a scan (treating both bounds as inclusive) but do not take the filter
    over: return False from `pushdown`, so DuckDB still applies it exactly.
    """

    var column: Int
    var low: Value
    var high: Value


struct Expression(ImplicitlyCopyable):
    """One node of a bound expression. Borrowed: valid only inside the callback that received it."""

    var _handle: duckdb_v2_expression_handle
    var _context: Pointer[NoneType, MutUntrackedOrigin]
    var _resolve: _ColumnResolver

    def __init__(
        out self, handle: duckdb_v2_expression_handle, context: Pointer[NoneType, MutUntrackedOrigin], resolve: _ColumnResolver
    ):
        self._handle = handle
        self._context = context
        self._resolve = resolve

    def kind(self) raises DuckDBError -> ExprKind:
        var t: DUCKDB_V2_EXPRESSION_TYPE = 0
        var err = null_error_info()
        check(lib().duckdb_v2_expression_get_type(self._handle, Pointer(to=t), Pointer(to=err)), err)
        return ExprKind(t)

    def return_type(self) raises DuckDBError -> LogicalType:
        var t = null[duckdb_v2_logical_type_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_expression_get_return_type(self._handle, Pointer(to=t), Pointer(to=err)), err)
        return LogicalType(_owned=t)

    def child_count(self) raises DuckDBError -> Int:
        var n: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_expression_get_child_count(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return Int(n)

    def child(self, i: Int) raises DuckDBError -> Expression:
        var c = null[duckdb_v2_expression_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_expression_get_child(self._handle, idx_t(i), Pointer(to=c), Pointer(to=err)), err)
        return Expression(c, self._context, self._resolve)

    def column(self) raises -> Int:
        """The declared column a `COLUMN` node refers to."""
        var idx: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_expression_column_ref_get_index(self._handle, Pointer(to=idx), Pointer(to=err)), err)
        return self._resolve(self._context, Int(idx))

    def value(self) raises DuckDBError -> Value:
        """The value of a `CONSTANT` node."""
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_expression_constant_get_value(self._handle, Pointer(to=v), Pointer(to=err)), err)
        return Value(_owned=v)

    def constant[T: Movable & Deinitable](self) raises DuckDBError -> Optional[T]:
        """The value of a `CONSTANT` node as `T`, `None` for NULL."""
        return self.value().get_optional[T]()

    def function_name(self) raises DuckDBError -> String:
        """The function a `FUNCTION` node (or a comparison: its operator) calls."""
        var n = duckdb_v2_identifier_t(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var err = null_error_info()
        check(lib().duckdb_v2_expression_function_get_name(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return str_to_string(n)

    def comparison(self) raises -> Optional[Comparison]:
        """`column <op> constant` (or `constant <op> column`, flipped), else `None`."""
        var kind = self.kind()
        if not kind.is_comparison() or self.child_count() != 2:
            return None
        var a = self.child(0)
        var b = self.child(1)
        var ka = a.kind()
        var kb = b.kind()
        if ka == ExprKind.COLUMN and kb == ExprKind.CONSTANT:
            return Comparison(a.column(), kind, b.value())
        if ka == ExprKind.CONSTANT and kb == ExprKind.COLUMN:
            return Comparison(b.column(), kind.flipped(), a.value())
        return None

    def between(self) raises -> Optional[Between]:
        """`column BETWEEN constant AND constant`, else `None`. See `Between`
        for why such a filter can narrow a scan but not be taken over."""
        if self.kind() != ExprKind.BETWEEN or self.child_count() != 3:
            return None
        var c = self.child(0)
        var low = self.child(1)
        var high = self.child(2)
        if c.kind() != ExprKind.COLUMN or low.kind() != ExprKind.CONSTANT or high.kind() != ExprKind.CONSTANT:
            return None
        return Between(c.column(), low.value(), high.value())
