"""Function signatures: parameter names, kinds and default values.

Every function kind (scalar, aggregate, table) keeps its parameters in a
`Signature`. Without names the parameters are positional-only. The function
types offer chainable methods that change it:

```mojo
con.register(
    ScalarFunction.from_function["scale", scale]()
    .named("x", "factor")              # pass by position or by name
    .default("factor", Float64(2.0))   # optional, 2.0 when omitted
)
# SELECT scale(3.0), scale(3.0, 4.0), scale(factor := 4.0, x := 3.0)
```

DuckDB hands the arguments to the callbacks in a fixed order: positional
and standard parameters as declared, then the `*args` values, then named-only
parameters, then the `**kwargs` values. An omitted argument arrives with its
default value, so a function always sees every declared parameter.
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info
from duckdb._runtime import api_error, check, lib
from duckdb.error import DuckDBError
from duckdb.types import LogicalType
from duckdb.value import Value


@fieldwise_init
struct ParamKind(Equatable, ImplicitlyCopyable, Writable):
    var value: DUCKDB_V2_FUNCTION_PARAMETER_KIND

    comptime POSITIONAL = Self(DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_ONLY)
    """By position only (the default without names)."""
    comptime STANDARD = Self(DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD)
    """By position or by name."""
    comptime VARARGS = Self(DUCKDB_V2_FUNCTION_PARAMETER_KIND_POSITIONAL_VARIADIC)
    """`*args`: the remaining positional arguments."""
    comptime NAMED_ONLY = Self(DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_ONLY)
    """By name only."""
    comptime KWARGS = Self(DUCKDB_V2_FUNCTION_PARAMETER_KIND_NAMED_VARIADIC)
    """`**kwargs`: named arguments that match no other parameter."""

    def write_to(self, mut writer: Some[Writer]):
        writer.write("ParamKind(", self.value, ")")


struct Param(Movable):
    var name: String
    var type: LogicalType
    var kind: ParamKind
    var default: Optional[Value]

    def __init__(out self, var name: String, var type: LogicalType, kind: ParamKind):
        self.name = name^
        self.type = type^
        self.kind = kind
        self.default = None



struct Signature(Movable):
    """The parameters of a function."""

    var params: List[Param]
    var fixed: Bool
    """Set for functions whose callbacks read a fixed list of arguments
    (row-wise scalar functions, aggregates): they cannot take `*args` or
    `**kwargs`, and named-only parameters must come last."""

    def __init__(out self, types: List[LogicalType], *, fixed: Bool):
        self.params = List[Param]()
        for i in range(len(types)):
            self.params.append(Param(String("arg", i + 1), types[i].copy(), ParamKind.POSITIONAL))
        self.fixed = fixed


    def _declared(self) -> Int:
        """Parameters other than `*args` and `**kwargs`."""
        var n = 0
        for i in range(len(self.params)):
            var k = self.params[i].kind
            if k != ParamKind.VARARGS and k != ParamKind.KWARGS:
                n += 1
        return n

    def _find(self, name: StringSlice) raises DuckDBError -> Int:
        var lower = String(name).lower()
        for i in range(len(self.params)):
            if self.params[i].name.lower() == lower:
                return i
        raise api_error(String("the function has no parameter named ", name))

    def name(mut self, var names: List[String]) raises DuckDBError:
        """Name the declared parameters in order. They can then be passed by name."""
        if len(names) != self._declared():
            raise api_error(String("got ", len(names), " names for ", self._declared(), " parameters"))
        var k = 0
        for i in range(len(self.params)):
            var kind = self.params[i].kind
            if kind == ParamKind.VARARGS or kind == ParamKind.KWARGS:
                continue
            self.params[i].name = names[k].copy()
            if kind == ParamKind.POSITIONAL:
                self.params[i].kind = ParamKind.STANDARD
            k += 1

    def set_default(mut self, name: StringSlice, var value: Value) raises DuckDBError:
        var i = self._find(name)
        var kind = self.params[i].kind
        if kind == ParamKind.VARARGS or kind == ParamKind.KWARGS:
            raise api_error("*args and **kwargs cannot have a default value")
        self.params[i].default = value^

    def set_named_only(mut self, name: StringSlice) raises DuckDBError:
        var i = self._find(name)
        self.params[i].kind = ParamKind.NAMED_ONLY

    def add(mut self, var name: String, var type: LogicalType, kind: ParamKind) raises DuckDBError:
        if self.fixed and (kind == ParamKind.VARARGS or kind == ParamKind.KWARGS):
            raise api_error("*args and **kwargs need a chunk-level scalar function or a table function")
        self.params.append(Param(name^, type^, kind))

    def _check(self) raises DuckDBError:
        if not self.fixed:
            return
        # The callbacks read argument i as declared parameter i, which holds
        # as long as named-only parameters come last.
        var named = False
        for i in range(len(self.params)):
            var is_named = self.params[i].kind == ParamKind.NAMED_ONLY
            if named and not is_named:
                raise api_error("named-only parameters must come after the others")
            named = named or is_named

    def _add_to(self, signature: duckdb_v2_function_signature_handle) raises DuckDBError:
        self._check()
        var err = null_error_info()
        for i in range(len(self.params)):
            ref p = self.params[i]
            var name = StrArg(p.name)
            var default = null[duckdb_v2_value_handle]()
            if p.default:
                default = p.default.value().handle()
            check(
                lib().duckdb_v2_function_signature_add_parameter(
                    signature, name.ptr(), p.type.handle(), default, p.kind.value, Pointer(to=err)
                ),
                err,
            )
