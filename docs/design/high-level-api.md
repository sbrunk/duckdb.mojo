# Design of the high-level Mojo API

The high-level API is a type-safe, idiomatic Mojo API built on DuckDB's V2 C API. This document describes its structure and the decisions
behind it, as well as the constraints that DuckDB's V2 API and the Mojo
language place on it. References of the form "V2 `foo`" refer to
functions in `third_party/duckdb/api_spec/v2`.

## Overview

The API covers querying DuckDB (connections, statements, results), reading
and writing typed data (columns, values, the appender), extending DuckDB
from Mojo (scalar, aggregate and table functions, casts, macros, `COPY`
formats, Arrow), and building DuckDB extensions as shared libraries.

Decisions on scope:

- The API targets the V2 C API only.
  The generated V1 bindings remain as an escape hatch for
  low-level users, but the API does not use them.
- DuckDB 2.0 marks every V2 function unstable. The package therefore pins
  `libduckdb` to the exact DuckDB version and is rebuilt for every DuckDB
  patch release, until the functions it uses are stable.
- The user-facing names follow the Python client (`connect`, `sql`,
  `execute`, `fetchall`), with one name per concept; aliases such as
  `query`, `cursor` and `rowcount` do not exist. Typed access is written
  `get[T]()`, and user-defined structs map to columns by their field names.

Design principles:

1. A single trait, `DuckDBMapped`, describes how a Mojo type maps to DuckDB:
   its logical type, how a column of it is read, how a value of it is
   written, and how it becomes a `Value`. Results, parameters, the
   appender, values and function signatures all go through this trait, so
   every part of the API accepts exactly the same set of types.
2. All column access goes through one reader. It presents flat, constant
   and dictionary vectors through a single vector view (`data`, `validity`,
   `sel`) and flattens the remaining encodings. No other module reads raw
   vector layouts, except the zero-copy paths listed under "Performance".
3. All failures are reported as `DuckDBError`, which holds one message and
   one code from the V2 error enum, grouped by its ranges (IO, input,
   resource, type, query, database, configuration, runtime).
4. Objects that own a DuckDB resource free it on destruction; objects that
   borrow one hold a compile-time reference to the owner (an "origin"), so
   a borrow cannot outlive what it borrows. There are no explicit
   ownership flags.
5. Functions are defined once and registered either on a connection or in
   an extension: both implement the `Registrar` trait.
6. The V2 API's irregular behavior is contained in the wrappers and never
   reaches the caller (section "DuckDB V2 behavior the wrappers contain").

## Modules

| Module | Contents |
|---|---|
| `_libduckdb_v2.mojo` | generated raw V2 bindings |
| `_libduckdb.mojo` | generated raw V1 bindings (unused by this API) |
| `api_v2.mojo`, `extension_v2.mojo` | raw V2 runtime (library global, `StrArg`, `null`), raw extension entrypoint |
| `_runtime.mojo`, `error.mojo` | process environment, built-in type factory, `check`; `DuckDBError`, `ErrorCode` |
| `types.mojo`, `scalar_types.mojo` | `TypeId`, `LogicalType`; `Date`, `Time`, `Timestamp`, `Decimal`, `UUID`, `Interval`, `Bit`, ... |
| `mapping.mojo`, `serde.mojo` | `DuckDBMapped` for scalars; emberserde formats for composite types, `ChunkWriter`, `VectorWriter` |
| `value.mojo` | `Value` |
| `database.mojo`, `default.mojo` | `Database`, `Connection`; module-level API on a default connection |
| `statement.mojo`, `result.mojo`, `chunk.mojo` | `Params`, `PreparedStatement`; `Result`, `MaterializedResult`, `Row`; `Chunk`, `ColumnReader` |
| `relation.mojo`, `expr.mojo` | SQL-string relations and fragment helpers |
| `appender.mojo` | `Appender` |
| `functions.mojo`, `aggregate.mojo`, `table.mojo`, `signature.mojo` | scalar, aggregate and table functions, signatures |
| `extend.mojo`, `expression.mojo`, `copy.mojo` | casts, replacement scans, macros, pragmas, options, custom types; filter-pushdown expressions; `COPY` formats |
| `settings.mojo`, `files.mojo`, `observe.mojo`, `arrow.mojo` | settings, file system, profiling/logging, Arrow |
| `extension.mojo` | `Extension` (V2 entrypoint) |
| `simd_math.mojo`, `kernels/` | `register_simd_math`; SIMD kernels |

The package root re-exports the module-level functions of `default.mojo`
(`duckdb.sql`, `duckdb.execute`, `duckdb.connect`, ...), which run on a
process-wide in-memory connection.

## Connections and their lifetime

V2 distinguishes three levels: an environment, which owns the instance
cache; an instance, which owns databases; and a connection. The API exposes
two of them. The environment exists once per process, is created on first
use and is never destroyed (`environment_destroy` refuses while instances
are alive). Because it is shared, the instance cache spans everything
opened through the package: opening the same file twice reports
`RESOURCE_IN_USE` instead of creating two independent instances on one
file.

A `Database` wraps an instance with one database attached as its default:
`Database(path=":memory:", config=...)`. Startup options are passed to
`instance_create_with_options`; options that require a running instance are
applied immediately afterwards through the instance config. A `Connection`
borrows its `Database` and keeps it alive through a reference count, so a
connection may outlive the `Database` object it was created from.
`duckdb.connect(path, *, read_only, config)` returns a connection that
implicitly owns its database.

```mojo
var con = duckdb.connect("my.db")
var db = Database(":memory:", config={"threads": "4"})
var a = db.connect()
var b = db.connect()
```

The methods of `Connection` are `execute`, `sql` (which returns a
`Relation`), `prepare`, `begin`/`commit`/`rollback`, `interrupt`,
`progress`, `settings`, `register` and `close`.

## Statements and parameters

V2 separates parsing from execution: `parse_sql` turns a string into
statements, and each parsed statement is executed with an array of
parameter values. The API follows the same order.

`con.execute(sql, *args)` binds positional parameters (`?` or `$1`, `$2`,
...) through the type mapping, so any mapped type may be a parameter:
scalars, lists, structs, maps. Named parameters are bound with
`con.execute(sql, params=...)` from a `Dict[String, Value]` or from a
struct whose field names are the parameter names. A string holding several
statements separated by `;` runs them in order: every statement but the
last is executed to completion, and the returned result is the last
statement's. Parameters require a single statement.

`con.prepare(sql)` produces a `PreparedStatement` that plans once and can be
executed repeatedly with `stmt.execute(*args)`;
`stmt.reuses_plan()` reports whether the plan was taken from the plan
cache. Because V2 passes all parameter values at execution time, there is
no per-parameter bind step and no way to clear bindings.
`con.describe(sql)` (`statement_bind`) returns the result columns and the
parameter types without executing anything.

## Results

A V2 result is lazy: rows are produced by DuckDB only while the caller
reads them, the connection rejects other statements while a result is open,
and a statement with side effects takes effect only once it is fully
consumed. Destroying a result before it is exhausted aborts its statement,
which invalidates the current transaction when one is open. The API
removes all of this for the caller except the deferred execution of
row-returning results.

The kind of a result (rows, a changed-rows count, or nothing) is known
before execution. Statements that return no rows (`INSERT`, `UPDATE`,
`DELETE`, DDL) are therefore executed to completion inside `execute`, and
the returned `Result` holds only `rows_changed()`: side effects have
applied and the connection is free again. Row-returning results stream.
`Result` deliberately does not hold a borrow on its connection: a V2 result
stays readable after its connection is destroyed, and Mojo's ownership
system does not prevent a second `execute` on a borrowed connection in any
case. Issuing a second statement while a result is open fails with
`RESOURCE_IN_USE` and DuckDB's own message, and a result that is no longer
used is destroyed at its last use, which releases the connection again.

Rows are read one of four ways:

```mojo
var users = con.execute("SELECT * FROM users WHERE age > ?", 30).fetchall().get[User]()
for chunk in con.execute("SELECT * FROM large").chunks(): ...
for row in con.execute("SELECT name, age FROM users").rows():
    print(row.get[String](0), row.get[Optional[Int32]](1))
var n = con.execute("DELETE FROM users WHERE age IS NULL").rows_changed()
```

`fetchone[T]()`, `fetchmany[T](n)` and `fetchall()` read rows; `fetchall`
collects the remaining chunks into a `MaterializedResult`, which owns them
(V2 hands out caller-owned chunks, so collecting needs no extra copy) and
releases the connection. A `MaterializedResult` indexes the start row of
each chunk, so random access does not scan. `result.show()` renders through
V2's `result_render_box`, the same renderer the DuckDB CLI uses.
`result.columns()` reports the result schema with full type information,
including decimal widths, list children and struct fields.

Reading a streaming result can fail in the middle of the loop, for example
on a division by zero in the fifth row. Because `Iterator.__next__` may
only raise `StopIteration` (section "Mojo language constraints"),
`result.chunks()` is not an `Iterator`: it uses a raising `__has_next__()`
to pull the next chunk and a non-raising `__next__()` to hand it out, so
the `DuckDBError` escapes the `for` loop. `result.try_chunks()` is the
alternative: a standard `Iterator` whose elements are chunk-or-error values,
with the error arriving as the final element.

Expanding statements (`PIVOT` and similar) do not know their columns until
execution has bound the expansion. A `Result` re-reads its metadata after
each step in that case (`_resolve_deferred`). `Connection.describe` and
`Relation.columns` do not execute the statement and report no columns for
these statements; see "Open points". Finally, `result.step()` and
`result.wait()` expose V2's non-blocking stepping for event-loop hosts: a
`Step` reports `WAITING`, `CHUNK`, `FINISHED` or `CANCELLED` and holds the
chunk.

## Chunks, columns and rows

A `Chunk` is either owned (from a result) or borrowed inside a callback with
an origin pointing at the callback's context. Its column types come from
the result schema or the function signature: a V2 vector does not include
its type, so every operation on one needs the type from elsewhere.

`chunk.column[T](i)` returns a `ColumnReader[T]`. Building the reader
resolves the vector view once (flattening sequence and FSST vectors if
needed), checks the column type once, and reads metadata (decimal width,
enum dictionary, ...) once; per-row reads then only apply the selection and
the validity mask. The reader offers `reader[i]` (raises on NULL),
`reader.get(i)` (an `Optional[T]`), `is_valid(i)`, `is_flat()`,
`is_constant()`, `has_nulls()`, and, for fixed-width numeric `T`, `span()`,
which yields the elements without copying when the vector is flat and has
no NULLs.

The convenience accessors `chunk.get[T](col=, row=)`, `chunk.get[T](row=)`
(row as struct or `Tuple`) and `chunk.get[T]()` (single-column results) are
built on the same readers. `chunk.value(col=, row=)` reads any cell as a
`Value`, for code that does not know the column types.
`chunk.array_span[T](col)` yields the elements of a flat ARRAY column as
one span without copying, or `None` when the column holds NULLs or a
selection. A `Row` borrows its chunk through an origin, so the compiler
rejects a row that outlives its chunk.

## The type mapping

`DuckDBMapped` is the single description of how a Mojo type appears in
DuckDB:

```mojo
trait DuckDBMapped:
    @staticmethod
    def logical_type(factory: Factory) raises -> LogicalType   # schema / signature
    @staticmethod
    def check(type: LogicalType) raises                        # can this column be read as Self?
    @staticmethod
    def read(view: VectorView, row: Int) raises -> Self        # row already sel-resolved, valid
    def write(self, mut writer: ColumnWriter, row: Int) raises # into a flat vector
    def to_value(self, factory: Factory) raises -> Value
```

The trait is implemented through `__extension` for `Bool`, the integer and
floating-point types, `Int128`/`UInt128`, `String`, `List[UInt8]` (BLOB),
and the value structs of `scalar_types.mojo` (`Date`, `Time`, `TimeNS`,
`TimeTZ`, the `Timestamp` variants, `Interval`, `Decimal`, `UUID`, `Bit`).
`Date`, `Time` and `Timestamp` compute their calendar fields in Mojo,
verified against DuckDB for dates back to 2200 BC. The composite types are
mapped generically: `Optional[T]` converts NULL to and from `None`,
`List[T]` maps to LIST (and to ARRAY when the column is an array),
`Dict[K, V]` maps to MAP, `Variant[...]` maps to UNION, and `Tuple[...]`
maps to STRUCT by position. A bare list literal passed to a generic
parameter is an `Array[T, n]` in Mojo rather than a `List`, so `Array` is
mapped as well. Struct types are mapped by emberserde; see the next
section.

Reading decodes DuckDB's storage representations in one place: the 16-byte
`bytes` layout of VARCHAR/BLOB/BIT, the UUID bit flip, the packing of
TIME_TZ, the decimal width tiers, the enum index widths, the UNION tag, and
MAP as a LIST of key/value structs. Writing uses the same trait, so
parameters, the appender and function outputs accept every type that
reading supports. A write is performed directly when the Mojo type's own
storage layout matches the column (any integer into an integer or DOUBLE
column, range-checked; `String` into an ENUM through the dictionary); every
other combination goes through `vector_set_value`, which asks DuckDB to
cast and produces DuckDB's error on failure.

## Composite types and emberserde

Structs and nested containers are serialized with
[emberserde](https://github.com/bgreni/EmberSerde), a Mojo serialization
framework in the style of Rust's serde. It is a git dependency pinned to
the `0.2.0` tag, built for Mojo 1.2.

**How emberserde works.** A type that wants to be serialized is described
by a sequence of events rather than by bytes: "struct begins", "field
`zip` has this value", "list of these elements", "this value is absent".
The type's `Serializer`/`Deserializer` implementation emits or consumes
those events. A *format* provides the other half: it implements the
matching traits and decides what the events are events *of*, turning them
into JSON text, into a binary stream, or, as here, into a position in a
DuckDB vector. Between these two halves the framework handles the struct
mechanics at compile time: it walks the fields, applies the
`Rename`/`Alias`/`Skip`/`Default` annotations, rejects unknown or duplicate
fields, and reports errors with a path such as `.address.zip` or `[17]`. It
provides event implementations for `Optional`, `List` (with staged
allocation for small lists), `Dict`, `Tuple`, `Variant`, `Array` and SIMD
types. A struct written for emberserde can therefore be read from a DuckDB
column and parsed from JSON with EmberJson without changes.

**How the API uses it.** `duckdb/serde.mojo` defines one emberserde format
whose representation is a DuckDB vector: a cursor that holds a vector tree
(built once per chunk from the column types) and a row index.
`_Cursor` implements `Deserializer` and the per-kind state traits so that
scalars are read at the cursor's row, `begin_seq` walks a LIST child's row
range, `begin_struct` walks a STRUCT column's children, and `begin_enum`
uses the UNION tag as the arm index. `_Writer` implements the matching
`Serializer` traits over a writable vector tree. `ColumnReader[T]` uses
`_Cursor` for any non-scalar `T`, and `ChunkWriter` uses `_Writer`: structs,
nested containers and whole rows-as-structs all go through this one path.

Details that need care:

- *Scalar intercept.* emberserde first gives a type the chance to decode
  itself, before the format sees anything. The native DuckDB types would
  therefore bypass the vector layout. The API intercepts them inside its
  own state methods: `expect_field_value[T]` and `expect_element[T]` check
  at compile time whether `T` has a native mapping (`Date`, `Decimal`,
  `UUID`, the timestamps, `Interval`, `Int128`, ...) and read the layout
  directly; anything else is handed to emberserde's `deserialize[T]`. The
  native types also implement emberserde's generic traits, so the same
  values still serialize to JSON correctly.
- *Field names.* The schema side of the mapping (section "The type
  mapping") must agree with emberserde about which field a column belongs
  to, so `check`, `logical_type` and struct-column matching use
  emberserde's exported `wire_field_names` and `field_index`, which apply
  `Rename`, `Alias` and `RenameAll`. DuckDB identifiers compare
  case-insensitively while emberserde compares field names byte-wise, so
  column names and STRUCT child names are resolved to field indexes
  case-insensitively once per chunk, and the struct state then advances by
  index instead of comparing strings per row. UNION members match
  `Variant` arms by name when every member name is an arm's tag
  (annotations on the arm types), and otherwise by type, when exactly one
  arm can hold the member; several members may share an arm, and a member
  that fits several arms is an error.

Errors keep the path: emberserde raises `DeserializationError` and
`SerializationError` from inside the walk, and the public entry points turn
them into `DuckDBError`s that add the row and column, for example
`row 17, column address: .zip: missing field`.

Some work stays outside emberserde because per-cell dispatch through a
format layer is not acceptable there. emberserde has no facility to
describe a type (a serializer sees values, not types), so `logical_type`,
`check` and the struct-column-to-field matching live in `DuckDBMapped`,
built on Mojo reflection plus emberserde's wire-name helpers. The
zero-copy `span()`, the SIMD scalar wrappers and the aggregate
`update_batch` fast path read vector layouts directly.

The costs: user structs with non-trivial fields must be `Defaultable`,
because emberserde builds a struct by default-constructing it and assigning
fields, so any struct with a `String`, `List` or nested-struct field needs
an `__init__(out self)`. (Writing has no such requirement.) Struct reads
also pay a per-cell round trip through the format layer, which is slower
than reading the layout directly; the column-to-field match is computed
once per chunk, so the per-row cost is the format layer itself.

## Logical types and values

A `LogicalType` is always owned. It is built from an id
(`LogicalType(TypeId.INTEGER)`), from SQL text
(`LogicalType.parse("STRUCT(a INTEGER, b VARCHAR[])")`, V2
`create_type_from_text`), from the type mapping (`LogicalType.of[T]()`), or
from the constructors for parameterized types (`decimal(18, 3)`,
`list(child)`, `struct_(...)`, `map(...)`, `array(...)`, `enum(...)`).
Inspection offers `id()`, `name()`, `to_text()`, deep `==`, and typed
accessors (`decimal_width()`, `scale()`, `child()`, `fields()`,
`enum_values()`, ...) built on V2's generic `get_param`.

Constructing a type or a value requires a *factory* in V2, which is bound
to the context that creates it. The API takes the factory from the
surrounding object (connection, extension, callback) and uses the instance
factory for built-in types, so callers rarely see factories.

A `Value` is always owned. `Value(x)` accepts any mapped type, including
nested ones, by a uniform route: the value is written into a one-row chunk
and the cell is taken with `vector_get_value`; reading does the reverse
(`vector_set_value` into a one-row chunk, then the cursor). This means
every type the column writer supports is also usable as a value and as a
statement parameter. Values offer `get[T]()`, `cast(type)`, `is_null()` and
`__str__`. `Value` is `Copyable` because V2 has no value copy; the copy
constructor casts a value to its own type, which DuckDB short-circuits to a
deep copy.

## The appender

V2 has no appender. `Appender` reproduces its behavior from the two
mechanisms DuckDB's own C++ appender is built on: a column data collection
(a buffer-managed set of chunks that can spill to disk) and
`statement_add_collection`, which makes a collection visible to a statement
under a table name.

`Appender(con, "table")` reads the table's column types, then allocates a
data chunk of those types and a matching collection. `append_row(struct)`,
`append_row(*values)`, `append_value(x)` and `append_rows(list)` write into
the chunk through `ColumnWriter`; a full chunk is moved into the
collection. `flush()` executes a prepared `INSERT INTO t (cols) SELECT *
FROM coll` over the collection and clears it. Like DuckDB's appender it
flushes every `flush_rows` rows (default 204,800), on `close()`, and on
destruction; the destructor cannot raise, so it prints a failing flush's
error, and `close()` raises it. A batch whose `INSERT` fails is dropped,
and the error states how many rows were lost. `on_conflict=
OnConflict.IGNORE/REPLACE` turns the statement into `INSERT OR
IGNORE/REPLACE`. Buffering is visible to users: rows become visible at
flush, and constraint violations surface at flush, for the whole batch. The collection must be destroyed before its database; the
borrowed connection guarantees this.

Omitted columns and their defaults need a second mechanism. V2 exposes
only whether a column has a default (`column_description_has_default`),
not the value, so the appender cannot fill defaults itself. Each appended
row instead records, in a hidden INTEGER column, the id of the set of
columns it left out, and a flush runs one `INSERT` per set with those
columns missing from the column list, so DuckDB evaluates the defaults per
row. Sequence defaults (`nextval(...)`) work. While a batch contains
several such sets, its `INSERT`s are separate statements: not atomic
outside a transaction, and the sets are inserted in set order. A row that
omits every column is rejected.

## Functions

A function definition is a description: the DuckDB function object is
created when it is registered, using the registrar's factory. That is what
lets one definition work on a connection and in an extension, which look up
different sets of V2 functions. The `Registrar` trait provides `factory`
and `register` for each function kind; `Connection` registers through
`connection_register_*` (transactional) and `Extension` through
`extension_register_*`. Not all kinds are transactional (casts, custom
options, log sinks, replacement scans), and a replacement scan cannot be
unregistered. V2 offers macros only for extensions; registering a macro on
a connection makes the wrapper run `CREATE TEMPORARY MACRO`, which lives as
long as the connection.

### Signatures and properties

A `Signature` is built from Mojo types, either directly
(`Signature[Int64, Int64, returns=Int64]`) or with a builder that adds
names, defaults and parameter kinds: `.named("x", "factor")`,
`.default("factor", value)`, `.named_only(...)`, and for chunk-level scalar
and table functions `.varargs(name, type)` and `.kwargs(name, type)`; table
functions also accept `.add_named(name, default)`. Mojo cannot read a
function's parameter names, so names are always declared. Row-wise
functions and aggregates use `fixed` signatures: they reject
`*args`/`**kwargs` and place named-only parameters last, so argument `i`
corresponds to declared parameter `i`.

Function properties are keyword arguments of the constructors:
`volatile`, `null_handling`, `fallible`, plus `order_dependent` and
`distinct_dependent` for aggregates. Registering a second function under an
existing name adds an overload, for scalar and table functions; DuckDB 2.0
rejects a second aggregate under one name, and one registration must use
one builder object, because re-registering a builder drops its user data.
Parameters and results of type `TypeId.ANY` are accepted for chunk-level
scalar functions, and for row-wise functions in the form of a `Value`
parameter or result. The result type of an ANY call is declared with
`.result_type_of(i)` or computed with `.resolve_type[f]()`; the resolve
callback sees the argument types before DuckDB casts them to the declared
parameter types.

### Scalar functions

A scalar function can be written at three levels of abstraction, and each
wrapper is generated from the Mojo function at compile time
(`scripts/generate_scalar_functions.py`).

*Row-wise* functions are ordinary Mojo functions:
`ScalarFunction.from_function["add", add]` for
`def add(a: Int64, b: Int64) -> Int64`. Arguments and results use the type
mapping, so strings, lists and structs work as well as numbers; there is
one generated overload per arity, up to eight arguments. NULL handling is
the wrapper's job: under default handling, a NULL argument produces a NULL
result without calling the function, and an `Optional` argument instead
receives NULLs as `None`, which makes the wrapper set SPECIAL null
handling. A raising function fails the query with its message.

*SIMD* functions (`from_simd_function["sqrt", Float64, math.sqrt]`)
compute over `SIMD` registers. The wrapper takes a zero-copy span of each
flat, NULL-free input, processes a constant input without expanding it (so
`f(x, 2.0)` stays on the SIMD path), and falls back to a row loop for
dictionary inputs or NULLs. When all arguments and the result are numeric
and the inputs are flat or constant without NULLs, the wrapper calls the
function in a raw array loop.

*Chunk-level* functions
(`ScalarFunction(name, signature, exec[fn])`, `fn(args: ScalarArgs,
mut out: VectorWriter[T])`) receive `ColumnReader`s and write the result
column directly, for hand-written kernels. They can also request
thread-local state with `init=init_callback[T]()` and read it with
`args.local_state[T]()`; row-wise functions do not offer thread state,
because a `mut` parameter would multiply the generated overloads.

The bind, resolve-types and init callbacks are optional, typed and
generated. Bind data is a struct conforming to `BindData`
(`bind(args: BindArgs) raises -> Self`) that appears as the function's
first Mojo parameter and is not part of the SQL signature. `BindArgs`
offers `count()`, `type(i)`, `constant[T](i)`, `index(name)` and
`name(i)`. The bind data is heap-allocated with a generated destructor,
receives an equality callback when the type is `Equatable`, and reaches the
exec callback read-only, shared by all threads.
`register_simd_math(con)` registers `mojo_sqrt`, `mojo_sin`, `mojo_cos`,
`mojo_ln`, `mojo_exp` and `mojo_log10` from the kernels in
`duckdb/kernels/`.

### Aggregate functions

An aggregate is a state struct that conforms to `AggregateState`: it has
`__init__(out self)`, `update(mut self, *args)` for one row,
`combine(mut self, other)` and `finalize(self, mut out: ColumnWriter,
row: Int)`. `AggregateFunction.from_state["name", State.update]()` infers
the state and argument types from the method, with generated overloads for
one to eight arguments.

Each state is allocated in DuckDB's state memory together with a "saw a
row" flag, so a group with no rows finalizes to NULL; the wrapper skips
NULL rows unless the argument is `Optional`. `combine` merges into the
target's freshly initialized state, so no copy of the source is needed.
When every row of a chunk belongs to one state and none is NULL, a state
that also conforms to `BatchUpdate` receives whole arrays instead of single
values; the prepared reductions `from_sum`, `from_min`, `from_max`,
`from_product`, `from_mean` and `from_reduce` use this to keep their SIMD
speed under `GROUP BY`. For integral `D` up to 64 bits, `from_sum`
accumulates in `Int128` and raises on overflow, matching DuckDB's `sum`,
which promotes `BIGINT` to `HUGEINT`; wider types have no wider accumulator
and wrap, as DuckDB's `sum` does there. A zero-length vectorized reduction
starts from the operation's identity (0 for sum, 1 for product, the type
maximum for min, the type minimum for max). A state can also declare
aggregate bind data by conforming to `AggregateBind`
(`comptime Bind: BindData`, `init_from(bind)`).

Every aggregate registered through the C API gets bind data holding its
name, with an equality callback on the name (`_identity_bind`). DuckDB
considers two aggregate calls the same expression when their callbacks,
argument types, return type and bind data all match, and all C API
aggregates share the same C++ callbacks; without the name, `SELECT f(x),
g(x)` would compute one of them twice. This is a DuckDB limitation (see
"Open points").

### Table functions

A table function is a struct conforming to `TableFunctionImpl`: `bind`
reads the arguments and declares the output columns, `init` creates the
per-thread state, and `scan` fills one batch of rows at a time. It is
registered with `TableFunction.from_impl[T](name, param_types[...]())`.

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
```

Columns are declared in `bind` with `columns.add[T](name)` or, for a row
struct, `columns.add_row[Struct]()`; `set_cardinality` states the expected
row count. `TableOutput` writes by declared column index
(`write`, `write_null`, `write_row`) and skips columns the query does not
consume (`wants(col)`): projection pushdown is always on.
`out.reference(...)` binds an output column to an existing vector instead
of copying; the Arrow importer uses this.

Parallel scans run with one state per thread. `max_threads` (default 1)
allows them, `ScanShared.claim()` distributes work units across threads and
`claimed()` feeds `progress(shared)`. By default DuckDB preserves row
order, which limits the parallelism a scan can use; a function can declare
its rows unordered (`columns.set_unordered()`, so parallel scans may feed
results, `INSERT` and `COPY`) or keep order by numbering its batches
(`OrderedBatches`, `out.set_batch(i)`). Filter pushdown is an optional
`pushdown(mut self, filter: Expression) -> Bool`, called per filter.
`Expression` exposes kind, children, return type, constants, columns
(resolved to declared indexes) and function names; `comparison()` and
`between()` recognize `column <op> constant` and ranges. DuckDB's optimizer
rewrites `i >= a AND i < b` into a single `BETWEEN` node, and the V2
expression API does not report whether the bounds are inclusive, so such a
filter may narrow a scan (inclusively) but must not be claimed as
fully understood ("Open points"). The wrapper also sizes every output
vector, because V2 takes the chunk size from the first vector only.

### Other extension points

The remaining registration kinds are thin typed wrappers over their V2
builders, in `extend.mojo`, `copy.mojo`, `settings.mojo`, `files.mojo`,
`observe.mojo` and `arrow.mojo`.

`CastFunction.from_function[f]` wraps a row-wise cast; in `TRY_CAST` a
raised error becomes NULL, in `CAST` it fails the query. An `implicit_cost`
and explicit source/target types are optional, the latter for catalog
types. `ReplacementScan.from_function[f]` resolves table names that the
catalog does not contain (`SELECT * FROM 'data.xyz'`): the function gets
the name as written and returns a `Replacement`, a table function call with
arguments or a subquery, optionally aliased, or `None` to decline.
`MacroFunction.scalar_macro`/`table_macro` declare defaults; a NULL
default is emitted as a typed NULL in the generated `CREATE MACRO` SQL.
`PragmaFunction.query[f]`/`exec[f]` register pragmas.
`CustomOption.of(name, default, description)` registers a custom setting,
with an optional `.validate[f]()` (raise to reject, return a replacement to
normalize; valid for both scopes) and `.global_only()`.
`CustomType.of[T](name)` / `CustomType(name, base)` register a custom type;
its values keep the base storage, so Mojo reads them as the base type, and
casts to and from them resolve through
`LogicalType.parse(name, con.factory())`.

Settings are reachable from a connection (`con.settings()`), from scalar
and bind callbacks (`args.settings()`) and from an extension
(`ext.settings()`), with `get[T](name)`, `value(name)` and
`set(name, value, scope)`. Profiling and logging live in `observe.mojo`:
`con.enable_profiling()` sets `enable_profiling = 'no_output'` so that
DuckDB does not print the tree of every query unless `print_profile=True`,
`con.query_metrics()` returns the profile as a tree of `MetricsNode`s with
named metrics, and `con.log(level, type, message)` writes an entry.
`LogSink.from_function[f](name)` registers a Mojo log sink (`LogSink`),
whose callback receives `LogEntry` values once the database selects it with
`SET logging_storage = '<name>'`.

The file system is reachable as `con.file_system()`, `args.file_system()`
and `ext.file_system()`: `exists`, `is_remote`, `expand`, `glob`, `move`,
`remove`, and `open` (flags are keyword arguments; since `read` is reserved
in Mojo they are named `readable`/`writable`) returning an owned `File`
with `read`, `read_at`, `write`, `write_at`, `seek`, `tell`, `size`, `sync`
and `close`, plus `read_all`/`write_all` on the file system itself.

Arrow data leaves the API through `con.execute_arrow(sql, *args,
batch_size=)`, which returns an `ArrowResult` that can be inspected
(`schema()`, `fetch_array()`) or handed on as a standard `ArrowArrayStream`
(`into_stream()`). It enters through `con.arrow_scan(stream)`, a relation
over a stream that is read by a hidden table function (`__mojo_arrow_scan`)
referencing the imported vectors without copying. An exported stream keeps
its connection busy until it has been read, so a full round trip needs two
connections. Nested columns, NULLs, filters and projections are supported
through the Arrow path.

`COPY` formats are registered with
`CopyFunction(name, extension=).writes[W]().reads[R]()`. A `CopyToFormat`
provides `bind(info)`, `open(path) -> File`, `write(mut file, rows:
Chunk)` and `close(mut file)`; DuckDB serializes the write calls of one
file and keeps their order, and the batch callback only retains the rows'
collection. A `CopyFromFormat` provides `bind`, `init` and `scan(mut
state, mut out: TableOutput)`. `CopyInfo` gives both the path, the columns,
typed options (`option[T](name)`) and the file system.

## Extensions

```mojo
def init(ext: Extension) raises:
    ext.register(ScalarFunction.from_function["mojo_add", add])

@export("my_ext_init_c_api_v2")
def entry(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    Extension.run[init](input)
```

`Extension.run[init]` is the driver for the generated entrypoint. The
`Extension` object implements `Registrar` and gives the load context's
factory, config and file system. Because DuckDB 2.0 has no stable V2
functions, the entrypoint uses the unstable extension API struct
(`duckdb_ext_api_v2_unstable`) and must be built with a matching footer
(`append_extension_metadata.py --abi-type C_STRUCT_UNSTABLE`); like
DuckDB's C++ extensions, such an extension loads only into the exact
DuckDB build it was compiled against. V2 gives an extension no way to run
SQL: code that needs a `Connection` cannot run inside an extension, which
the type system shows directly, as `Extension` is not a `Connection`. The
built-in type factory (a hidden in-memory instance) does work inside an
extension.

## DuckDB V2 behavior the wrappers contain

This is the inventory of V2 irregularities that the wrappers take over from
the caller. New code that touches the raw API needs the same care.

- Error slots: the return code is authoritative, and the error object may
  hold a stale message; a failing call reuses the slot. One fresh slot per
  call, destroyed after reading. Inside a callback, only the code fails the
  call, not the text, and the callback's own slot must not be passed to
  calls whose failure is handled.
- The result vector of a scalar callback has size 0 until it is set: sizes
  must be set before NULLs are written (`VectorWriter` does this).
- Data chunks have no `set_size`; every vector in one must be sized.
- Default null handling still delivers per-row NULLs to scalar exec and
  aggregate update callbacks.
- Sequence, FSST and shredded vectors must be flattened before reading;
  flattening a dictionary invalidates earlier pointers into its child.
- Query results are always flat vectors; non-flat vectors only occur inside
  callbacks. The result reader can therefore ignore the selection path,
  the UDF wrappers cannot.
- Statement side effects apply only when the result is drained; destroying
  an undrained result can invalidate the current transaction.
- `parse_sql` takes a NUL-terminated string; all other text inputs are
  length-delimited views.
- Arrow import and export require a callback context, so client-side Arrow
  goes through `statement_execute_arrow` and
  `arrow_result_to_arrow_c_stream` instead.
- `environment_destroy` fails while instances are alive, so the process
  environment is never destroyed.
- Writing a string inlines up to 12 bytes (zero-padded) and otherwise
  allocates in the target vector's arena; long strings survive resizes of
  a LIST child.
- Writing nested vectors: LIST children are grown with `vector_set_size`
  (geometric reserve, final size at the end), STRUCT children follow their
  parent's size, and `vector_set_null` preserves the nested NULL invariant.
  A failed write leaves the NULL written by `vector_set_null` in place, so
  the writer marks the row valid again (recursively over STRUCT, UNION and
  ARRAY) before the row is reused.

## Mojo language constraints

The following Mojo behavior shapes the API and explains several of its
apparent quirks.

**Extensions are visible only in their own module.** An `__extension`
conformance or method is not visible in other modules, so no public generic
can use `DuckDBMapped` as a bound. Public functions take `T: Movable &
Deinitable` and call the dispatch functions of `mapping.mojo`
(`mapped_read[T]`, `mapped_accepts[T]`, `mapped_to_value[T]`, ...), which
live in the same module as the extensions and check
`conforms_to(T, DuckDBMapped)` there; an unmapped `T` is a compile-time
error with a clear message, and user types that conform to the trait
directly keep working. Methods such as `sql`, `table` and `read_*` live in
the module that declares the extensions they dispatch through.

**A value is destroyed after its last use.** Mojo destroys a value at its
final *use*, not at the end of its scope. A raw handle or pointer taken
from an owner (`x.handle()`, `list.unsafe_ptr()`, a chunk's data) and
handed to DuckDB is therefore freed before the C call may even run, unless
the owner is kept alive past the call (`_ = owner^`) or the borrower holds
the owner's origin. This applies to every borrow in the API: `Settings`,
`FileSystem`, `MetricsNode`, `Row` and `ColumnReader` hold their owner's
origin for exactly this reason; without it, the last statement of a block
destroys the owner first, which shows up as misleading DuckDB errors rather
than as a clean failure.

**Raising iterators.** `Iterator.__next__` may only raise `StopIteration`,
which a `for` loop swallows. Iteration that can fail uses the alternative
loop protocol the compiler accepts: a raising `__has_next__()` plus a
non-raising `__next__()` (`ChunkIter`). It is not an `Iterator`, so
`enumerate` does not apply; a standard `Iterator` of element-or-error
values (`TryChunkIter`) is offered beside it.

**Literals and temporaries.** A bare list literal passed to a generic
parameter is an `Array[T, n]`, not a `List`, so `Array` is mapped too.
Default argument values must be compile-time constants. Generic return
types need `Movable & Deinitable`, or the compiler cannot drop the
temporary.

**Copy constructors.** A `Copyable` copy constructor may not declare
`raises` (Mojo 1.2). `Value`'s copy constructor cannot therefore raise; its
only remaining failure is allocation, which aborts anyway.

## Performance

The per-cell path through the mapping runs only where the API itself is
per-cell. Where bulk work is possible, the implementation bypasses it and
touches the vector layout directly: scalar UDF arguments are read from
storage and results written to the result vector without going through the
mapping, `span()` and `array_span()` give away no copies, the raw-array
loop covers numeric SIMD functions, `BatchUpdate` covers aggregates, and
`Date`/`Time`/`Timestamp` calendar arithmetic is computed in Mojo rather
than by a DuckDB call per value. The relevant benchmarks are
`benchmark/udf_bench.mojo` (Mojo functions against built-ins),
`benchmark/serde_bench.mojo` (nested reads and appends) and
`benchmark/knn_compare.mojo` (vector search).

## Open points

The single list of open work. Update it when something is done or found.

**Before the release**
- **Packaging:** emberserde 0.2.0 must be released on modular-community
  (only 0.1.0, for Mojo 1.1, is there). A conda build cannot fetch a git
  dependency, so the recipes and the CI `conda-recipe` job fail until then.
  Every compiler bump also needs an emberserde rebuild in the chain before ours works again.
- **Upstream reports:** (1) DuckDB merges different C API aggregates with
  the same signature over the same arguments, because they share C++
  callbacks (see "Aggregate functions"); the `_identity_bind` workaround
  can go once this is fixed. (2) The V2 expression API does not report
  whether `BETWEEN` bounds are inclusive, so pushed-down ranges cannot be
  claimed (see "Table functions"). Both still need a minimal reproduction.

**Not covered yet**
- Table functions: partition info for the partitioned-aggregate
  optimization (`partitioning_set_partition_info` plus partition values per
  batch), claim-batch callbacks, `get_bind_info` (column identifiers,
  options), multi-file functions.
- Copy functions: parallel `COPY ... FROM`, progress for it, the batch size
  and statistics callbacks of `COPY ... TO`.
- Arrow converters inside callbacks (the importer and exporter as their own
  types; today only `arrow_scan` uses the importer).
- Log sinks: the flush callback.
- `ANY` parameters for aggregates.
- `BIGNUM` (new in DuckDB 2.0, bytes-backed): `TypeId.BIGNUM` exists but no
  Mojo type accepts it, so such columns are unreadable through the typed
  APIs. A `Bignum` struct decoded via `duckdb_v2_bignum_decode` would close
  the gap.
- **SIMD scalar functions:** per-chunk setup (type handles, write tree)
  costs about 0.3 µs per chunk, the rest of the gap to the pre-V2 speed.
- **DECIMAL writes of another width or scale** go through a DuckDB cast per
  value; a native rescale would be faster.
- **`fetchone`/`fetchmany` then chunks:** reading chunks raises while rows
  of the current chunk are unread (kept on purpose; it could hand back the
  rest of the chunk).
- **Row-wise thread state:** only chunk-level scalar functions get
  `LocalState`.
- **Appender destructor:** a flush that fails on destruction only prints
  the error. Forcing an explicit `close()` does not work on Mojo 1.2
  (linear types need `try`/`except` around every raising call; `with`
  copies its value). Revisit with a newer Mojo.
- **Stable V2 target:** extensions load only into the exact DuckDB build;
  revisit when DuckDB marks V2 functions stable.
- **Expanding statements and `describe`:** `PIVOT` and similar statements
  learn their columns while they run; `Result` re-reads the metadata per
  chunk (`_resolve_deferred`), but `Connection.describe` and
  `Relation.columns` do not run the query and report 0 columns for such
  statements. A way to report "deferred" instead of empty would be better.
- **Out-of-line string writes** fetch the vector's arena once per value;
  the arena is a property of the vector and could be fetched once per
  write.
