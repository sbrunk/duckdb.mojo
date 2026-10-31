"""Data chunks and typed column readers."""

from std.memory import ArcPointer
from std.reflection import reflect
from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import null, null_error_info, str_to_string
from duckdb._runtime import api_error, check, lib
from duckdb.error import DuckDBError, ErrorCode
from duckdb.mapping import ColumnMeta, RawData, is_mapped, mapped_accepts, mapped_read
from duckdb.serde import _ReadTree, read_row, read_value
from duckdb.types import LogicalType, TypeId
from duckdb.value import Value


struct _Schema(Movable):
    """Column names and types of a result, shared by its chunks."""

    var names: List[String]
    var types: List[LogicalType]

    def __init__(out self, var names: List[String], var types: List[LogicalType]):
        self.names = names^
        self.types = types^


def _read_schema(schema: duckdb_v2_schema_handle) raises DuckDBError -> _Schema:
    """Copy a V2 schema's names and types (the schema keeps ownership of its own)."""
    var err = null_error_info()
    var names = List[String]()
    var types = List[LogicalType]()
    var n: idx_t = 0
    check(lib().duckdb_v2_schema_get_count(schema, Pointer(to=n), Pointer(to=err)), err)
    for i in range(Int(n)):
        var name = duckdb_v2_str(null[Pointer[c_char, ImmUntrackedOrigin]](), 0)
        var t = null[duckdb_v2_logical_type_handle]()  # borrowed from the schema
        check(lib().duckdb_v2_schema_get_field(schema, idx_t(i), Pointer(to=name), Pointer(to=t), Pointer(to=err)), err)
        var owned = null[duckdb_v2_logical_type_handle]()
        check(lib().duckdb_v2_logical_type_copy(t, Pointer(to=owned), Pointer(to=err)), err)
        types.append(LogicalType(_owned=owned))
        names.append(str_to_string(name))
    return _Schema(names^, types^)


struct Chunk(Movable, Sized):
    """An owned data chunk: a batch of rows, stored column by column.

    V2 vectors do not carry their type, so a chunk keeps the schema of the
    result it came from.
    """

    var _handle: duckdb_v2_data_chunk_handle
    var _schema: ArcPointer[_Schema]
    var _size: Int

    def __init__(out self, *, _owned: duckdb_v2_data_chunk_handle, schema: ArcPointer[_Schema]) raises DuckDBError:
        self._handle = _owned
        self._schema = schema
        var size: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_data_chunk_get_size(self._handle, Pointer(to=size), Pointer(to=err)), err)
        self._size = Int(size)

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle
        self._schema = move._schema^
        self._size = move._size

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_data_chunk_destroy(Pointer(to=self._handle))

    def __len__(self) -> Int:
        """The number of rows."""
        return self._size

    def column_count(self) -> Int:
        return len(self._schema[].types)

    def column_name(self, col: Int) -> String:
        return self._schema[].names[col].copy()

    def column_type(self, col: Int) -> LogicalType:
        return self._schema[].types[col].copy()

    def column[T: Movable & Deinitable](ref self, col: Int) raises DuckDBError -> ColumnReader[T, origin_of(self)]:
        """A typed reader over column `col`.

        `T` is a scalar type or a nested one (`List`, `Dict`, `Optional`,
        `Variant`, `Tuple`, a struct). For scalars the type is checked once
        and raises `ErrorCode.TYPE_MISMATCH`; nested types are checked per
        value.
        """
        return column_reader[T, origin_of(self)](
            self._vector(col), String("column ", col, " (", self._schema[].names[col], ")"), self._schema[].types[col], self._size
        )

    def array_span[T: Movable & Deinitable](self, col: Int) raises DuckDBError -> Optional[Span[T, origin_of(self)]]:
        """The elements of an ARRAY column (`FLOAT[256]`, ...) as one span of
        `len(self) * array_size` values in row order, without copying.

        `None` when the vector is not flat or has a NULL array or element;
        read those with `column[List[T]]`. `T` must be the element's numeric
        type (`Float32` for FLOAT), else it raises `ErrorCode.TYPE_MISMATCH`.
        """
        comptime assert reflect[T].base_name() == "SIMD", "array_span() needs a fixed-width numeric type"
        ref type = self._schema[].types[col]
        var what = String("column ", col, " (", self._schema[].names[col], ") is ", type)
        if type.id() != TypeId.ARRAY:
            raise DuckDBError(ErrorCode.TYPE_MISMATCH, String(what, ", not an ARRAY"))
        var element = type.child_types()[0][1].copy()
        if not mapped_accepts[T](element.id()):
            raise DuckDBError(ErrorCode.TYPE_MISMATCH, String(what, " and cannot be read as ", reflect_name[T](), " elements"))
        var vec = self._vector(col)
        var err = null_error_info()
        var kind: DUCKDB_V2_VECTOR_TYPE = DUCKDB_V2_VECTOR_TYPE_OTHER
        check(lib().duckdb_v2_vector_get_vector_type(vec, Pointer(to=kind), Pointer(to=err)), err)
        if kind != DUCKDB_V2_VECTOR_TYPE_FLAT:
            return None
        var child = null[duckdb_v2_vector_handle]()
        check(lib().duckdb_v2_vector_get_child(vec, 0, Pointer(to=child), Pointer(to=err)), err)
        var count = self._size * type.array_size()
        var parent = _view(vec)
        var elements = _view(child)
        if not _all_valid(parent.validity, self._size) or not _all_valid(elements.validity, count):
            return None
        return Span[T, origin_of(self)](
            unsafe_ptr=elements.data.unsafe_bitcast[T]().unsafe_origin_cast[origin_of(self)](), length=count
        )

    def _vector(self, col: Int) raises DuckDBError -> duckdb_v2_vector_handle:
        if col < 0 or col >= self.column_count():
            raise api_error(String("column index ", col, " out of range (", self.column_count(), " columns)"))
        var err = null_error_info()
        var vec = null[duckdb_v2_vector_handle]()
        check(lib().duckdb_v2_data_chunk_get_vector(self._handle, idx_t(col), Pointer(to=vec), Pointer(to=err)), err)
        return vec

    def _read_tree(self) raises DuckDBError -> _ReadTree:
        """Views of all columns for reading rows. Valid while the chunk lives."""
        var vectors = List[duckdb_v2_vector_handle]()
        for i in range(self.column_count()):
            vectors.append(self._vector(i))
        return _ReadTree(vectors, self._schema[].names, self._schema[].types)

    def get[T: Movable & Deinitable](self, *, row: Int) raises DuckDBError -> T:
        """Row `row` as `T`: a struct (columns matched to fields by name,
        case-insensitively), a `Tuple` (by position), or a scalar when the
        chunk has one column."""
        if row < 0 or row >= self._size:
            raise api_error(String("row ", row, " out of range (", self._size, " rows)"))
        var tree = self._read_tree()
        return read_row[T](tree, row)

    def get[T: Movable & Deinitable](self) raises DuckDBError -> List[T]:
        """All rows as `T` (see `get[T](row=)`)."""
        var out = List[T](capacity=self._size)
        self._append_rows[T](out)
        return out^

    def _append_rows[T: Movable & Deinitable](self, mut out: List[T]) raises DuckDBError:
        var tree = self._read_tree()
        for r in range(self._size):
            out.append(read_row[T](tree, r))

    def get[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> T:
        """One value. Raises on NULL (use `get_optional` to allow it)."""
        return self.column[T](col)[row]

    def value(self, *, col: Int, row: Int) raises DuckDBError -> Value:
        """One cell as a `Value`, of any type (NULL included). Slower than a
        typed read. For code that does not know the column types."""
        if row < 0 or row >= self._size:
            raise api_error(String("row ", row, " out of range (", self._size, " rows)"))
        var v = null[duckdb_v2_value_handle]()
        var err = null_error_info()
        check(lib().duckdb_v2_vector_get_value(self._vector(col), idx_t(row), Pointer(to=v), Pointer(to=err)), err)
        return Value(_owned=v)

    def get_optional[T: Movable & Deinitable](self, *, col: Int, row: Int) raises DuckDBError -> Optional[T]:
        """One value, or `None` for NULL."""
        return self.column[T](col).get(row)


def column_reader[T: Movable & Deinitable, origin: ImmOrigin](
    vec: duckdb_v2_vector_handle, what: String, type: LogicalType, size: Int
) raises DuckDBError -> ColumnReader[T, origin]:
    """A reader over `vec` (of `type`, `size` rows). For scalar `T` the type
    is checked here. `what` names the vector in the error."""
    var meta = ColumnMeta(type)
    comptime if not is_mapped[T]():
        var tree = _ReadTree([vec], ["v"], [type.copy()])
        return ColumnReader[T, origin](vec, meta^, size, ArcPointer(tree^))
    else:
        if not mapped_accepts[T](meta.kind):
            raise DuckDBError(
                ErrorCode.TYPE_MISMATCH, String(what, " is ", type, " and cannot be read as ", reflect_name[T]())
            )
        return ColumnReader[T, origin](vec, meta^, size)


def _view(vec: duckdb_v2_vector_handle) raises DuckDBError -> duckdb_v2_vector_view:
    var view = duckdb_v2_vector_view(
        null[RawData](),
        null[Pointer[UInt64, ImmUntrackedOrigin]](),
        null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
        0,
    )
    var err = null_error_info()
    check(lib().duckdb_v2_vector_get_view(vec, Pointer(to=view), Pointer(to=err)), err)
    return view.copy()


def _all_valid(validity: Pointer[UInt64, ImmUntrackedOrigin], count: Int) -> Bool:
    """Whether the first `count` bits of a validity mask are set (no mask: all valid)."""
    if Int(validity) == 0:
        return True
    for w in range(count >> 6):
        if validity[unsafe_offset=w] != ~UInt64(0):
            return False
    var rest = count & 63
    if rest != 0:
        var mask = (UInt64(1) << UInt64(rest)) - 1
        if validity[unsafe_offset=count >> 6] & mask != mask:
            return False
    return True


def reflect_name[T: AnyType]() -> String:
    return String(reflect[T].name())


struct ColumnReader[T: Movable & Deinitable, origin: ImmOrigin](Sized):
    """Reads the values of one column as `T`.

    Works on every vector representation: flat, constant and dictionary
    vectors are read through their selection, other representations
    (sequence, compressed) are flattened first. It borrows the chunk, so it
    cannot outlive it. Nested types are read through emberserde.
    """

    var _data: RawData
    var _validity: Pointer[UInt64, ImmUntrackedOrigin]
    var _sel: Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]
    var _meta: ColumnMeta
    var _size: Int
    var _constant: Bool
    var _tree: Optional[ArcPointer[_ReadTree]]  # nested types only

    def __init__(
        out self, vec: duckdb_v2_vector_handle, var meta: ColumnMeta, size: Int, tree: Optional[ArcPointer[_ReadTree]] = None
    ) raises DuckDBError:
        var err = null_error_info()
        var kind: DUCKDB_V2_VECTOR_TYPE = DUCKDB_V2_VECTOR_TYPE_OTHER
        check(lib().duckdb_v2_vector_get_vector_type(vec, Pointer(to=kind), Pointer(to=err)), err)
        if kind == DUCKDB_V2_VECTOR_TYPE_OTHER:
            check(lib().duckdb_v2_vector_flatten(vec, Pointer(to=err)), err)
        var view = duckdb_v2_vector_view(
            null[RawData](),
            null[Pointer[UInt64, ImmUntrackedOrigin]](),
            null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
            0,
        )
        check(lib().duckdb_v2_vector_get_view(vec, Pointer(to=view), Pointer(to=err)), err)
        self._data = view.data
        self._validity = view.validity
        self._sel = view.sel
        self._meta = meta^
        self._size = size
        self._constant = kind == DUCKDB_V2_VECTOR_TYPE_CONSTANT
        self._tree = tree

    def __len__(self) -> Int:
        return self._size

    @always_inline
    def _index(self, row: Int) -> Int:
        return Int(self._sel[unsafe_offset=row]) if Int(self._sel) != 0 else row

    @always_inline
    def _valid_at(self, idx: Int) -> Bool:
        if Int(self._validity) == 0:
            return True
        return (self._validity[unsafe_offset=idx >> 6] >> UInt64(idx & 63)) & 1 == 1

    def span(self) -> Optional[Span[Self.T, Self.origin]]:
        """The values without copying, when the vector is flat and has no
        NULLs, else `None`. Only for fixed-width numeric `T`."""
        comptime assert reflect[Self.T].base_name() == "SIMD", "span() needs a fixed-width numeric type"
        if Int(self._sel) != 0 or self.has_nulls():
            return None
        return Span[Self.T, Self.origin](
            unsafe_ptr=self._data.unsafe_bitcast[Self.T]().unsafe_origin_cast[Self.origin](), length=self._size
        )

    def is_flat(self) -> Bool:
        """Whether row i is stored at index i (no selection: not a constant or dictionary vector)."""
        return Int(self._sel) == 0

    def is_constant(self) -> Bool:
        """Whether every row is stored at index 0 (a constant vector, such
        as a literal argument of a function)."""
        return self._constant

    def has_nulls(self) -> Bool:
        if Int(self._validity) == 0:
            return False
        for i in range(self._size):
            if not self._valid_at(self._index(i)):
                return True
        return False

    def unsafe_data(self) -> RawData:
        """The storage (indexed through the selection, see `is_flat`)."""
        return self._data

    def is_valid(self, row: Int) -> Bool:
        """Whether row `row` is not NULL."""
        return self._valid_at(self._index(row))

    def __getitem__(self, row: Int) raises DuckDBError -> Self.T:
        """The value at `row`. Raises on NULL and for rows out of range."""
        if row < 0 or row >= self._size:
            raise api_error(String("row ", row, " out of range (", self._size, " rows)"))
        comptime if not is_mapped[Self.T]():
            # NULLs: an Optional reads them as None, other types raise.
            return read_value[Self.T](self._tree.value()[], 1, row)
        else:
            var idx = self._index(row)
            if not self._valid_at(idx):
                raise api_error(String("row ", row, " is NULL (use get() to read NULLs as None)"))
            return mapped_read[Self.T](self._meta, self._data, idx)

    def get(self, row: Int) raises DuckDBError -> Optional[Self.T]:
        """The value at `row`, or `None` for NULL."""
        if row < 0 or row >= self._size:
            raise api_error(String("row ", row, " out of range (", self._size, " rows)"))
        var idx = self._index(row)
        if not self._valid_at(idx):
            return None
        comptime if not is_mapped[Self.T]():
            return Optional[Self.T](read_value[Self.T](self._tree.value()[], 1, row))
        else:
            return Optional[Self.T](mapped_read[Self.T](self._meta, self._data, idx))

    def to_list(self) raises DuckDBError -> List[Optional[Self.T]]:
        """All values, NULLs as `None`."""
        var out = List[Optional[Self.T]](capacity=self._size)
        for i in range(self._size):
            out.append(self.get(i))
        return out^
