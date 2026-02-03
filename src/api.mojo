from sys.ffi import _Global

comptime _DUCKDB_GLOBAL = _Global["DuckDB", _init_duckdb_global]

fn _init_duckdb_global() -> _DuckDBGlobal:
    return _DuckDBGlobal()


struct _DuckDBGlobal(Defaultable, Movable):
    var libduckdb: LibDuckDB

    fn __init__(out self):
        self.libduckdb = LibDuckDB()

fn _get_duckdb_interface() raises -> Pointer[LibDuckDB, StaticConstantOrigin]:
    """Returns an immutable static pointer to the LibDuckDB global.

    The returned pointer is immutable to prevent invalid shared mutation of
    this global variable. Once it is initialized, it may not be mutated.
    """

    var ptr = _DUCKDB_GLOBAL.get_or_create_ptr()
    var ptr2 = UnsafePointer(to=ptr[].libduckdb).as_immutable().unsafe_origin_cast[StaticConstantOrigin
    ]()
    return Pointer(to=ptr2[])


struct DuckDB(ImplicitlyCopyable):
    var _impl: Pointer[LibDuckDB, StaticConstantOrigin]
    
    fn __init__(out self):
        try:
            self._impl = _get_duckdb_interface()
        except e:
            abort(String("Failed to load libduckdb", e))

    @always_inline
    fn libduckdb(self) -> ref [StaticConstantOrigin] LibDuckDB:
        return self._impl[]

    @staticmethod
    fn connect(db_path: String) raises -> Connection:
        return Connection(db_path)
