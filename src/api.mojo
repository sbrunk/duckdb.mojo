from ffi import _Global, external_call

comptime _DUCKDB_GLOBAL = _Global["DuckDB", _init_duckdb_global]


# ===--------------------------------------------------------------------===#
# Extension API pointer (set before first DuckDB() access in extension mode)
# ===--------------------------------------------------------------------===#


fn _kgen_insert_global(name: StringSlice, ptr: UnsafePointer):
    external_call["KGEN_CompilerRT_InsertGlobal", NoneType](
        name, ptr.bitcast[NoneType](),
    )


fn _kgen_get_global(name: StringSlice) -> UnsafePointer[NoneType, MutExternalOrigin]:
    return external_call[
        "KGEN_CompilerRT_GetGlobalOrNull",
        UnsafePointer[NoneType, MutExternalOrigin],
    ](name.unsafe_ptr(), name.byte_length())


fn _set_ext_api_ptr(ptr: UnsafePointer[duckdb_ext_api_v1, ImmutExternalOrigin]):
    """Store the stable extension API pointer for later use by
    _init_duckdb_global.

    Must be called BEFORE the first DuckDB() access so that LibDuckDB is
    constructed from the API struct instead of via dlopen/dlsym.
    """
    _kgen_insert_global("DuckDB_ExtApiPtr", ptr)


fn _get_ext_api_ptr() -> UnsafePointer[duckdb_ext_api_v1, ImmutExternalOrigin]:
    """Retrieve the previously stored stable extension API pointer, or null."""
    var raw = _kgen_get_global("DuckDB_ExtApiPtr")
    return UnsafePointer[duckdb_ext_api_v1, ImmutExternalOrigin](
        unsafe_from_address=Int(raw)
    )


fn _set_ext_api_unstable_ptr(
    ptr: UnsafePointer[duckdb_ext_api_v1_unstable, ImmutExternalOrigin],
):
    """Store the unstable extension API pointer for later use by
    _init_duckdb_global.

    Must be called BEFORE the first DuckDB() access so that LibDuckDB is
    constructed from the API struct instead of via dlopen/dlsym.
    """
    _kgen_insert_global("DuckDB_ExtApiUnstablePtr", ptr)


fn _get_ext_api_unstable_ptr() -> UnsafePointer[
    duckdb_ext_api_v1_unstable, ImmutExternalOrigin
]:
    """Retrieve the previously stored unstable extension API pointer, or null."""
    var raw = _kgen_get_global("DuckDB_ExtApiUnstablePtr")
    return UnsafePointer[duckdb_ext_api_v1_unstable, ImmutExternalOrigin](
        unsafe_from_address=Int(raw)
    )


# ===--------------------------------------------------------------------===#
# Global singleton
# ===--------------------------------------------------------------------===#

fn _init_duckdb_global() -> _DuckDBGlobal:
    # Check unstable first (superset of stable)
    var unstable_ptr = _get_ext_api_unstable_ptr()
    if unstable_ptr:
        return _DuckDBGlobal(unstable_ptr)
    var api_ptr = _get_ext_api_ptr()
    if api_ptr:
        return _DuckDBGlobal(api_ptr)
    return _DuckDBGlobal()


struct _DuckDBGlobal(Defaultable, Movable):
    var libduckdb: LibDuckDB

    fn __init__(out self):
        """Standalone mode: load all functions via dlopen/dlsym."""
        self.libduckdb = LibDuckDB()

    fn __init__(
        out self, api: UnsafePointer[duckdb_ext_api_v1, ImmutExternalOrigin]
    ):
        """Extension mode (stable): construct LibDuckDB from the stable API struct."""
        self.libduckdb = LibDuckDB(api)

    fn __init__(
        out self,
        api: UnsafePointer[duckdb_ext_api_v1_unstable, ImmutExternalOrigin],
    ):
        """Extension mode (unstable): construct LibDuckDB from the unstable API struct."""
        self.libduckdb = LibDuckDB(api)


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
