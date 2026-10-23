"""Smoke tests for the generated V2 C API bindings (duckdb/_libduckdb_v2.mojo).

These call the raw functions directly, to check that the generated types and
calling conventions match libduckdb: handles, out-parameters, error info,
string views, the `bytes` union and vector views.
"""

from duckdb._libduckdb_v2 import *
from duckdb._libduckdb_v2 import _duckdb_v2_error_info
from duckdb.database import _null_ptr
from duckdb.api_v2 import DuckDBV2, StrArg, check, null, null_error_info, str_to_string
from std.sys.info import size_of
from std.testing import assert_equal, assert_true
from std.testing.suite import TestSuite


def _str_to_string(s: duckdb_v2_str) raises -> String:
    var bytes = Span[UInt8, ImmUntrackedOrigin](
        unsafe_ptr=s.ptr.unsafe_bitcast[UInt8](), length=Int(s.len)
    )
    return String(from_utf8=bytes)


struct _Session:
    """An environment, an instance and a connection, destroyed in reverse order."""

    var lib: LibDuckDBV2
    var env: duckdb_v2_environment_handle
    var instance: duckdb_v2_instance_handle
    var conn: duckdb_v2_connection_handle

    def __init__(out self) raises:
        self.lib = LibDuckDBV2()
        self.env = duckdb_v2_environment_handle.unsafe_dangling()
        self.instance = duckdb_v2_instance_handle.unsafe_dangling()
        self.conn = duckdb_v2_connection_handle.unsafe_dangling()
        var err = _null_ptr[_duckdb_v2_error_info, MutUntrackedOrigin]()
        assert_equal(self.lib.duckdb_v2_environment_create(Pointer(to=self.env), Pointer(to=err)), DUCKDB_V2_ERROR_NONE)
        assert_equal(
            self.lib.duckdb_v2_instance_create(self.env, Pointer(to=self.instance), Pointer(to=err)),
            DUCKDB_V2_ERROR_NONE,
        )
        assert_equal(
            self.lib.duckdb_v2_connection_create(self.instance, Pointer(to=self.conn), Pointer(to=err)),
            DUCKDB_V2_ERROR_NONE,
        )

    def __deinit__(deinit self):
        _ = self.lib.duckdb_v2_connection_destroy(Pointer(to=self.conn))
        _ = self.lib.duckdb_v2_instance_destroy(Pointer(to=self.instance))
        _ = self.lib.duckdb_v2_environment_destroy(Pointer(to=self.env))

    def execute(
        mut self, sql: String, mut err: duckdb_v2_error_info_handle
    ) -> Tuple[DUCKDB_V2_ERROR, duckdb_v2_result_handle]:
        """Parse and run the first statement of ``sql``."""
        var result = duckdb_v2_result_handle.unsafe_dangling()
        var iterator = duckdb_v2_statement_iterator_handle.unsafe_dangling()
        var sql_copy = sql.copy()
        var code = self.lib.duckdb_v2_parse_sql(
            self.conn, sql_copy.as_c_string_span().ptr(), Pointer(to=iterator), Pointer(to=err)
        )
        _ = sql_copy^  # keep the text alive until parse_sql has returned
        if code != DUCKDB_V2_ERROR_NONE:
            return (code, result)
        var statement = duckdb_v2_sql_statement_handle.unsafe_dangling()
        code = self.lib.duckdb_v2_statement_iterator_next(iterator, Pointer(to=statement), Pointer(to=err))
        if code == DUCKDB_V2_ERROR_NONE:
            code = self.lib.duckdb_v2_statement_execute(
                self.conn,
                statement,
                _null_ptr[duckdb_v2_identifier_t, ImmUntrackedOrigin](),
                _null_ptr[duckdb_v2_value_handle, ImmUntrackedOrigin](),
                0,
                Pointer(to=result),
                Pointer(to=err),
            )
            _ = self.lib.duckdb_v2_sql_statement_destroy(Pointer(to=statement))
        _ = self.lib.duckdb_v2_statement_iterator_destroy(Pointer(to=iterator))
        return (code, result)


def test_struct_sizes() raises:
    """The structs DuckDB reads or writes in place must match its C layout."""
    assert_equal(size_of[duckdb_v2_str](), 16)
    assert_equal(size_of[duckdb_v2_bytes](), 16)
    assert_equal(size_of[duckdb_v2_opaque](), 24)
    assert_equal(size_of[duckdb_v2_vector_view](), 32)
    assert_equal(size_of[duckdb_v2_list_entry](), 16)
    assert_equal(size_of[duckdb_v2_hugeint_t](), 16)
    assert_equal(size_of[duckdb_v2_uhugeint_t](), 16)
    assert_equal(size_of[duckdb_v2_interval_t](), 16)
    assert_equal(size_of[duckdb_v2_extension_input](), 32)
    assert_equal(size_of[ArrowSchema](), 72)
    assert_equal(size_of[ArrowArray](), 80)
    assert_equal(size_of[ArrowArrayStream](), 40)


def test_library_version() raises:
    var lib = LibDuckDBV2()
    var version = duckdb_v2_str(_null_ptr[c_char, ImmUntrackedOrigin](), 0)
    var err = _null_ptr[_duckdb_v2_error_info, MutUntrackedOrigin]()
    assert_equal(lib.duckdb_v2_library_version(Pointer(to=version), Pointer(to=err)), DUCKDB_V2_ERROR_NONE)
    assert_equal(_str_to_string(version), DUCKDB_V2_SOURCE_VERSION)


def test_query_and_read_vectors() raises:
    var s = _Session()
    var err = _null_ptr[_duckdb_v2_error_info, MutUntrackedOrigin]()
    var executed = s.execute(
        "SELECT 42::INTEGER AS i, 'a string longer than twelve bytes' AS long_s, 'short' AS short_s", err
    )
    assert_equal(executed[0], DUCKDB_V2_ERROR_NONE)
    var result = executed[1]

    var chunk = duckdb_v2_data_chunk_handle.unsafe_dangling()
    assert_equal(s.lib.duckdb_v2_result_fetch_chunk(result, Pointer(to=chunk), Pointer(to=err)), DUCKDB_V2_ERROR_NONE)
    var size: idx_t = 0
    assert_equal(s.lib.duckdb_v2_data_chunk_get_size(chunk, Pointer(to=size), Pointer(to=err)), DUCKDB_V2_ERROR_NONE)
    assert_equal(size, 1)

    var views = List[duckdb_v2_vector_view]()
    for col in range(3):
        var vector = duckdb_v2_vector_handle.unsafe_dangling()
        assert_equal(
            s.lib.duckdb_v2_data_chunk_get_vector(chunk, idx_t(col), Pointer(to=vector), Pointer(to=err)),
            DUCKDB_V2_ERROR_NONE,
        )
        var view = duckdb_v2_vector_view(
            _null_ptr[NoneType, ImmUntrackedOrigin](),
            _null_ptr[UInt64, ImmUntrackedOrigin](),
            _null_ptr[duckdb_v2_sel_t, ImmUntrackedOrigin](),
            0,
        )
        assert_equal(s.lib.duckdb_v2_vector_get_view(vector, Pointer(to=view), Pointer(to=err)), DUCKDB_V2_ERROR_NONE)
        assert_equal(view.count, 1)
        views.append(view^)

    assert_equal(views[0].data.unsafe_bitcast[Int32]()[], 42)

    # VARCHAR values are `bytes`: inline up to 12 bytes, otherwise a pointer.
    # The union members share their address, so read them through a cast.
    ref long_s = views[1].data.unsafe_bitcast[duckdb_v2_bytes_pointer]()[]
    var long_len = Int(long_s.length)
    assert_true(long_len > DUCKDB_V2_BYTES_INLINE_LENGTH)
    var long_str = String(
        from_utf8=Span[UInt8, MutUntrackedOrigin](unsafe_ptr=long_s.ptr.unsafe_bitcast[UInt8](), length=long_len)
    )
    assert_equal(long_str, "a string longer than twelve bytes")

    var short_s = views[2].data.unsafe_bitcast[duckdb_v2_bytes_inlined]()
    assert_equal(Int(short_s[].length), 5)
    var short_str = String(
        from_utf8=Span[UInt8, ImmUntrackedOrigin](
            unsafe_ptr=short_s.unsafe_bitcast[UInt8]().unsafe_offset(4), length=5
        )
    )
    assert_equal(short_str, "short")

    _ = s.lib.duckdb_v2_data_chunk_destroy(Pointer(to=chunk))
    _ = s.lib.duckdb_v2_result_destroy(Pointer(to=result))


def test_error_info() raises:
    var s = _Session()
    var err = _null_ptr[_duckdb_v2_error_info, MutUntrackedOrigin]()
    var executed = s.execute("SELECT * FROM table_that_does_not_exist", err)
    assert_true(executed[0] != DUCKDB_V2_ERROR_NONE)
    var text = duckdb_v2_str(_null_ptr[c_char, ImmUntrackedOrigin](), 0)
    assert_equal(s.lib.duckdb_v2_error_info_get_text(err, Pointer(to=text)), DUCKDB_V2_ERROR_NONE)
    assert_true("table_that_does_not_exist" in _str_to_string(text))
    _ = s.lib.duckdb_v2_error_info_destroy(Pointer(to=err))


def test_api_v2_helpers() raises:
    """The `DuckDBV2` global loads in client mode, and `check` turns errors into `Error`s."""
    ref lib = DuckDBV2().libduckdb()
    var env = null[duckdb_v2_environment_handle]()
    var err = null_error_info()
    check(lib.duckdb_v2_environment_create(Pointer(to=env), Pointer(to=err)), err)

    # Creating an instance with an invalid option fails with a message.
    var instance = null[duckdb_v2_instance_handle]()
    var name = StrArg("no_such_option")
    var value = StrArg("1")
    var raised = False
    try:
        check(
            lib.duckdb_v2_instance_create_with_options(
                env, name.ptr(), value.ptr(), 1, Pointer(to=instance), Pointer(to=err)
            ),
            err,
        )
    except e:
        raised = True
        assert_true("no_such_option" in String(e), String(e))
    assert_true(raised)
    assert_equal(Int(err), 0)  # check() destroyed the error info and reset the slot
    _ = lib.duckdb_v2_environment_destroy(Pointer(to=env))
    var abc = StrArg("abc")
    assert_equal(str_to_string(abc.ptr()[]), "abc")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
