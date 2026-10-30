"""DuckDB's file system: the one queries read and write through, so paths
work as they do in SQL (`~`, and remote or virtual file systems that
extensions such as httpfs register).

```mojo
var fs = con.file_system()
var f = fs.open("data/out.txt", writable=True, create=True)
f.write("hello".as_bytes())
f.close()
for path in fs.glob("data/*.txt"):
    print(path, fs.exists(path))
```
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import StrArg, null, null_error_info, str_to_string
from duckdb.v2._runtime import check, lib
from duckdb.v2.error import DuckDBError


def _collect_text(
    text: Pointer[duckdb_v2_str, ImmUntrackedOrigin],
    user_data: Pointer[NoneType, MutUntrackedOrigin],
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    """Append each text DuckDB hands over to the `List[String]` at `user_data`."""
    user_data.unsafe_bitcast[List[String]]()[].append(str_to_string(text[]))


struct FileSystem[origin: ImmOrigin](ImplicitlyCopyable):
    """DuckDB's file system, borrowed from a connection or a callback's context."""

    var _handle: duckdb_v2_file_system_handle
    var _owner: Pointer[NoneType, Self.origin]

    def __init__(out self, handle: duckdb_v2_file_system_handle, owner: Pointer[NoneType, Self.origin]):
        self._handle = handle
        self._owner = owner

    def exists(self, path: StringSlice) raises DuckDBError -> Bool:
        """Whether a file or directory exists at `path`."""
        var p = StrArg(path)
        var out = False
        var err = null_error_info()
        check(lib().duckdb_v2_file_system_path_exists(self._handle, p.ptr(), Pointer(to=out), Pointer(to=err)), err)
        return out

    def is_remote(self, path: StringSlice) raises DuckDBError -> Bool:
        """Whether `path` is on a remote file system (`s3://`, `https://`, ...)."""
        var p = StrArg(path)
        var out = False
        var err = null_error_info()
        check(lib().duckdb_v2_file_system_path_is_remote(self._handle, p.ptr(), Pointer(to=out), Pointer(to=err)), err)
        return out

    def expand(self, path: StringSlice) raises DuckDBError -> String:
        """`path` as opening it would see it (a leading `~` expanded, ...)."""
        var p = StrArg(path)
        var out = List[String]()
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_system_path_expand(
                self._handle, p.ptr(), _collect_text, Pointer(to=out).unsafe_bitcast[NoneType](), Pointer(to=err)
            ),
            err,
        )
        return String().join(out)

    def glob(self, pattern: StringSlice) raises DuckDBError -> List[String]:
        """The paths matching `pattern` (`*`, `?`, `**`, as in SQL `glob()`)."""
        var p = StrArg(pattern)
        var out = List[String]()
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_system_path_glob(
                self._handle, p.ptr(), _collect_text, Pointer(to=out).unsafe_bitcast[NoneType](), Pointer(to=err)
            ),
            err,
        )
        return out^

    def move(self, source: StringSlice, target: StringSlice) raises DuckDBError:
        var s = StrArg(source)
        var t = StrArg(target)
        var err = null_error_info()
        check(lib().duckdb_v2_file_system_path_move(self._handle, s.ptr(), t.ptr(), Pointer(to=err)), err)

    def remove(self, path: StringSlice) raises DuckDBError -> Bool:
        """Delete the file at `path`; False if there was none."""
        var p = StrArg(path)
        var removed = False
        var err = null_error_info()
        check(lib().duckdb_v2_file_system_path_remove(self._handle, p.ptr(), Pointer(to=removed), Pointer(to=err)), err)
        return removed

    def open(
        self,
        path: StringSlice,
        *,
        readable: Bool = True,
        writable: Bool = False,
        create: Bool = False,
        create_new: Bool = False,
        append: Bool = False,
    ) raises DuckDBError -> File:
        """Open a file. `create` creates it when missing, `create_new` also
        truncates an existing one; `append` writes at the end."""
        var err = null_error_info()
        var options = null[duckdb_v2_file_open_options_handle]()
        check(lib().duckdb_v2_file_open_options_create(self._handle, Pointer(to=options), Pointer(to=err)), err)
        try:
            var flags = List[DUCKDB_V2_FILE_FLAG]()
            if readable:
                flags.append(DUCKDB_V2_FILE_FLAG_READ)
            if writable or create or create_new or append:
                flags.append(DUCKDB_V2_FILE_FLAG_WRITE)
            if create:
                flags.append(DUCKDB_V2_FILE_FLAG_CREATE)
            if create_new:
                flags.append(DUCKDB_V2_FILE_FLAG_CREATE_NEW)
            if append:
                flags.append(DUCKDB_V2_FILE_FLAG_APPEND)
            for f in flags:
                check(lib().duckdb_v2_file_open_options_set_flag(options, f, Pointer(to=err)), err)
            var p = StrArg(path)
            var file = null[duckdb_v2_file_handle]()
            check(lib().duckdb_v2_file_system_open(self._handle, p.ptr(), options, Pointer(to=file), Pointer(to=err)), err)
            return File(_owned=file)
        finally:
            _ = lib().duckdb_v2_file_open_options_destroy(Pointer(to=options))

    def read_all(self, path: StringSlice) raises DuckDBError -> List[UInt8]:
        """The whole file."""
        var f = self.open(path)
        var out = f.read(f.size())
        f.close()
        return out^

    def write_all(self, path: StringSlice, data: Span[UInt8, _]) raises DuckDBError:
        """Replace the file with `data` (created if missing)."""
        var f = self.open(path, readable=False, create_new=True)
        f.write(data)
        f.close()


struct File(Movable):
    """An open file (owned; closed when destroyed)."""

    var _handle: duckdb_v2_file_handle

    def __init__(out self, *, var _owned: duckdb_v2_file_handle):
        self._handle = _owned

    def __init__(out self, *, deinit move: Self):
        self._handle = move._handle

    def __deinit__(deinit self):
        _ = lib().duckdb_v2_file_destroy(Pointer(to=self._handle))

    def read(mut self, n: Int) raises DuckDBError -> List[UInt8]:
        """Up to `n` bytes from the current position (fewer at the end of the file)."""
        var buf = List[UInt8](length=n, fill=0)
        var got: idx_t = 0
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_read(self._handle, buf.unsafe_ptr().unsafe_bitcast[NoneType](), idx_t(n), Pointer(to=got), Pointer(to=err)),
            err,
        )
        buf.resize(Int(got), 0)
        return buf^

    def read_at(self, n: Int, offset: Int) raises DuckDBError -> List[UInt8]:
        """Exactly `n` bytes at `offset` (does not move the position)."""
        var buf = List[UInt8](length=n, fill=0)
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_read_at(self._handle, buf.unsafe_ptr().unsafe_bitcast[NoneType](), idx_t(n), idx_t(offset), Pointer(to=err)),
            err,
        )
        return buf^

    def write(mut self, data: Span[UInt8, _]) raises DuckDBError -> Int:
        """Write at the current position; returns the bytes written."""
        var written: idx_t = 0
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_write(
                self._handle,
                data.unsafe_ptr().unsafe_bitcast[NoneType]().unsafe_mut_cast[True]().unsafe_origin_cast[MutUntrackedOrigin](),
                idx_t(len(data)),
                Pointer(to=written),
                Pointer(to=err),
            ),
            err,
        )
        return Int(written)

    def write_at(self, data: Span[UInt8, _], offset: Int) raises DuckDBError:
        var err = null_error_info()
        check(
            lib().duckdb_v2_file_write_at(
                self._handle,
                data.unsafe_ptr().unsafe_bitcast[NoneType]().unsafe_mut_cast[True]().unsafe_origin_cast[MutUntrackedOrigin](),
                idx_t(len(data)),
                idx_t(offset),
                Pointer(to=err),
            ),
            err,
        )

    def size(self) raises DuckDBError -> Int:
        var n: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_file_size(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return Int(n)

    def tell(self) raises DuckDBError -> Int:
        var n: idx_t = 0
        var err = null_error_info()
        check(lib().duckdb_v2_file_tell(self._handle, Pointer(to=n), Pointer(to=err)), err)
        return Int(n)

    def seek(mut self, position: Int) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_file_seek(self._handle, idx_t(position), Pointer(to=err)), err)

    def sync(self) raises DuckDBError:
        var err = null_error_info()
        check(lib().duckdb_v2_file_sync(self._handle, Pointer(to=err)), err)

    def close(mut self) raises DuckDBError:
        """Close the file, raising write errors (destruction closes silently)."""
        var err = null_error_info()
        check(lib().duckdb_v2_file_close(self._handle, Pointer(to=err)), err)


def context_file_system(context: duckdb_v2_context_handle) raises DuckDBError -> FileSystem[ImmUntrackedOrigin]:
    var fs = null[duckdb_v2_file_system_handle]()
    var err = null_error_info()
    check(lib().duckdb_v2_context_get_file_system(context, Pointer(to=fs), Pointer(to=err)), err)
    return FileSystem[ImmUntrackedOrigin](fs, context.unsafe_bitcast[NoneType]().as_imm().unsafe_origin_cast[ImmUntrackedOrigin]())
