# Demo Mojo Extension for DuckDB

This is a demo DuckDB extension written in [Mojo](https://www.modular.com/mojo) using the [duckdb.mojo](https://github.com/sbrunk/duckdb.mojo) bindings and the [DuckDB C Extension API](https://duckdb.org/docs/extensions/extending_duckdb/c_extensions).

It demonstrates how to create DuckDB extensions in Mojo, following a similar pattern to [DuckDB's demo_capi extension](https://github.com/duckdb/duckdb/tree/main/extension/demo_capi) and the [extension-template](https://github.com/duckdb/extension-template).

## What's Inside

The extension registers a scalar function `mojo_add_numbers(a BIGINT, b BIGINT) -> BIGINT` that adds two integers together.

## Directory Structure

```
demo-extension/
├── README.md                         # This file
├── src/
│   └── demo_mojo_extension.mojo      # Extension source code
├── test/
│   └── sql/
│       └── demo_mojo.test            # SQL logic test
└── build/                            # Build output (gitignored)
```

## Building

### Prerequisites

- [pixi](https://pixi.sh) (manages Mojo compiler, DuckDB, and duckdb.mojo dependencies)

### Build with pixi

From the workspace root:

```sh
pixi run build-demo-extension
```

This will:
1. Build the duckdb.mojo package (if needed)
2. Compile the extension as a shared library
3. Append the required DuckDB extension metadata footer

### Build manually

```sh
# Build the duckdb.mojo package first
pixi run build

# Compile the shared library
mojo build demo-extension/src/demo_mojo_extension.mojo \
    --emit shared-lib \
    -o demo-extension/build/demo_mojo.duckdb_extension

# Append metadata footer
python3 scripts/append_extension_metadata.py \
    demo-extension/build/demo_mojo.duckdb_extension
```

## Testing

### With pixi

```sh
pixi run test-demo-extension
```

### Manually

```sh
pixi run duckdb -unsigned -c "
    LOAD 'demo-extension/build/demo_mojo.duckdb_extension';
    SELECT mojo_add_numbers(40, 2);
"
```

Expected output: `42`

## Limitations

> [!NOTE]
> Mojo extensions currently resolve DuckDB functions via dynamic symbol lookup
> rather than through the `duckdb_ext_api_v1` struct, meaning unstable/internal
> APIs are accessible even when targeting the stable `C_STRUCT` ABI. In practice,
> this ties extensions to a specific DuckDB version. Additionally, the build
> system differs from the official CMake-based toolchain, so extensions cannot
> yet be published as signed extensions through DuckDB's extension distribution
> mechanism.

## Creating Your Own Extension

To create your own Mojo extension for DuckDB:

1. **Copy this directory** as a starting point
2. **Write your functions** using the duckdb.mojo API (`ScalarFunction`, `AggregateFunction`, `TableFunction`)
3. **Create an init function** that registers them via a `Connection`
4. **Export the entry point** using `@export("{name}_init_c_api", ABI="C")`
5. **Build and load** using the pixi tasks or manual steps above

### Entry Point Convention

The entry point function must be named `{extension_name}_init_c_api` and have this signature:

```mojo
@export("my_extension_init_c_api", ABI="C")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    ...
```

The `{extension_name}` part must match the filename stem of the `.duckdb_extension` file (e.g., `demo_mojo.duckdb_extension` → `demo_mojo_init_c_api`).

### Using `Extension.run` (recommended)

The simplest way to implement the entry point. Write an init function that
receives a `Connection` and registers your functions, then pass it to
`Extension.run`:

```mojo
from duckdb._libduckdb import duckdb_extension_info
from duckdb.extension import duckdb_extension_access, Extension
from duckdb.connection import Connection
from duckdb.scalar_function import ScalarFunction

fn add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

fn init(conn: Connection) raises:
    ScalarFunction.from_function[
        "mojo_add_numbers", DType.int64, DType.int64, DType.int64, add_numbers
    ](conn)

@export("my_extension_init_c_api", ABI="C")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    return Extension.run[init](info, access)
```

`Extension.run` handles creating the connection and reporting errors back to
DuckDB automatically. If `init` raises, the error message is forwarded to
DuckDB via `set_error`.

### Using `Extension` directly

For more control (e.g. to access the `Database` handle or report custom errors),
create an `Extension` manually:

```mojo
@export("my_extension_init_c_api", ABI="C")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutExternalOrigin],
) -> Bool:
    var ext = Extension(info, access)
    try:
        var conn = ext.connect()
        # Register functions via conn ...
    except e:
        ext.set_error(String(e))
        return False
    return True
```
