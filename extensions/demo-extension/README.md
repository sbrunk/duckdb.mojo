# Demo Mojo Extension for DuckDB

This is a demo DuckDB extension written in [Mojo](https://www.modular.com/mojo) using the [duckdb.mojo](https://github.com/sbrunk/duckdb.mojo) bindings and the [DuckDB C Extension API](https://duckdb.org/docs/extensions/extending_duckdb/c_extensions).

It demonstrates how to create DuckDB extensions in Mojo, following a similar pattern to [DuckDB's demo_capi extension](https://github.com/duckdb/duckdb/tree/main/extension/demo_capi) and the [extension-template](https://github.com/duckdb/extension-template).

## What's Inside

The extension registers a scalar function `mojo_add_numbers(a BIGINT, b BIGINT) -> BIGINT` that adds two integers together.

## Directory Structure

```
extensions/demo-extension/
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
mojo build extensions/demo-extension/src/demo_mojo_extension.mojo \
    --emit shared-lib \
    -o extensions/demo-extension/build/demo_mojo.duckdb_extension

# Append metadata footer
python3 scripts/append_extension_metadata.py \
    extensions/demo-extension/build/demo_mojo.duckdb_extension
```

## Testing

### With pixi

```sh
pixi run test-demo-extension
```

### Manually

```sh
pixi run duckdb -unsigned -c "
    LOAD 'extensions/demo-extension/build/demo_mojo.duckdb_extension';
    SELECT mojo_add_numbers(40, 2);
"
```

Expected output: `42`

## API version

`Extension.run` requests extension API version v1.5.6. DuckDB 1.5.6
stabilized every function that used to be unstable, so extensions get the full
C API, and they load in DuckDB 1.5.6 or newer.

## Limitations

> [!NOTE]
> The build system differs from the official CMake-based toolchain, so
> extensions cannot yet be published as signed extensions through DuckDB's
> extension distribution mechanism.

## Creating Your Own Extension

To create your own Mojo extension for DuckDB:

1. Copy this directory as a starting point
2. Write your functions using the duckdb.mojo API (`ScalarFunction`, `AggregateFunction`, `TableFunction`)
3. Create an init function that registers them via a `Connection`
4. Export the entry point using `@export("{name}_init_c_api")` with the `abi("C")` effect
5. Build and load it using the pixi tasks or manual steps above

### Entry Point Convention

The entry point function must be named `{extension_name}_init_c_api` and have this signature:

```mojo
@export("my_extension_init_c_api")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutUntrackedOrigin],
) abi("C") -> Bool:
    ...
```

The `{extension_name}` part must match the filename stem of the `.duckdb_extension` file (for example, `demo_mojo.duckdb_extension` needs `demo_mojo_init_c_api`).

### Using `Extension.run` (recommended)

This is the easiest way to implement the entry point. Write an init function that
receives a `Connection` and registers your functions, then pass it to `Extension.run`:

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

@export("my_extension_init_c_api")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutUntrackedOrigin],
) abi("C") -> Bool:
    return Extension.run[init](info, access)
```

`Extension.run` creates the connection and reports errors back to DuckDB. If `init` raises, the error message is forwarded to
DuckDB via `set_error`.

The `Connection` has the full C API. See the [API version](#api-version)
section above for details.

### Using `Extension` directly

For more control (for example to access the `Database` handle or report custom errors),
create an `Extension` manually:

```mojo
@export("my_extension_init_c_api")
fn my_extension_init_c_api(
    info: duckdb_extension_info,
    access: UnsafePointer[duckdb_extension_access, MutUntrackedOrigin],
) abi("C") -> Bool:
    var ext = Extension(info, access)
    try:
        var conn = ext.connect()
        # Register functions via conn ...
    except e:
        ext.set_error(String(e))
        return False
    return True
```
