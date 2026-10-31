# Demo Mojo Extension for DuckDB

This is a demo DuckDB extension written in [Mojo](https://www.modular.com/mojo) using the [duckdb.mojo](https://github.com/sbrunk/duckdb.mojo) bindings and DuckDB's V2 C extension API.

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

# Append metadata footer (unstable V2 target)
python3 scripts/append_extension_metadata.py \
    extensions/demo-extension/build/demo_mojo.duckdb_extension --abi-type C_STRUCT_UNSTABLE
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

## DuckDB version

The extension uses DuckDB's V2 C API. Every V2 function is still marked
unstable, so the extension carries the `C_STRUCT_UNSTABLE` footer and loads
only into the exact DuckDB build it was compiled against (like DuckDB's C++
extensions). Rebuild it for each DuckDB release.

## Limitations

> [!NOTE]
> The build system differs from the official CMake-based toolchain, so
> extensions cannot yet be published as signed extensions through DuckDB's
> extension distribution mechanism.

## Creating Your Own Extension

To create your own Mojo extension for DuckDB:

1. Copy this directory as a starting point
2. Write your functions as plain Mojo functions (`ScalarFunction.from_function`), SIMD functions (`from_simd_function`) or chunk-level functions (`from_chunk_function`)
3. Create an init function that registers them on the `Extension`
4. Export the entry point `{name}_init_c_api_v2` with the `abi("C")` effect
5. Build and load it using the pixi tasks or manual steps above

### Entry point convention

The entry point must be named `{extension_name}_init_c_api_v2`, where
`{extension_name}` is the file name stem of the `.duckdb_extension` file (for
example, `demo_mojo.duckdb_extension` needs `demo_mojo_init_c_api_v2`):

```mojo
from duckdb._libduckdb_v2 import duckdb_v2_extension_input
from duckdb import Extension, ScalarFunction

def add_numbers(a: Int64, b: Int64) -> Int64:
    return a + b

def init(ext: Extension) raises:
    ext.register(ScalarFunction.from_function["mojo_add_numbers", add_numbers]())

@export("my_extension_init_c_api_v2")
def my_extension_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    Extension.run[init](input)
```

`Extension.run` loads the V2 API and runs `init`. If `init` raises, the load
fails with its message. An extension has no `Connection`: it cannot run SQL.

The signature comes from the Mojo function: arguments and results can be
numbers, strings, dates, lists, maps and structs. A NULL argument gives NULL
without calling the function, unless the argument is an `Optional`.
