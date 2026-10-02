# CPP-ABI extensions

The [C extension API](../README.md#writing-extensions) is enough for
scalar/aggregate/table UDFs, but it cannot reach DuckDB internals such as
mutating catalog entries, adding an `OptimizerExtension`, or registering a
custom logical operator. For those you need DuckDB's CPP ABI.
This is how the [mojo-kernel-overrides](../extensions/mojo-kernel-overrides/README.md)
and [mojo-gpu-operator](../extensions/mojo-gpu-operator/README.md) extensions are built.

In this repo, a CPP-ABI extension is a C++ extension that calls Mojo-compiled
kernels over a C ABI. No DuckDB C++ type crosses into Mojo:

- Mojo exports kernels over raw pointers with `@export(...) ... abi("C")`.
- C++ declares those symbols in an `extern "C"` block and calls them, and does
  all the DuckDB-internal work (catalog, optimizer, operators) against the internal
  C++ headers.

The C++ side provides the entry points DuckDB's loader looks up by extension name:

```cpp
#include "duckdb.hpp"
#include "duckdb/main/extension/extension_loader.hpp"

extern "C" {                                  // Mojo kernels, linked into this .so
void myext_scale_f64(const double *in, double *out, int64_t n, double k);
}

namespace duckdb {
void RegisterMyExt(DatabaseInstance &db) {
    // internal C++ API: mutate the catalog / add an OptimizerExtension / register
    // a TableFunction, calling myext_scale_f64(...) on raw FLAT column buffers.
}
} // namespace duckdb

extern "C" {
// LOAD entry point: DuckDB calls <extension_name>_duckdb_cpp_init.
__attribute__((visibility("default")))
void myext_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
    duckdb::RegisterMyExt(loader.GetDatabaseInstance());
}
__attribute__((visibility("default")))
const char *myext_version() { return duckdb::DuckDB::LibraryVersion(); }

// Optional: let an embedder that already holds a connection install it directly
// (no LOAD, so no footer/version check; the caller must match the DuckDB version).
__attribute__((visibility("default")))
void register_myext(duckdb_connection connection) {
    auto con = reinterpret_cast<duckdb::Connection *>(connection);
    duckdb::RegisterMyExt(*con->context->db);
}
}
```

Build it in two steps: compile the Mojo kernels, then link them into a C++ shared
object and append the `CPP` metadata footer.

```sh
# 1. Mojo kernels. --emit object gives a plain .o with no Mojo runtime deps (CPU/SIMD
#    only), so the final .so is self-contained (links only libm). If the kernels need
#    the Mojo GPU/AsyncRT runtime, use --emit shared-lib instead and link + rpath the
#    resulting companion dylib (see extensions/mojo-gpu-operator/build.sh).
mojo build --emit object kernels.mojo -o kernels.o

# 2. C++ extension. DuckDB symbols are left unresolved and bound at load time against
#    the host libduckdb (-undefined dynamic_lookup on macOS; -Wl,--allow-shlib-undefined
#    on Linux). Internal headers come from conda libduckdb-devel.
clang++ -std=c++17 -O2 -fPIC -shared -undefined dynamic_lookup \
    myext.cpp kernels.o -I "$CONDA_PREFIX/include" -lm \
    -o myext.duckdb_extension

# 3. Append the footer (run from the repo root). CPP is version-locked, so the version field is the DuckDB
#    version (not the C API version).
python3 scripts/append_extension_metadata.py myext.duckdb_extension \
    --abi-type CPP --duckdb-version v1.5.6
```

Caveats specific to the CPP ABI:

- Version-locked: the footer carries the exact DuckDB version and `LOAD` rejects
  any mismatch. Rebuild for each DuckDB version. The stable C API, in contrast, is
  forward-compatible.
- It needs the internal C++ headers and an ABI-matched libduckdb from conda
  `libduckdb-devel`, not the stable C extension API.
- It is unsigned, so load it with `-unsigned` / `allow_unsigned_extensions`.
- A host that statically links DuckDB and `dlopen`s a CPP extension must link with
  `-rdynamic` so DuckDB's symbols resolve in the loaded extension.
