# duckdb-mojo-helpers

Pointer-based shim for DuckDB C API calls (e.g. duckdb_fetch_chunk) that normally take duckdb_result by value. Mojo cannot yet safely pass large structs by value across FFI, so this library exposes pointer-friendly wrappers.
