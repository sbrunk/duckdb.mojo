# DuckDB From Source Package

This package builds DuckDB from source and provides the full headers and library needed for building extensions that require internal DuckDB APIs (like the operator-replacement package).

## Purpose

The `duckdb-operator-replacement` package requires access to internal DuckDB headers that are not available in the precompiled `libduckdb-devel` package from conda-forge. This package:

1. Clones DuckDB v1.4.4 from GitHub
2. Builds it from source with CMake/Ninja
3. Installs the library and **all** headers to `$PREFIX`
4. Gets cached by pixi-build

## Caching

Once built, this package is cached by pixi. Subsequent builds of `duckdb-operator-replacement` will reuse the cached version instead of rebuilding DuckDB, making the `-e full` environment much faster on repeated uses.

## Version

- **DuckDB Version**: 1.4.4
- **Build Type**: Release
- **Build System**: CMake + Ninja

## Usage

This package is automatically built and consumed by the `duckdb-operator-replacement` package. You typically don't need to build it directly, but if needed:

```bash
cd packages/duckdb-from-source
pixi build
```
