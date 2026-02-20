#!/bin/sh
# Clone DuckDB source at the version matching the installed library.
# Used to provide the JSON schema files needed for code generation.
set -e

DUCKDB_TAG="v$(duckdb --version | head -1 | awk '{print $1}' | sed 's/^v//')"
DIR=".duckdb-src"

if [ -d "$DIR/.git" ]; then
    CURRENT_TAG=$(git -C "$DIR" describe --tags --exact-match 2>/dev/null || echo "")
    if [ "$CURRENT_TAG" = "$DUCKDB_TAG" ]; then
        echo "DuckDB source already at $DUCKDB_TAG in $DIR"
        exit 0
    fi
    echo "Updating DuckDB source from $CURRENT_TAG to $DUCKDB_TAG"
    rm -rf "$DIR"
fi

echo "Cloning DuckDB $DUCKDB_TAG into $DIR (shallow)..."
git clone --depth 1 --branch "$DUCKDB_TAG" https://github.com/duckdb/duckdb.git "$DIR"
