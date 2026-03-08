#!/bin/bash
# Build the mojopkg once, then compile each test against it instead of
# recompiling from source. This drastically reduces peak memory on CI
# where the Mojo compiler's monomorphization can exceed 7GB RAM.
set -e

PROJDIR="$(pwd)"
WORKDIR=$(mktemp -d)
trap "rm -rf $WORKDIR" EXIT

# Build the package once from source
mojo package src -o "$WORKDIR/duckdb.mojopkg"

# Copy test files to the work directory
cp -r test "$WORKDIR/test"

# Get the absolute path to mojo so we can call it from the work directory
MOJO="$(which mojo)"

# Run from the work directory so mojo doesn't find src/ and recompile
cd "$WORKDIR"

for f in test/test_*.mojo; do
    bin=$(mktemp)
    "$MOJO" build "$f" -I "$WORKDIR" -o "$bin"
    "$bin"
    s=$?
    rm -f "$bin"
    if [ $s -ne 0 ]; then
        exit 1
    fi
done
