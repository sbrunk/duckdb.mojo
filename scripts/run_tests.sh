#!/bin/bash
# Build the mojopkg once, then compile each test against it instead of
# recompiling from source. This drastically reduces peak memory on CI
# where the Mojo compiler's monomorphization can exceed 7GB RAM.
set -e

WORKDIR=$(mktemp -d)
trap "rm -rf $WORKDIR" EXIT

# Build the package once from source
mojo package src -o "$WORKDIR/duckdb.mojopkg"

# Copy test files to a clean directory (no src/ to shadow the mojopkg)
cp -r test "$WORKDIR/test"

# Compile and run each test file using the pre-built package
for f in "$WORKDIR"/test/test_*.mojo; do
    bin=$(mktemp)
    mojo build "$f" -I "$WORKDIR" -o "$bin"
    "$bin"
    s=$?
    rm -f "$bin"
    if [ $s -ne 0 ]; then
        exit 1
    fi
done
