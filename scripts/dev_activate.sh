#!/bin/bash
# Development activation script: makes mojo import duckdb from source.
# The src/ directory is the duckdb package, but mojo needs it in a directory
# named "duckdb". We create a symlink and point the import path there.

DEV_DIR="$PIXI_PROJECT_ROOT/.dev"
if [ ! -L "$DEV_DIR/duckdb" ]; then
    mkdir -p "$DEV_DIR"
    ln -sfn "$PIXI_PROJECT_ROOT/src" "$DEV_DIR/duckdb"
fi

export MODULAR_MOJO_MAX_IMPORT_PATH="$DEV_DIR,$CONDA_PREFIX/lib/mojo"
