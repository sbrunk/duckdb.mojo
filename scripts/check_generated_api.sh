#!/bin/bash
# Verify that duckdb/_libduckdb.mojo is in sync with the DuckDB version.
#
# The bindings are generated from the DuckDB source (see scripts/generate_mojo_api.py).
# This guards against forgetting to regenerate them after a DuckDB version bump.
#
# Run via `pixi run check-generated-api`, which regenerates the bindings (against
# the version matching the installed `duckdb`) as a dependency before this script
# runs. Here we only check whether that regeneration changed the committed file.
set -e

if git diff --quiet -- duckdb/_libduckdb.mojo; then
    echo "duckdb/_libduckdb.mojo is up to date with the DuckDB source."
    exit 0
fi

echo "ERROR: duckdb/_libduckdb.mojo is out of date with the DuckDB source."
echo "Regenerate and commit it:"
echo
echo "    pixi run generate-api"
echo
echo "Diff (committed vs. freshly generated):"
git --no-pager diff -- duckdb/_libduckdb.mojo
exit 1
