#!/bin/bash
# Verify that the generated bindings (duckdb/_libduckdb.mojo for the V1 C API,
# duckdb/_libduckdb_v2.mojo for the V2 C API) are in sync with the DuckDB version.
#
# The bindings are generated from the DuckDB source (see scripts/generate_mojo_api.py
# and scripts/generate_mojo_api_v2.py). This guards against forgetting to regenerate
# them after a DuckDB version bump.
#
# Run via `pixi run check-generated-api`, which regenerates the bindings (against
# the version matching the installed `duckdb`) as a dependency before this script
# runs. Here we only check whether that regeneration changed the committed files.
set -e

FILES="duckdb/_libduckdb.mojo duckdb/_libduckdb_v2.mojo"

if git diff --quiet -- $FILES && [ -z "$(git ls-files --others --exclude-standard -- $FILES)" ]; then
    echo "The generated bindings are up to date with the DuckDB source."
    exit 0
fi

echo "ERROR: the generated bindings are out of date with the DuckDB source."
echo "Regenerate and commit them:"
echo
echo "    pixi run generate-api"
echo
echo "Diff (committed vs. freshly generated):"
git --no-pager diff -- $FILES
git ls-files --others --exclude-standard -- $FILES
exit 1
