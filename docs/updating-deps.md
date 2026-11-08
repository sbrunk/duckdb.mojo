# Updating Mojo and DuckDB

The Mojo and DuckDB versions are pinned in several files. When you bump
either, update every file the checklist names; CI fails if they disagree.

## Mojo

The Mojo compiler version is pinned in `pixi.toml` (currently `1.2.0` from the
`https://conda.modular.com/max/` stable channel, set in
`package.host-dependencies`, `package.build-dependencies`, the `[dependencies]`
`mojo`, and the `operator-replacement` feature's `mojo`) and also in
`conda.recipe/recipe.yaml` and `conda.recipe/recipe.local.yaml`
(`requirements.build`/`host`/`run`). To update:

1. Check available versions: query
   `https://conda.modular.com/max/osx-arm64/repodata.json` (stable releases) or
   `https://conda.modular.com/max-nightly/osx-arm64/repodata.json` (nightlies)
   for `mojo-compiler` packages. Check `linux-64` and `linux-aarch64` too.
   `curl` must follow redirects (`-L`).
2. Update the version pin in `pixi.toml` (both `host-dependencies` and
   `build-dependencies`). When moving between stable and nightly also update
   the channel in `[workspace] channels` (stable = `.../max/`, nightly =
   `.../max-nightly/`).
3. Update the same pin in both `conda.recipe/recipe*.yaml` (all three of
   `build`/`host`/`run`). Otherwise `pixi build` and the published conda
   package will disagree. The CI `conda-recipe` job's
   `-c https://conda.modular.com/max...` channel must match too.
4. Run `pixi install` to update the lockfile.
5. Run `pixi run test-library` to verify compatibility.
6. Releases can have breaking changes, so check the release notes. If a
   nightly fails, try earlier nightlies.

## DuckDB

DuckDB source lives in the `third_party/duckdb` git submodule, pinned (via the
gitlink in the index, with `branch`/`shallow` recorded in `.gitmodules`) to the
release tag the FFI bindings were generated against. The `libduckdb`/`duckdb-cli`
conda pins and the submodule pin must match. `pixi run clone-duckdb` warns
when they drift. To bump (for example from `2.0.0` to `2.0.1`):

1. Move the submodule pin to the new tag and stage it:
   ```shell
   git -C third_party/duckdb fetch --depth 1 origin tag v2.0.1
   git -C third_party/duckdb checkout tags/v2.0.1
   git add third_party/duckdb
   ```
   Optionally bump `branch = v2.0.1` in `.gitmodules`.
2. Bump the conda pins to match: `libduckdb-devel`/`duckdb-cli` in `pixi.toml`,
   the `libduckdb >=…` ranges in both `conda.recipe/recipe*.yaml`, and the
   `version`/`tag` in `duckdb-from-source/{pixi.toml,recipe.yaml}` plus the
   `duckdb-from-source ==…` pins in `extensions/operator-replacement/recipe.yaml`.
3. `pixi install` to refresh the lockfile.
4. `pixi run generate-api` to regenerate `duckdb/_libduckdb.mojo` and
   `duckdb/_libduckdb_v2.mojo` from the new source, then commit them (CI
   `check-generated-api` fails otherwise). The unstable V2 API can change in
   any release, so expect V2 generator or test fixes.
5. `pixi run test` to verify. The CPP-ABI extensions
   (`mojo-kernel-overrides`, `mojo-gpu-operator`) are version-locked to the
   exact DuckDB and need a rebuild (`pixi run overrides-build` /
   `gpu-op-build`). Major releases usually change DuckDB's internal C++ API,
   so expect porting work, not only a rebuild (all three C++ extensions
   needed one for 2.0). Since 2.0, DuckDB's CMake does not take the version
   from `git describe`: a source build without `OVERRIDE_GIT_DESCRIBE=vX.Y.Z`
   reports its commit hash and rejects extensions whose footer says `vX.Y.Z`.
   `build_runner.sh` and the `duckdb-from-source` recipe pass it.
