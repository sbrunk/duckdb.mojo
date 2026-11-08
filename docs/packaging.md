# Packaging and publishing

Both of these build a conda package of the bindings. Keep their pins in sync
(see `updating-deps.md`):

- `pixi build`: uses the `[package]` block and the `pixi-build-mojo` backend
  in `pixi.toml`. The backend infers the build steps, no recipe. Use it for
  local builds and when another Pixi workspace consumes duckdb.mojo as a
  source dependency.
- `conda.recipe/recipe.yaml` (rattler-build): the explicit recipe. Submit it
  to the
  [modular-community](https://github.com/modular/modular-community) channel.
  Their CI runs `rattler-build` on it. `mojo-compiler` is pinned exactly in
  `run`: a precompiled `.mojoc` only loads under the compiler that built it,
  and `pin_compatible` would accept a newer one. `libduckdb` is a `run`
  dependency because the bindings `dlopen` it.

Check a release locally with `conda.recipe/recipe.local.yaml`. It builds from
the working tree instead of a pushed git SHA:

```shell
rattler-build build --recipe conda.recipe/recipe.local.yaml -c conda-forge \
  -c https://conda.modular.com/max -c https://repo.prefix.dev/modular-community
```

A successful build runs the in-package smoke test and writes the `.conda`
under `output/<platform>/`. Before submitting, set `source.rev` in
`recipe.yaml` to the full release commit SHA.

The sub-packages in `extensions/` build through the `pixi-build-rattler-build`
backend instead (rattler-build on their own `recipe.yaml`, via `pixi build`).
That path does not publish `duckdb-mojo`.
