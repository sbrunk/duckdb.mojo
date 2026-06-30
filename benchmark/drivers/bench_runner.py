#!/usr/bin/env python3
"""Unified DuckDB benchmark_runner compare driver.

Runs DuckDB's `benchmark_runner` once per engine (each engine toggles which
extension loads), parses the per-iteration timings it prints, and tabulates the
warm median per benchmark. Engines:

    stock   no extension
    cpu     mojo-kernel-overrides  (DUCKDB_BENCH_EXTENSION)
    gpu     mojo-gpu-operator      (DUCKDB_BENCH_EXTENSION, +GPU_OP_MIN_ROWS=0)

Usage:
    bench_runner.py <group> [--engines stock,cpu,gpu] [--by-suffix] [runner-args...]

    <group>       a dir under benchmark/sql/ (mojo_simd | gpu_xover | gpu_knn), or a
                  path under the runner tree for built-ins (e.g. tpch/sf1/q0[16]).
    --engines     toggle mode (default): run the SAME benchmarks under each engine,
                  one column per engine.
    --by-suffix   per-file mode: each <regime>_<engine>.benchmark runs under the
                  extension named by its suffix (stock|cpu|gpu); rows are regimes.
    runner-args   anything else (e.g. --threads=1) is passed through to the runner.

Env overrides: DUCKDB_SRC, RUNNER, OVR_EXT, GPU_EXT.
"""
from __future__ import annotations

import argparse
import os
import shutil
import statistics
import subprocess
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent.parent
SRC = Path(os.environ.get("DUCKDB_SRC", ROOT / "third_party" / "duckdb"))
RUNNER = Path(os.environ.get("RUNNER", SRC / "build/release/benchmark/benchmark_runner"))
OVR_EXT = os.environ.get("OVR_EXT", str(ROOT / "packages/mojo-kernel-overrides/build/mojo_overrides.duckdb_extension"))
GPU_EXT = os.environ.get("GPU_EXT", str(ROOT / "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension"))

# Per-engine environment overlaid on the inherited env (so caller-exported flags
# like GPU_OP_TRANSCENDENTAL still reach the runner).
ENGINE_ENV = {
    "stock": {},
    "cpu": {"DUCKDB_BENCH_EXTENSION": OVR_EXT},
    "gpu": {"DUCKDB_BENCH_EXTENSION": GPU_EXT, "GPU_OP_MIN_ROWS": "0"},
}


def stage_sql_groups() -> None:
    """Copy committed benchmark/sql/<group>/*.benchmark into the runner tree."""
    for group in sorted((ROOT / "benchmark" / "sql").glob("*")):
        if not group.is_dir():
            continue
        dst = SRC / "benchmark" / "micro" / group.name
        dst.mkdir(parents=True, exist_ok=True)
        for f in group.glob("*.benchmark"):
            shutil.copy(f, dst)


def run_medians(target: str, engine: str, runner_args: list[str]) -> dict[str, float]:
    """Run the runner for `target` (a regex or path) under `engine`; return
    {benchmark_name: median_ms}. The runner prints `name<TAB>run<TAB>timing`
    rows; timings are seconds (ERROR rows are skipped)."""
    env = {**os.environ, **ENGINE_ENV[engine]}
    proc = subprocess.run(
        [str(RUNNER), target, *runner_args],
        cwd=SRC, env=env, capture_output=True, text=True,
    )
    samples: dict[str, list[float]] = {}
    for line in (proc.stdout + proc.stderr).splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue
        name, _run, timing = parts
        try:
            t = float(timing)  # skip the header ("timing") and ERROR rows first,
        except ValueError:     # so they never create an empty bucket
            continue
        samples.setdefault(name, []).append(t)
    return {n: statistics.median(v) * 1000 for n, v in samples.items()}


def toggle_mode(group: str, engines: list[str], runner_args: list[str]) -> None:
    regex = f"benchmark/{group}.*" if "/" in group else f"benchmark/micro/{group}/.*"
    per_engine = {e: run_medians(regex, e, runner_args) for e in engines}
    names = sorted({n for d in per_engine.values() for n in d})

    def cell(e, n):
        return f"{per_engine[e][n]:10.2f}" if n in per_engine[e] else f"{'-':>10}"

    print(f"{'benchmark':40}" + "".join(f"{e:>10}" for e in engines) + f"{'winner':>9}")
    for n in names:
        present = {e: per_engine[e][n] for e in engines if n in per_engine[e]}
        winner = min(present, key=present.get) if present else "-"
        short = n.replace("benchmark/micro/", "")
        print(f"{short:40}" + "".join(cell(e, n) for e in engines) + f"{winner:>9}")


def suffix_mode(group: str, runner_args: list[str]) -> None:
    # ext per file from the <regime>_<engine>.benchmark name; rows = regime.
    rows: dict[str, dict[str, float]] = {}
    engines_seen: list[str] = []
    for f in sorted((SRC / "benchmark" / "micro" / group).glob("*.benchmark")):
        regime, _, engine = f.stem.rpartition("_")
        if engine not in ENGINE_ENV:
            continue
        med = run_medians(f"benchmark/micro/{group}/{f.name}", engine, runner_args)
        ms = next(iter(med.values()), None)  # one benchmark per file
        rows.setdefault(regime, {})[engine] = ms
        if engine not in engines_seen:
            engines_seen.append(engine)
    engines = [e for e in ("stock", "cpu", "gpu") if e in engines_seen]
    print(f"{'regime':12}" + "".join(f"{e:>12}" for e in engines))
    for regime, by_eng in rows.items():
        cells = "".join(f"{by_eng[e]:12.2f}" if by_eng.get(e) is not None else f"{'-':>12}" for e in engines)
        print(f"{regime:12}{cells}")


def main() -> int:
    p = argparse.ArgumentParser(description="Compare stock/CPU/GPU via DuckDB's benchmark_runner.")
    p.add_argument("group", help="benchmark/sql group name, or a runner-tree path (e.g. tpch/sf1/q0[16])")
    p.add_argument("--engines", default="stock,cpu,gpu", help="comma list (toggle mode)")
    p.add_argument("--by-suffix", action="store_true", help="per-file engine from name suffix")
    # pixi forwards a literal `--`; drop it so argparse doesn't treat it as
    # end-of-options (which would make --engines/--by-suffix positional).
    argv = [a for a in sys.argv[1:] if a != "--"]
    args, runner_args = p.parse_known_args(argv)

    if not RUNNER.exists():
        sys.exit(f"no benchmark_runner at {RUNNER} - run 'pixi run bench-build'")
    stage_sql_groups()

    if args.by_suffix:
        suffix_mode(args.group, runner_args)
    else:
        toggle_mode(args.group, args.engines.split(","), runner_args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
