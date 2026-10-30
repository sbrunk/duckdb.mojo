#!/usr/bin/env python3
"""Generate the per-arity parts of duckdb/v2/functions.mojo.

Mojo infers a function's argument types from a function value, but not
through variadic function types, so `ScalarFunction.from_function` has one
overload (and one exec callback) per argument count. This script writes them
between the BEGIN/END GENERATED markers. Run it after changing the template:

    python3 scripts/generate_scalar_functions.py
"""

import re
from pathlib import Path

MAX_ARITY = 8
PATH = Path(__file__).resolve().parent.parent / "duckdb" / "v2" / "functions.mojo"
AGG_PATH = PATH.parent / "aggregate.mojo"


def types(n):
    return [f"A{i}" for i in range(1, n + 1)]


def from_function(n):
    ts = types(n)
    infer = ", ".join(f"{t}: Movable & Deinitable" for t in ts)
    sig = ", ".join(ts)
    params = "\n".join(f"        _param[{t}](params, special_nulls)" for t in ts[1:])
    lines = [
        "    @staticmethod",
        "    def from_function[",
        f"        {infer}, R: Movable & Deinitable, //,",
        "        name: StringLiteral,",
        f"        f: def({sig}) raises thin -> R,",
        "    ](*, volatile: Bool = False) raises DuckDBError -> ScalarFunction:",
    ]
    if n == 1:
        lines.append('        """A scalar function from a Mojo function (see the module docs)."""')
    lines += [
        "        var params = List[LogicalType]()",
        "        var special_nulls = False",
        "        var bind = Optional[_BindFn](None)",
        "        comptime if is_bind_data[A1]():",
        "            var callback: _BindFn = _bind_callback[A1]",
        "            bind = callback",
        "        else:",
        "            _param[A1](params, special_nulls)",
    ]
    if params:
        lines.append(params)
    lines += [
        "        return ScalarFunction(",
        f"            String(name), params^, logical_type_of[R](), _exec{n}[{sig}, R, f], bind, volatile, special_nulls",
        "        )",
        "",
    ]
    return "\n".join(lines)


def exec_fn(n):
    ts = types(n)
    infer = ", ".join(f"{t}: Movable & Deinitable" for t in ts)
    sig = ", ".join(ts)

    def call(first_sql):
        args = []
        for i, t in enumerate(ts):
            if first_sql == 1 and i == 0:
                args.append("b[]")
            else:
                k = i - first_sql
                args.append(f"call.get[{t}]({k}, v{k}, row)")
        return f"f({', '.join(args)})"

    def loop(first_sql, indent):
        p = " " * indent
        sql = ts[first_sql:]
        out = [f"{p}var v{k} = call.view({k})" for k in range(len(sql))]
        out += [f"{p}comptime r{k} = _required[{t}]()" for k, t in enumerate(sql)]
        out.append(f"{p}for row in range(call.size):")
        if sql:
            cond = " or ".join(f"(r{k} and not _valid(v{k}, row))" for k in range(len(sql)))
            out += [
                f"{p}    if {cond}:",
                f"{p}        call.out.write_null(row)",
                f"{p}        continue",
            ]
        out.append(f"{p}    call.out.write(row, {call(first_sql)})")
        return out

    lines = [
        f"def _exec{n}[{infer}, R: Movable & Deinitable, f: def({sig}) raises thin -> R](",
        "    info: duckdb_v2_scalar_function_exec_info_handle,",
        "    context: duckdb_v2_context_handle,",
        "    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],",
        ') abi("C"):',
        "    try:",
        f"        _run{n}[{sig}, R, f](info)",
        "    except e:",
        "        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)",
        "",
        "",
        f"def _run{n}[{infer}, R: Movable & Deinitable, f: def({sig}) raises thin -> R](",
        "    info: duckdb_v2_scalar_function_exec_info_handle,",
        ") raises:",
        "    var call = _Call(info)",
        "    comptime if is_bind_data[A1]():",
        "        var b = call.bind_data[A1]()",
    ]
    lines += loop(1, 8)
    lines.append("    else:")
    numeric = ", ".join(ts) + ", R"
    lines.append(f"        comptime if _all_numbers[{numeric}]():")
    lines.append("            if call.all_flat():")
    lines += [f"                var p{k} = call.numbers[{t}]({k})" for k, t in enumerate(ts)]
    lines += [f"                var s{k} = call.step({k})" for k in range(len(ts))]
    lines.append("                var dst = call.out.data[downcast[R, TrivialRegisterPassable]]()")
    args = ", ".join(f"p{k}[unsafe_offset=row * s{k}]" for k in range(len(ts)))
    lines += [
        "                for row in range(call.size):",
        f"                    dst[unsafe_offset=row] = rebind_var[downcast[R, TrivialRegisterPassable]](f({args}))",
        "                call.out.finish()",
        "                return",
    ]
    lines += loop(0, 8)
    lines += ["    call.out.finish()", "", ""]
    return "\n".join(lines)


def from_state(n):
    ts = types(n)
    infer = ", ".join(f"{t}: Movable & Deinitable" for t in ts)
    sig = ", ".join(ts)
    lines = [
        "    @staticmethod",
        "    def from_state[",
        f"        S: AggregateState, {infer}, //,",
        "        name: StringLiteral,",
        f"        update: def(mut S, {sig}) raises thin -> None,",
        "    ](*, order_dependent: Bool = False) raises DuckDBError -> AggregateFunction:",
    ]
    if n == 1:
        lines.append('        """An aggregate from a state type and its `update` method (see the module docs)."""')
    lines += ["        var params = List[LogicalType]()", "        var special_nulls = False"]
    lines += [f"        _param[{t}](params, special_nulls)" for t in ts]
    lines += [
        f"        return AggregateFunction.__init__[S](String(name), params^, _update{n}[S, {sig}, update], special_nulls, order_dependent)",
        "",
    ]
    return "\n".join(lines)


def update_fn(n):
    ts = types(n)
    infer = ", ".join(f"{t}: Movable & Deinitable" for t in ts)
    sig = ", ".join(ts)
    head = f"S: AggregateState, {infer}, update: def(mut S, {sig}) raises thin -> None"
    lines = [
        f"def _update{n}[{head}](",
        "    info: duckdb_v2_aggregate_function_update_info_handle,",
        "    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],",
        ') abi("C"):',
        "    try:",
        f"        _run_update{n}[S, {sig}, update](info)",
        "    except e:",
        "        _report(err, String(e), DUCKDB_V2_ERROR_QUERY_EXECUTOR)",
        "",
        "",
        f"def _run_update{n}[{head}](",
        "    info: duckdb_v2_aggregate_function_update_info_handle,",
        ") raises:",
        "    var call = _UpdateCall(info)",
    ]
    if n == 1:
        lines += [
            "    comptime if _batchable[S, A1]():",
            "        if call.one_state() and call.flat_without_nulls(0):",
            "            _batch[S](call)",
            "            return",
        ]
    lines += [f"    var v{k} = call.a.view({k})" for k in range(n)]
    lines += [f"    comptime r{k} = _required[{t}]()" for k, t in enumerate(ts)]
    cond = " or ".join(f"(r{k} and not _valid(v{k}, row))" for k in range(n))
    args = ", ".join(f"call.a.get[{t}]({k}, v{k}, row)" for k, t in enumerate(ts))
    lines += [
        "    for row in range(call.size):",
        f"        if {cond}:",
        "            continue",
        "        ref slot = _slot[S](call.states[unsafe_offset=row])",
        f"        update(slot.state, {args})",
        "        slot.seen = True",
        "",
        "",
        "",
    ]
    return "\n".join(lines)


def replace(text, marker, body):
    pattern = re.compile(
        rf"(?P<indent>[ ]*)# BEGIN GENERATED {marker}[^\n]*\n.*?(?P=indent)# END GENERATED {marker}\n", re.S
    )
    m = pattern.search(text)
    assert m, f"marker {marker} not found"
    indent = m.group("indent")
    block = (
        f"{indent}# BEGIN GENERATED {marker} (scripts/generate_scalar_functions.py)\n"
        + body
        + f"{indent}# END GENERATED {marker}\n"
    )
    return text[: m.start()] + block + text[m.end() :]


def main():
    text = PATH.read_text()
    text = replace(text, "from_function", "\n".join(from_function(n) for n in range(1, MAX_ARITY + 1)))
    text = replace(text, "exec", "\n\n" + "".join(exec_fn(n) for n in range(1, MAX_ARITY + 1)).rstrip("\n") + "\n\n\n")
    PATH.write_text(text)
    agg = AGG_PATH.read_text()
    agg = replace(agg, "from_state", "\n".join(from_state(n) for n in range(1, MAX_ARITY + 1)))
    agg = replace(agg, "update", "\n\n" + "".join(update_fn(n) for n in range(1, MAX_ARITY + 1)).rstrip("\n") + "\n\n\n")
    AGG_PATH.write_text(agg)


if __name__ == "__main__":
    main()
