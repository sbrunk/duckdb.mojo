"""Test extension for the V2 C API (`duckdb.extension_v2`).

The build compiles this file once and copies it under three names, each with
its own footer. DuckDB calls `<file name>_init_c_api_v2`, so each copy runs a
different entrypoint:

- `mojo_v2` (C_STRUCT_UNSTABLE): registers `mojo_v2_add(a BIGINT, b BIGINT)`
  through the raw V2 functions. Its exec callback reads the arguments through
  vector views, so it handles flat, constant and dictionary vectors (and
  flattens the others) and NULLs.
- `mojo_v2_fail` (C_STRUCT_UNSTABLE): its init function raises, so the load
  fails with that message.
- `mojo_v2_stable` (C_STRUCT v2.0.0): uses the stable target. DuckDB 2.0.0 has
  no stable V2 functions and refuses to load it.
"""

from duckdb._libduckdb_v2 import *
from duckdb.api_v2 import DuckDBV2, StrArg, check, null, null_error_info
from duckdb.extension_v2 import ExtensionV2


@always_inline
def _is_valid(view: duckdb_v2_vector_view, row: Int) -> Bool:
    if Int(view.validity) == 0:
        return True
    return (view.validity[unsafe_offset=row >> 6] >> UInt64(row & 63)) & 1 == 1


def _row(view: duckdb_v2_vector_view, i: Int) -> Int:
    return Int(view.sel[unsafe_offset=i]) if Int(view.sel) != 0 else i


def _add_rows(info: duckdb_v2_scalar_function_exec_info_handle) raises:
    ref lib = DuckDBV2().libduckdb()
    var err = null_error_info()
    var count: idx_t = 0
    check(lib.duckdb_v2_scalar_function_exec_get_row_count(info, Pointer(to=count), Pointer(to=err)), err)

    var views = List[duckdb_v2_vector_view]()
    for arg in range(2):
        var vector = null[duckdb_v2_vector_handle]()
        check(lib.duckdb_v2_scalar_function_exec_get_arg(info, idx_t(arg), Pointer(to=vector), Pointer(to=err)), err)
        # Views cover flat, constant and dictionary vectors. Others (sequence,
        # FSST, shredded) must be flattened first.
        var kind: DUCKDB_V2_VECTOR_TYPE = DUCKDB_V2_VECTOR_TYPE_OTHER
        check(lib.duckdb_v2_vector_get_vector_type(vector, Pointer(to=kind), Pointer(to=err)), err)
        if kind == DUCKDB_V2_VECTOR_TYPE_OTHER:
            check(lib.duckdb_v2_vector_flatten(vector, Pointer(to=err)), err)
        var view = duckdb_v2_vector_view(
            null[Pointer[NoneType, ImmUntrackedOrigin]](),
            null[Pointer[UInt64, ImmUntrackedOrigin]](),
            null[Pointer[duckdb_v2_sel_t, ImmUntrackedOrigin]](),
            0,
        )
        check(lib.duckdb_v2_vector_get_view(vector, Pointer(to=view), Pointer(to=err)), err)
        views.append(view^)

    var result = null[duckdb_v2_vector_handle]()
    check(lib.duckdb_v2_scalar_function_exec_get_result(info, Pointer(to=result), Pointer(to=err)), err)
    var out_raw = null[Pointer[NoneType, MutUntrackedOrigin]]()
    check(lib.duckdb_v2_vector_get_data_mutable(result, Pointer(to=out_raw), Pointer(to=err)), err)
    var out = out_raw.unsafe_bitcast[Int64]()
    var a = views[0].data.unsafe_bitcast[Int64]()
    var b = views[1].data.unsafe_bitcast[Int64]()
    # Write NULLs straight into the result's validity mask. vector_set_null
    # checks the row against the vector size, which is still 0 here.
    var validity = null[Pointer[UInt64, MutUntrackedOrigin]]()
    check(lib.duckdb_v2_vector_flat_get_validity_mutable(result, Pointer(to=validity), Pointer(to=err)), err)

    for i in range(Int(count)):
        var ra = _row(views[0], i)
        var rb = _row(views[1], i)
        if _is_valid(views[0], ra) and _is_valid(views[1], rb):
            out[unsafe_offset=i] = a[unsafe_offset=ra] + b[unsafe_offset=rb]
        else:
            validity[unsafe_offset=i >> 6] &= ~(UInt64(1) << UInt64(i & 63))


def _add_exec(
    info: duckdb_v2_scalar_function_exec_info_handle,
    context: duckdb_v2_context_handle,
    err: Pointer[duckdb_v2_error_info_handle, MutUntrackedOrigin],
) abi("C"):
    try:
        _add_rows(info)
    except e:
        ref lib = DuckDBV2().libduckdb()
        var message = StrArg(String(e))
        _ = lib.duckdb_v2_error_info_set_code(err[], DUCKDB_V2_ERROR_QUERY_EXECUTOR)
        _ = lib.duckdb_v2_error_info_set_text(err[], message.ptr())


def init(ext: ExtensionV2) raises:
    ref lib = ext.libduckdb()
    var factory = ext.factory()
    var err = null_error_info()

    var bigint = null[duckdb_v2_logical_type_handle]()
    check(
        lib.duckdb_v2_factory_create_type_from_id(
            factory,
            DUCKDB_V2_LOGICAL_TYPE_ID_BIGINT,
            null[Pointer[duckdb_v2_identifier_t, ImmUntrackedOrigin]](),
            null[Pointer[duckdb_v2_value_handle, ImmUntrackedOrigin]](),
            0,
            Pointer(to=bigint),
            Pointer(to=err),
        ),
        err,
    )

    var function = null[duckdb_v2_scalar_function_handle]()
    check(lib.duckdb_v2_scalar_function_create(factory, Pointer(to=function), Pointer(to=err)), err)
    var name = StrArg("mojo_v2_add")
    check(lib.duckdb_v2_scalar_function_set_name(function, name.ptr(), Pointer(to=err)), err)

    var signature = null[duckdb_v2_function_signature_handle]()
    check(lib.duckdb_v2_scalar_function_get_signature(function, Pointer(to=signature), Pointer(to=err)), err)
    for param in ["a", "b"]:
        var param_name = StrArg(param)
        check(
            lib.duckdb_v2_function_signature_add_parameter(
                signature,
                param_name.ptr(),
                bigint,
                null[duckdb_v2_value_handle](),  # no default value
                DUCKDB_V2_FUNCTION_PARAMETER_KIND_STANDARD,
                Pointer(to=err),
            ),
            err,
        )
    check(lib.duckdb_v2_function_signature_set_return_type(signature, bigint, Pointer(to=err)), err)
    check(lib.duckdb_v2_scalar_function_set_exec_callback(function, _add_exec, Pointer(to=err)), err)
    check(lib.duckdb_v2_extension_register_scalar_function(ext.extension, function, Pointer(to=err)), err)

    _ = lib.duckdb_v2_scalar_function_destroy(Pointer(to=function))
    _ = lib.duckdb_v2_logical_type_destroy(Pointer(to=bigint))


def init_fail(ext: ExtensionV2) raises:
    raise Error("mojo_v2_fail: intentional failure in the init function")


def init_stable(ext: ExtensionV2) raises:
    pass


@export("mojo_v2_init_c_api_v2")
def mojo_v2_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    ExtensionV2.run[init, unstable=True](input)


@export("mojo_v2_fail_init_c_api_v2")
def mojo_v2_fail_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    ExtensionV2.run[init_fail, unstable=True](input)


@export("mojo_v2_stable_init_c_api_v2")
def mojo_v2_stable_init_c_api_v2(input: Pointer[duckdb_v2_extension_input, MutUntrackedOrigin]) abi("C"):
    ExtensionV2.run[init_stable, unstable=False](input)
