#include "duckdb.h"

// Workaround wrappers for Mojo FFI limitations with by-value struct passing.
// Mojo's OwnedDLHandle/get_function does not correctly implement C ABI struct
// coercion (https://github.com/modular/modular/issues/3144). Multi-field structs
// get corrupted or crash when passed/returned by value through function pointers
// obtained via dlsym. These shims convert to/from pointer-based calling.
//
// Single-field wrapper structs (duckdb_date, duckdb_time, duckdb_timestamp, etc.)
// work fine because they behave like scalars at the ABI level.

// ---------------------------------------------------------------------------
// duckdb_result by-value workarounds
// ---------------------------------------------------------------------------

duckdb_data_chunk workaround_fetch_chunk_ptr(duckdb_result *result) {
    if (!result) {
        return (duckdb_data_chunk) { 0 };
    }
    return duckdb_fetch_chunk(*result);
}

duckdb_statement_type workaround_result_statement_type_ptr(duckdb_result *result) {
    if (!result) {
        return DUCKDB_STATEMENT_TYPE_INVALID;
    }
    return duckdb_result_statement_type(*result);
}

// ---------------------------------------------------------------------------
// duckdb_decimal workarounds (multi-field: width, scale, value)
// ---------------------------------------------------------------------------

void workaround_get_decimal_ptr(duckdb_value val, duckdb_decimal *out) {
    if (out) {
        *out = duckdb_get_decimal(val);
    }
}

duckdb_value workaround_create_decimal_ptr(duckdb_decimal *decimal) {
    if (!decimal) {
        return NULL;
    }
    return duckdb_create_decimal(*decimal);
}

double workaround_decimal_to_double_ptr(duckdb_decimal *val) {
    if (!val) {
        return 0.0;
    }
    return duckdb_decimal_to_double(*val);
}

void workaround_double_to_decimal_ptr(double val, uint8_t width, uint8_t scale, duckdb_decimal *out) {
    if (out) {
        *out = duckdb_double_to_decimal(val, width, scale);
    }
}

// ---------------------------------------------------------------------------
// Date/time struct conversion workarounds
// All params use raw scalars or pointers — no struct-by-value at all,
// because the DLHandle ABI bug corrupts even single-field structs when
// combined with other arguments.
// ---------------------------------------------------------------------------

void workaround_from_date_ptr(int32_t days, duckdb_date_struct *out) {
    if (out) {
        duckdb_date d = {days};
        *out = duckdb_from_date(d);
    }
}

int32_t workaround_to_date_ptr(duckdb_date_struct *date) {
    if (!date) return 0;
    return duckdb_to_date(*date).days;
}

void workaround_from_time_ptr(int64_t micros, duckdb_time_struct *out) {
    if (out) {
        duckdb_time t = {micros};
        *out = duckdb_from_time(t);
    }
}

int64_t workaround_to_time_ptr(duckdb_time_struct *time) {
    if (!time) return 0;
    return duckdb_to_time(*time).micros;
}

void workaround_from_timestamp_ptr(int64_t micros, duckdb_timestamp_struct *out) {
    if (out) {
        duckdb_timestamp ts = {micros};
        *out = duckdb_from_timestamp(ts);
    }
}

int64_t workaround_to_timestamp_ptr(duckdb_timestamp_struct *ts) {
    if (!ts) return 0;
    return duckdb_to_timestamp(*ts).micros;
}

void workaround_from_time_tz_ptr(uint64_t bits, duckdb_time_tz_struct *out) {
    if (out) {
        duckdb_time_tz tz = {bits};
        *out = duckdb_from_time_tz(tz);
    }
}

// ---------------------------------------------------------------------------
// Query progress workaround (multi-field return: percentage, rows, total)
// Takes opaque connection pointer, returns via out-param.
// ---------------------------------------------------------------------------

void workaround_query_progress_ptr(duckdb_connection conn, duckdb_query_progress_type *out) {
    if (out) {
        *out = duckdb_query_progress(conn);
    }
}

