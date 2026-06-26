// mojo_overrides — a DuckDB C++ extension that dispatches selected built-in
// scalar/aggregate functions to Mojo SIMD kernels, WITHOUT patching DuckDB.
//
// At load (`mojo_overrides_duckdb_cpp_init`) it mutates the built-in catalog
// function entries in place: it captures each original function pointer (used as
// the slow-path fallback) and installs a wrapper that, for FLAT/all-valid input,
// calls a linked-in Mojo SIMD kernel over the raw column buffers. Aggregate
// state structs are mirrored and guarded by a runtime state_size check.
//
// The Mojo SIMD kernels are linked into this same shared object (capi_shim.mojo,
// emitted as an object and linked by build.sh).

#include "duckdb.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/catalog_entry/aggregate_function_catalog_entry.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include <cstdint>
#include <cstdio>

// SIMD kernels, linked into this same shared object from capi_shim.mojo.
extern "C" {
void mojo_sqrt_f64(const double *, double *, int64_t);
void mojo_sin_f64(const double *, double *, int64_t);
void mojo_cos_f64(const double *, double *, int64_t);
void mojo_ln_f64(const double *, double *, int64_t);
void mojo_exp_f64(const double *, double *, int64_t);
void mojo_log10_f64(const double *, double *, int64_t);
double mojo_sum_f64(const double *, int64_t);
double mojo_min_f64(const double *, int64_t);
double mojo_max_f64(const double *, int64_t);
float mojo_min_f32(const float *, int64_t);
float mojo_max_f32(const float *, int64_t);
// int128 reduce: writes the chunk sum to *out, sets *overflow!=0 if any add
// overflowed int128 (caller then falls back to stock for exact throw semantics).
void mojo_sum_i128(const void *, int64_t, void *out, int32_t *overflow);
// per-row vector-distance folds over two contiguous array buffers of length n.
float mojo_array_dot_f32(const float *, const float *, int64_t);
double mojo_array_dot_f64(const double *, const double *, int64_t);
float mojo_array_l2dist_f32(const float *, const float *, int64_t);
double mojo_array_l2dist_f64(const double *, const double *, int64_t);
float mojo_array_cosine_sim_f32(const float *, const float *, int64_t);
double mojo_array_cosine_sim_f64(const double *, const double *, int64_t);
// nullable (validity-masked) reductions: reduce only valid lanes, return valid count.
void mojo_sum_f64_masked(const double *, const uint64_t *valid, int64_t, double *out_sum, int64_t *out_count);
void mojo_min_f64_masked(const double *, const uint64_t *valid, int64_t, double *out_val, int64_t *out_count);
void mojo_max_f64_masked(const double *, const uint64_t *valid, int64_t, double *out_val, int64_t *out_count);
void mojo_min_f32_masked(const float *, const uint64_t *valid, int64_t, float *out_val, int64_t *out_count);
void mojo_max_f32_masked(const float *, const uint64_t *valid, int64_t, float *out_val, int64_t *out_count);
void mojo_sum_i128_masked(const void *, const uint64_t *valid, int64_t, void *out_val, int64_t *out_count,
                          int32_t *overflow);
}

namespace duckdb {

typedef double (*mojo_red64_t)(const double *, int64_t);
typedef float (*mojo_red32_t)(const float *, int64_t);
typedef void (*mojo_un64_t)(const double *, double *, int64_t);

static bool IsD(const LogicalType &t) {
	return t.id() == LogicalTypeId::DOUBLE;
}
static bool IsF(const LogicalType &t) {
	return t.id() == LogicalTypeId::FLOAT;
}

// --- mirrored aggregate state layouts (verified by runtime state_size checks) ---
struct SumStateM {
	bool isset;
	double value;
};
struct AvgStateM {
	uint64_t count;
	double value;
};
struct MinMaxD {
	double value;
	bool isset;
};
struct MinMaxF {
	float value;
	bool isset;
};
// SumState<hugeint_t> / AvgState<hugeint_t> — the INT128 / high-precision DECIMAL
// accumulator layouts (verified by runtime state_size checks). hugeint_t is
// {uint64_t lower; int64_t upper}, so both states are 24 bytes.
struct SumStateHugeint {
	bool isset;
	hugeint_t value;
};
struct AvgStateHugeint {
	uint64_t count;
	hugeint_t value;
};
static_assert(sizeof(SumStateHugeint) == 24, "SumState<hugeint_t> layout drift");
static_assert(sizeof(AvgStateHugeint) == 24, "AvgState<hugeint_t> layout drift");

// Checked 128-bit add: state += partial, returns true on overflow (state left
// unchanged). hugeint_t shares its little-endian layout with __int128.
static inline bool CheckedAdd128(hugeint_t &state, const hugeint_t &partial) {
	__int128 a, b, r;
	__builtin_memcpy(&a, &state, 16);
	__builtin_memcpy(&b, &partial, 16);
	if (__builtin_add_overflow(a, b, &r)) {
		return true;
	}
	__builtin_memcpy(&state, &r, 16);
	return false;
}

// ---------------- scalar (double -> double) wrappers via X-macro ----------------
#define MOJO_SCALAR(NAME)                                                                                              \
	static scalar_function_t g_orig_##NAME = nullptr;                                                                  \
	static mojo_un64_t g_k_##NAME = nullptr;                                                                           \
	static void Mojo_##NAME(DataChunk &args, ExpressionState &state, Vector &result) {                                 \
		auto &in = args.data[0];                                                                                       \
		if (g_k_##NAME && in.GetVectorType() == VectorType::FLAT_VECTOR) {                                             \
			result.SetVectorType(VectorType::FLAT_VECTOR);                                                             \
			g_k_##NAME(FlatVector::GetData<double>(in), FlatVector::GetData<double>(result), (int64_t)args.size());    \
			/* NULL slots computed f(garbage) above are masked out by copying the input validity. */                  \
			if (!FlatVector::Validity(in).AllValid()) {                                                                \
				FlatVector::Validity(result).Copy(FlatVector::Validity(in), args.size());                              \
			}                                                                                                          \
			return;                                                                                                    \
		}                                                                                                              \
		g_orig_##NAME(args, state, result);                                                                            \
	}
MOJO_SCALAR(sqrt)
MOJO_SCALAR(sin)
MOJO_SCALAR(cos)
MOJO_SCALAR(ln)
MOJO_SCALAR(exp)
MOJO_SCALAR(log10)

// ---------------- aggregate simple_update wrappers ----------------
static aggregate_simple_update_t g_orig_sum = nullptr, g_orig_avg = nullptr;
static aggregate_simple_update_t g_orig_min64 = nullptr, g_orig_max64 = nullptr, g_orig_min32 = nullptr,
                                 g_orig_max32 = nullptr;
static mojo_red64_t g_k_sum = nullptr, g_k_min64 = nullptr, g_k_max64 = nullptr;
static mojo_red32_t g_k_min32 = nullptr, g_k_max32 = nullptr;

static bool FlatValid(Vector &v) {
	return v.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(v).AllValid();
}
static inline bool IsFlat(Vector &v) {
	return v.GetVectorType() == VectorType::FLAT_VECTOR;
}
// Validity bitmask words (uint64, bit set = valid). Only valid to call when
// !AllValid() (otherwise GetData() may be null — but we always gate on that).
static inline const uint64_t *ValidWords(Vector &v) {
	return reinterpret_cast<const uint64_t *>(FlatVector::Validity(v).GetData());
}

static void MojoSum(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_sum && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<SumStateM *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			st.value += g_k_sum(data, (int64_t)n);
			st.isset = true;
			return;
		}
		double partial;
		int64_t cnt;
		mojo_sum_f64_masked(data, ValidWords(in[0]), (int64_t)n, &partial, &cnt);
		st.value += partial;
		if (cnt > 0) { st.isset = true; }
		return;
	}
	g_orig_sum(in, aid, ic, sp, n);
}
static void MojoAvg(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_sum && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<AvgStateM *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			st.value += g_k_sum(data, (int64_t)n);
			st.count += n;
			return;
		}
		double partial;
		int64_t cnt;
		mojo_sum_f64_masked(data, ValidWords(in[0]), (int64_t)n, &partial, &cnt);
		st.value += partial;
		st.count += (uint64_t)cnt;
		return;
	}
	g_orig_avg(in, aid, ic, sp, n);
}
static void MojoMin64(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_min64 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			double c = g_k_min64(data, (int64_t)n);
			if (!st.isset || c < st.value) { st.value = c; st.isset = true; }
			return;
		}
		double c;
		int64_t cnt;
		mojo_min_f64_masked(data, ValidWords(in[0]), (int64_t)n, &c, &cnt);
		if (cnt > 0 && (!st.isset || c < st.value)) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_min64(in, aid, ic, sp, n);
}
static void MojoMax64(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_max64 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			double c = g_k_max64(data, (int64_t)n);
			if (!st.isset || c > st.value) { st.value = c; st.isset = true; }
			return;
		}
		double c;
		int64_t cnt;
		mojo_max_f64_masked(data, ValidWords(in[0]), (int64_t)n, &c, &cnt);
		if (cnt > 0 && (!st.isset || c > st.value)) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_max64(in, aid, ic, sp, n);
}
static void MojoMin32(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_min32 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		auto data = FlatVector::GetData<float>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			float c = g_k_min32(data, (int64_t)n);
			if (!st.isset || c < st.value) { st.value = c; st.isset = true; }
			return;
		}
		float c;
		int64_t cnt;
		mojo_min_f32_masked(data, ValidWords(in[0]), (int64_t)n, &c, &cnt);
		if (cnt > 0 && (!st.isset || c < st.value)) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_min32(in, aid, ic, sp, n);
}
static void MojoMax32(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_max32 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		auto data = FlatVector::GetData<float>(in[0]);
		if (FlatVector::Validity(in[0]).AllValid()) {
			float c = g_k_max32(data, (int64_t)n);
			if (!st.isset || c > st.value) { st.value = c; st.isset = true; }
			return;
		}
		float c;
		int64_t cnt;
		mojo_max_f32_masked(data, ValidWords(in[0]), (int64_t)n, &c, &cnt);
		if (cnt > 0 && (!st.isset || c > st.value)) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_max32(in, aid, ic, sp, n);
}

// ---------------- int128 / high-precision-decimal sum & avg ----------------
// HUGEINT and DECIMAL(19..38) sum/avg accumulate via the overflow-checked,
// non-inlined Hugeint::Add (a call per element). Route FLAT/all-valid input
// through the multi-accumulator Mojo reduce; fall back to stock on any int128
// overflow (so the exact throw semantics are preserved).
static aggregate_simple_update_t g_orig_sum_i128 = nullptr, g_orig_avg_i128 = nullptr;

static bool IsI128(const LogicalType &t) {
	return t.InternalType() == PhysicalType::INT128;
}

static void MojoSumHugeint(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (n > 0 && IsFlat(in[0])) {
		auto data = FlatVector::GetData<hugeint_t>(in[0]);
		hugeint_t partial;
		int32_t of = 0;
		int64_t cnt = (int64_t)n;
		if (FlatVector::Validity(in[0]).AllValid()) {
			mojo_sum_i128(data, (int64_t)n, &partial, &of);
		} else {
			mojo_sum_i128_masked(data, ValidWords(in[0]), (int64_t)n, &partial, &cnt, &of);
		}
		if (!of) {
			auto &st = *reinterpret_cast<SumStateHugeint *>(sp);
			hugeint_t cur = st.value;
			if (!CheckedAdd128(cur, partial)) {
				st.value = cur;
				if (cnt > 0) { st.isset = true; }
				return;
			}
		}
	}
	g_orig_sum_i128(in, aid, ic, sp, n);
}
static void MojoAvgHugeint(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (n > 0 && IsFlat(in[0])) {
		auto data = FlatVector::GetData<hugeint_t>(in[0]);
		hugeint_t partial;
		int32_t of = 0;
		int64_t cnt = (int64_t)n;
		if (FlatVector::Validity(in[0]).AllValid()) {
			mojo_sum_i128(data, (int64_t)n, &partial, &of);
		} else {
			mojo_sum_i128_masked(data, ValidWords(in[0]), (int64_t)n, &partial, &cnt, &of);
		}
		if (!of) {
			auto &st = *reinterpret_cast<AvgStateHugeint *>(sp);
			hugeint_t cur = st.value;
			if (!CheckedAdd128(cur, partial)) {
				st.value = cur;
				st.count += (uint64_t)cnt;
				return;
			}
		}
	}
	g_orig_avg_i128(in, aid, ic, sp, n);
}

// DECIMAL sum/avg are bind-dispatched (the concrete per-internal-type function is
// produced at bind time, like min/max) → wrap the bind, swap the resolved
// simple_update only when it resolves to the INT128 state (24-byte hugeint state).
static bind_aggregate_function_t g_orig_sum_dec_bind = nullptr, g_orig_avg_dec_bind = nullptr;

static unique_ptr<FunctionData> MojoSumDecimalBind(ClientContext &ctx, AggregateFunction &fn,
                                                   vector<unique_ptr<Expression>> &args) {
	auto r = g_orig_sum_dec_bind(ctx, fn, args);
	if (fn.simple_update && fn.state_size && !fn.arguments.empty() && IsI128(fn.arguments[0]) &&
	    fn.state_size(fn) == sizeof(SumStateHugeint)) {
		if (!g_orig_sum_i128) g_orig_sum_i128 = fn.simple_update;
		fn.simple_update = MojoSumHugeint;
	}
	return r;
}
static unique_ptr<FunctionData> MojoAvgDecimalBind(ClientContext &ctx, AggregateFunction &fn,
                                                   vector<unique_ptr<Expression>> &args) {
	auto r = g_orig_avg_dec_bind(ctx, fn, args);
	if (fn.simple_update && fn.state_size && !fn.arguments.empty() && IsI128(fn.arguments[0]) &&
	    fn.state_size(fn) == sizeof(AvgStateHugeint)) {
		if (!g_orig_avg_i128) g_orig_avg_i128 = fn.simple_update;
		fn.simple_update = MojoAvgHugeint;
	}
	return r;
}

// min/max are registered as ANY->ANY with a bind callback; the concrete per-type
// simple_update is produced at bind time. So we wrap the bind: run the original
// (which resolves the concrete function), then swap its double/float simple_update.
static bind_aggregate_function_t g_orig_min_bind = nullptr, g_orig_max_bind = nullptr;

static unique_ptr<FunctionData> MojoMinBind(ClientContext &ctx, AggregateFunction &fn,
                                            vector<unique_ptr<Expression>> &args) {
	auto r = g_orig_min_bind(ctx, fn, args);
	if (fn.simple_update && fn.state_size && !fn.arguments.empty()) {
		auto ssz = fn.state_size(fn);
		if (IsD(fn.arguments[0]) && ssz == sizeof(MinMaxD)) {
			if (!g_orig_min64) g_orig_min64 = fn.simple_update;
			fn.simple_update = MojoMin64;
		} else if (IsF(fn.arguments[0]) && ssz == sizeof(MinMaxF)) {
			if (!g_orig_min32) g_orig_min32 = fn.simple_update;
			fn.simple_update = MojoMin32;
		}
	}
	return r;
}
static unique_ptr<FunctionData> MojoMaxBind(ClientContext &ctx, AggregateFunction &fn,
                                            vector<unique_ptr<Expression>> &args) {
	auto r = g_orig_max_bind(ctx, fn, args);
	if (fn.simple_update && fn.state_size && !fn.arguments.empty()) {
		auto ssz = fn.state_size(fn);
		if (IsD(fn.arguments[0]) && ssz == sizeof(MinMaxD)) {
			if (!g_orig_max64) g_orig_max64 = fn.simple_update;
			fn.simple_update = MojoMax64;
		} else if (IsF(fn.arguments[0]) && ssz == sizeof(MinMaxF)) {
			if (!g_orig_max32) g_orig_max32 = fn.simple_update;
			fn.simple_update = MojoMax32;
		}
	}
	return r;
}

static void WrapMinMaxBind(Catalog &cat, ClientContext &ctx, const char *name, bind_aggregate_function_t wrapper,
                           bind_aggregate_function_t &orig_slot) {
	auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	for (auto &f : e.functions.functions) {
		// the scalar templated overload: ANY -> ANY (skip the ANY -> ANY[] list variant)
		if (f.return_type.id() == LogicalTypeId::ANY && f.bind) {
			orig_slot = f.bind;
			f.bind = wrapper;
		}
	}
}

// sum/avg over DECIMAL are bind-dispatched too (one DECIMAL overload with a bind
// callback). Swap that overload's bind so the concrete INT128 resolution is rerouted.
static void WrapDecimalAggBind(Catalog &cat, ClientContext &ctx, const char *name,
                               bind_aggregate_function_t wrapper, bind_aggregate_function_t &orig_slot) {
	auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	for (auto &f : e.functions.functions) {
		if (!f.arguments.empty() && f.arguments[0].id() == LogicalTypeId::DECIMAL && f.bind) {
			orig_slot = f.bind;
			f.bind = wrapper;
		}
	}
}

static void OverrideScalar(Catalog &cat, ClientContext &ctx, const char *name, scalar_function_t wrapper,
                           scalar_function_t &orig_slot) {
	auto &e = cat.GetEntry<ScalarFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	for (auto &f : e.functions.functions) {
		if (f.arguments.size() == 1 && IsD(f.arguments[0]) && IsD(f.return_type)) {
			orig_slot = f.function;
			f.function = wrapper;
		}
	}
}

// ---------------- array distance / similarity folds ----------------
// array_distance / array_inner_product / array_cosine_* fold two arrays to a
// scalar per row via a serial single-accumulator scalar loop in stock DuckDB.
// We swap the per-overload `function` pointer for a wrapper that mirrors
// ArrayGenericFold exactly (NULL-row -> NULL, NULL child element -> throw,
// constant-vector result when count==1) and only swaps the inner reduction for a
// SIMD kernel. The caller-side POST transform derives negative_inner_product
// (-x) and cosine_distance (1 - x) from the dot / cosine_sim kernels.
enum class FoldPost { IDENTITY, ONE_MINUS, NEGATE };

template <class T, T (*KERNEL)(const T *, const T *, int64_t), FoldPost POST>
static void MojoArrayFold(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lstate = state.Cast<ExecuteFunctionState>();
	const auto &expr = lstate.expr.Cast<BoundFunctionExpression>();
	const auto &func_name = expr.function.name;

	const auto count = args.size();
	auto &lhs_child = ArrayVector::GetEntry(args.data[0]);
	auto &rhs_child = ArrayVector::GetEntry(args.data[1]);

	const auto &lhs_child_validity = FlatVector::Validity(lhs_child);
	const auto &rhs_child_validity = FlatVector::Validity(rhs_child);

	UnifiedVectorFormat lhs_format;
	UnifiedVectorFormat rhs_format;
	args.data[0].ToUnifiedFormat(count, lhs_format);
	args.data[1].ToUnifiedFormat(count, rhs_format);

	auto lhs_data = FlatVector::GetData<T>(lhs_child);
	auto rhs_data = FlatVector::GetData<T>(rhs_child);
	auto res_data = FlatVector::GetData<T>(result);

	const auto array_size = ArrayType::GetSize(args.data[0].GetType());
	D_ASSERT(array_size == ArrayType::GetSize(args.data[1].GetType()));

	for (idx_t i = 0; i < count; i++) {
		const auto lhs_idx = lhs_format.sel->get_index(i);
		const auto rhs_idx = rhs_format.sel->get_index(i);

		if (!lhs_format.validity.RowIsValid(lhs_idx) || !rhs_format.validity.RowIsValid(rhs_idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}

		const auto left_offset = lhs_idx * array_size;
		if (!lhs_child_validity.CheckAllValid(left_offset + array_size, left_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: left argument can not contain NULL values", func_name));
		}
		const auto right_offset = rhs_idx * array_size;
		if (!rhs_child_validity.CheckAllValid(right_offset + array_size, right_offset)) {
			throw InvalidInputException(StringUtil::Format("%s: right argument can not contain NULL values", func_name));
		}

		T v = KERNEL(lhs_data + left_offset, rhs_data + right_offset, (int64_t)array_size);
		if (POST == FoldPost::ONE_MINUS) {
			v = static_cast<T>(1.0) - v;
		} else if (POST == FoldPost::NEGATE) {
			v = -v;
		}
		res_data[i] = v;
	}

	if (count == 1) {
		result.SetVectorType(VectorType::CONSTANT_VECTOR);
	}
}

// Swap the FLOAT / DOUBLE array-array overloads' function pointer. The array
// child type is set at catalog-registration time (the array *size* is not, but
// we don't need it here); the bind (ArrayGenericBinaryBind) is left untouched.
static void OverrideArrayFold(Catalog &cat, ClientContext &ctx, const char *name, scalar_function_t f32_wrap,
                              scalar_function_t f64_wrap) {
	auto &e = cat.GetEntry<ScalarFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	for (auto &f : e.functions.functions) {
		if (f.arguments.size() == 2 && f.arguments[0].id() == LogicalTypeId::ARRAY &&
		    f.arguments[1].id() == LogicalTypeId::ARRAY) {
			const auto child = ArrayType::GetChildType(f.arguments[0]).id();
			if (child == LogicalTypeId::FLOAT) {
				f.function = f32_wrap;
			} else if (child == LogicalTypeId::DOUBLE) {
				f.function = f64_wrap;
			}
		}
	}
}

void RegisterMojoOverrides(DatabaseInstance &db) {
	// Kernels are linked into this shared object; bind them directly.
	g_k_sqrt = mojo_sqrt_f64;
	g_k_sin = mojo_sin_f64;
	g_k_cos = mojo_cos_f64;
	g_k_ln = mojo_ln_f64;
	g_k_exp = mojo_exp_f64;
	g_k_log10 = mojo_log10_f64;
	g_k_sum = mojo_sum_f64;
	g_k_min64 = mojo_min_f64;
	g_k_max64 = mojo_max_f64;
	g_k_min32 = mojo_min_f32;
	g_k_max32 = mojo_max_f32;

	Connection con(db);
	con.context->RunFunctionInTransaction([&]() {
		auto &cat = Catalog::GetSystemCatalog(*con.context);
		auto &ctx = *con.context;

		OverrideScalar(cat, ctx, "sqrt", Mojo_sqrt, g_orig_sqrt);
		OverrideScalar(cat, ctx, "sin", Mojo_sin, g_orig_sin);
		OverrideScalar(cat, ctx, "cos", Mojo_cos, g_orig_cos);
		OverrideScalar(cat, ctx, "ln", Mojo_ln, g_orig_ln);
		OverrideScalar(cat, ctx, "exp", Mojo_exp, g_orig_exp);
		OverrideScalar(cat, ctx, "log10", Mojo_log10, g_orig_log10);

		// sum/avg have concrete per-type overloads in the catalog → override simple_update directly.
		auto agg = [&](const char *name, aggregate_simple_update_t wrap, aggregate_simple_update_t &orig,
		               size_t mirror) {
			auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
			for (auto &f : e.functions.functions) {
				if (f.arguments.size() == 1 && IsD(f.arguments[0]) && f.simple_update && f.state_size &&
				    f.state_size(f) == mirror) {
					orig = f.simple_update;
					f.simple_update = wrap;
				}
			}
		};
		agg("sum", MojoSum, g_orig_sum, sizeof(SumStateM));
		agg("avg", MojoAvg, g_orig_avg, sizeof(AvgStateM));

		// HUGEINT sum/avg: concrete per-type overload → override simple_update directly.
		auto aggI128 = [&](const char *name, aggregate_simple_update_t wrap, aggregate_simple_update_t &orig,
		                   size_t mirror) {
			auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
			for (auto &f : e.functions.functions) {
				if (f.arguments.size() == 1 && f.arguments[0].id() == LogicalTypeId::HUGEINT && f.simple_update &&
				    f.state_size && f.state_size(f) == mirror) {
					if (!orig) orig = f.simple_update;
					f.simple_update = wrap;
				}
			}
		};
		aggI128("sum", MojoSumHugeint, g_orig_sum_i128, sizeof(SumStateHugeint));
		aggI128("avg", MojoAvgHugeint, g_orig_avg_i128, sizeof(AvgStateHugeint));

		// DECIMAL(19..38) sum/avg resolve to the same INT128 state at bind → wrap the bind.
		WrapDecimalAggBind(cat, ctx, "sum", MojoSumDecimalBind, g_orig_sum_dec_bind);
		WrapDecimalAggBind(cat, ctx, "avg", MojoAvgDecimalBind, g_orig_avg_dec_bind);

		// min/max are bind-dispatched (ANY->ANY) → wrap the bind to swap f64/f32 simple_update.
		WrapMinMaxBind(cat, ctx, "min", MojoMinBind, g_orig_min_bind);
		WrapMinMaxBind(cat, ctx, "max", MojoMaxBind, g_orig_max_bind);

		// array distance / similarity folds: swap the per-overload function pointer.
		OverrideArrayFold(cat, ctx, "array_inner_product",
		                  MojoArrayFold<float, mojo_array_dot_f32, FoldPost::IDENTITY>,
		                  MojoArrayFold<double, mojo_array_dot_f64, FoldPost::IDENTITY>);
		OverrideArrayFold(cat, ctx, "array_negative_inner_product",
		                  MojoArrayFold<float, mojo_array_dot_f32, FoldPost::NEGATE>,
		                  MojoArrayFold<double, mojo_array_dot_f64, FoldPost::NEGATE>);
		OverrideArrayFold(cat, ctx, "array_distance",
		                  MojoArrayFold<float, mojo_array_l2dist_f32, FoldPost::IDENTITY>,
		                  MojoArrayFold<double, mojo_array_l2dist_f64, FoldPost::IDENTITY>);
		OverrideArrayFold(cat, ctx, "array_cosine_similarity",
		                  MojoArrayFold<float, mojo_array_cosine_sim_f32, FoldPost::IDENTITY>,
		                  MojoArrayFold<double, mojo_array_cosine_sim_f64, FoldPost::IDENTITY>);
		OverrideArrayFold(cat, ctx, "array_cosine_distance",
		                  MojoArrayFold<float, mojo_array_cosine_sim_f32, FoldPost::ONE_MINUS>,
		                  MojoArrayFold<double, mojo_array_cosine_sim_f64, FoldPost::ONE_MINUS>);
	});
	fprintf(stderr, "[mojo_overrides] installed (kernels linked in)\n");
}

} // namespace duckdb

extern "C" {
// Extension entry point: `LOAD 'mojo_overrides.duckdb_extension'`.
__attribute__((visibility("default"))) void mojo_overrides_duckdb_cpp_init(duckdb::ExtensionLoader &loader) {
	duckdb::RegisterMojoOverrides(loader.GetDatabaseInstance());
}
__attribute__((visibility("default"))) const char *mojo_overrides_version() {
	return duckdb::DuckDB::LibraryVersion();
}

// Client-callable entry point: a client (e.g. the Mojo bindings) can dlopen this
// library and call this with its connection handle to install the overrides,
// without LOAD / unsigned-extension handling. Same effect as the extension init.
__attribute__((visibility("default"))) void register_mojo_overrides(duckdb_connection connection) {
	auto con = reinterpret_cast<duckdb::Connection *>(connection);
	duckdb::RegisterMojoOverrides(*con->context->db);
}
}
