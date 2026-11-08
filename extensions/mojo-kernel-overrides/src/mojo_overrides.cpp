// mojo_overrides: a DuckDB C++ extension that dispatches selected built-in
// scalar/aggregate functions to Mojo SIMD kernels, without patching DuckDB.
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
#include "duckdb/common/vector/array_vector.hpp"
#include "duckdb/common/clustered_aggregate.hpp"
#include "duckdb/function/aggregate_state_layout.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/main/config.hpp"

#include <cstdint>
#include <cstdio>
#include <cmath>
#include <string>
#include <vector>

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
// blocked multi-query kNN: M queries x N embeddings, top-k each. Norms are
// per-vector (cosine: L2 norm; l2: squared norm; ip: ignored). Outputs row-major
// [query*k + slot]: out_ids (rowid or -1 for padding) and out_dists.
void mojo_knn_cosine_f32(const float *q, const float *nrm_q, int64_t m, const float *e, const float *nrm_e,
                         int64_t n, int64_t d, int64_t k, int64_t *out_ids, float *out_dists);
void mojo_knn_l2_f32(const float *q, const float *nrm_q, int64_t m, const float *e, const float *nrm_e, int64_t n,
                     int64_t d, int64_t k, int64_t *out_ids, float *out_dists);
void mojo_knn_ip_f32(const float *q, const float *nrm_q, int64_t m, const float *e, const float *nrm_e, int64_t n,
                     int64_t d, int64_t k, int64_t *out_ids, float *out_dists);
// fused sum-of-transcendental: the plain version returns the sum, the masked
// version returns the sum and the valid count.
double mojo_fsum_sqrt_f64(const double *, int64_t);
double mojo_fsum_sin_f64(const double *, int64_t);
double mojo_fsum_cos_f64(const double *, int64_t);
double mojo_fsum_ln_f64(const double *, int64_t);
double mojo_fsum_exp_f64(const double *, int64_t);
double mojo_fsum_log10_f64(const double *, int64_t);
void mojo_fsum_sqrt_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
void mojo_fsum_sin_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
void mojo_fsum_cos_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
void mojo_fsum_ln_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
void mojo_fsum_exp_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
void mojo_fsum_log10_f64_masked(const double *, const uint64_t *, int64_t, double *, int64_t *);
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
// DuckDB 2.0: SumState<T> is {T value; bool is_set} (was {bool isset; T value}).
struct SumStateM {
	double value;
	bool isset;
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
// SumState<hugeint_t> / AvgState<hugeint_t>: the INT128 / high-precision DECIMAL
// accumulator layouts (verified by runtime state_size checks). hugeint_t is
// {uint64_t lower; int64_t upper}, so both states are 24 bytes.
struct SumStateHugeint {
	hugeint_t value;
	bool isset;
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
			g_k_##NAME(FlatVector::GetData<double>(in), FlatVector::GetDataMutable<double>(result), (int64_t)args.size());    \
			/* NULL slots computed f(garbage) above are masked out by copying the input validity. */                  \
			if (!FlatVector::Validity(in).CannotHaveNull()) {                                                          \
				FlatVector::ValidityMutable(result).Copy(FlatVector::Validity(in), args.size());                       \
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
static aggregate_cluster_update_t g_orig_sum = nullptr, g_orig_avg = nullptr;
static aggregate_cluster_update_t g_orig_min64 = nullptr, g_orig_max64 = nullptr, g_orig_min32 = nullptr,
                                 g_orig_max32 = nullptr;
static mojo_red64_t g_k_sum = nullptr, g_k_min64 = nullptr, g_k_max64 = nullptr;
static mojo_red32_t g_k_min32 = nullptr, g_k_max32 = nullptr;

// DuckDB 2.0 replaced simple_update with cluster_update, which is also called by the
// grouped hash aggregate with several runs. Only the ungrouped shape (one run over
// rows 0..n-1, no selection) takes the Mojo fast path; everything else falls back.
static inline data_ptr_t SingleRunState(const ClusteredAggr &cl, idx_t n) {
	auto runs = cl.runs();
	if (runs.size() != 1 || runs[0].sel != nullptr || runs[0].count != n) {
		return nullptr;
	}
	return runs[0].state;
}

// State layout guards: DuckDB 2.0 describes each state via get_state_type, so check
// field offsets, not only the size (SumState changed field order at equal size).
static bool OptionalLayoutIs(const BoundAggregateFunction &bf, optional_ptr<FunctionData> bd, idx_t value_size) {
	if (!bf.HasGetStateTypeCallback()) { return false; }
	auto layout = bf.GetStateType(bd);
	return layout.field.kind == AggregateFieldKind::OPTIONAL_VALUE && layout.field.field_offset == value_size &&
	       layout.field.children.size() == 1 && layout.field.children[0].field_offset == 0;
}
static bool AvgLayoutIs(const BoundAggregateFunction &bf, optional_ptr<FunctionData> bd, idx_t value_offset) {
	if (!bf.HasGetStateTypeCallback()) { return false; }
	auto layout = bf.GetStateType(bd);
	return layout.field.kind == AggregateFieldKind::STRUCT && layout.field.children.size() == 2 &&
	       layout.field.children[0].field_offset == 0 && layout.field.children[1].field_offset == value_offset;
}
static idx_t StateSize(const BoundAggregateFunction &bf, optional_ptr<FunctionData> bd) {
	return bf.HasStateSizeCallback() ? bf.GetStateSize(bd.get()) : 0;
}
static inline bool IsFlat(Vector &v) {
	return v.GetVectorType() == VectorType::FLAT_VECTOR;
}
// Validity bitmask words (uint64, bit set = valid). Only valid to call when
// !AllValid(). Otherwise GetData() may be null, but callers always check this first.
static inline const uint64_t *ValidWords(Vector &v) {
	return reinterpret_cast<const uint64_t *>(FlatVector::Validity(v).GetData());
}

static void MojoSum(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_sum && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<SumStateM *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_sum(in, aid, ic, cl, n);
}
static void MojoAvg(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_sum && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<AvgStateM *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_avg(in, aid, ic, cl, n);
}
static void MojoMin64(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_min64 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_min64(in, aid, ic, cl, n);
}
static void MojoMax64(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_max64 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		auto data = FlatVector::GetData<double>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_max64(in, aid, ic, cl, n);
}
static void MojoMin32(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_min32 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		auto data = FlatVector::GetData<float>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_min32(in, aid, ic, cl, n);
}
static void MojoMax32(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && g_k_max32 && n > 0 && IsFlat(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		auto data = FlatVector::GetData<float>(in[0]);
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_max32(in, aid, ic, cl, n);
}

// ---------------- int128 / high-precision-decimal sum & avg ----------------
// HUGEINT and DECIMAL(19..38) sum/avg accumulate via the overflow-checked,
// non-inlined Hugeint::Add (a call per element). Route FLAT/all-valid input
// through the multi-accumulator Mojo reduce; fall back to stock on any int128
// overflow (so the exact throw semantics are preserved).
static aggregate_cluster_update_t g_orig_sum_i128 = nullptr, g_orig_avg_i128 = nullptr;

static bool IsI128(const LogicalType &t) {
	return t.InternalType() == PhysicalType::INT128;
}

static void MojoSumHugeint(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && n > 0 && IsFlat(in[0])) {
		auto data = FlatVector::GetData<hugeint_t>(in[0]);
		hugeint_t partial;
		int32_t of = 0;
		int64_t cnt = (int64_t)n;
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_sum_i128(in, aid, ic, cl, n);
}
static void MojoAvgHugeint(Vector in[], AggregateInputData &aid, idx_t ic, const ClusteredAggr &cl, idx_t n) {
	data_ptr_t sp = SingleRunState(cl, n);
	if (sp && n > 0 && IsFlat(in[0])) {
		auto data = FlatVector::GetData<hugeint_t>(in[0]);
		hugeint_t partial;
		int32_t of = 0;
		int64_t cnt = (int64_t)n;
		if (FlatVector::Validity(in[0]).CannotHaveNull()) {
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
	g_orig_avg_i128(in, aid, ic, cl, n);
}

// DECIMAL sum/avg are bind-dispatched (the concrete per-internal-type function is
// produced at bind time, like min/max). So we wrap the bind and swap the resolved
// simple_update only when it resolves to the INT128 state (24-byte hugeint state).
static bind_aggregate_function_t g_orig_sum_dec_bind = nullptr, g_orig_avg_dec_bind = nullptr;

static unique_ptr<FunctionData> MojoSumDecimalBind(BindAggregateFunctionInput &input) {
	auto r = g_orig_sum_dec_bind(input);
	auto &fn = input.GetBoundFunction();
	auto &args = fn.GetArguments();
	if (fn.HasStateClusterUpdateCallback() && !args.empty() && IsI128(args[0]) &&
	    StateSize(fn, r.get()) == sizeof(SumStateHugeint) && OptionalLayoutIs(fn, r.get(), sizeof(hugeint_t))) {
		if (!g_orig_sum_i128) g_orig_sum_i128 = fn.GetStateClusterUpdateCallback();
		fn.SetStateClusterUpdateCallback(MojoSumHugeint);
	}
	return r;
}
static unique_ptr<FunctionData> MojoAvgDecimalBind(BindAggregateFunctionInput &input) {
	auto r = g_orig_avg_dec_bind(input);
	auto &fn = input.GetBoundFunction();
	auto &args = fn.GetArguments();
	if (fn.HasStateClusterUpdateCallback() && !args.empty() && IsI128(args[0]) &&
	    StateSize(fn, r.get()) == sizeof(AvgStateHugeint) && AvgLayoutIs(fn, r.get(), sizeof(uint64_t))) {
		if (!g_orig_avg_i128) g_orig_avg_i128 = fn.GetStateClusterUpdateCallback();
		fn.SetStateClusterUpdateCallback(MojoAvgHugeint);
	}
	return r;
}

// min/max are registered as ANY->ANY with a bind callback; the concrete per-type
// cluster_update is produced at bind time. So we wrap the bind: run the original
// (which resolves the concrete function), then swap its double/float cluster_update.
static bind_aggregate_function_t g_orig_min_bind = nullptr, g_orig_max_bind = nullptr;

template <bool IS_MIN>
static unique_ptr<FunctionData> MojoMinMaxBind(BindAggregateFunctionInput &input) {
	auto r = (IS_MIN ? g_orig_min_bind : g_orig_max_bind)(input);
	auto &fn = input.GetBoundFunction();
	auto &args = fn.GetArguments();
	if (fn.HasStateClusterUpdateCallback() && !args.empty()) {
		auto ssz = StateSize(fn, r.get());
		auto &o64 = IS_MIN ? g_orig_min64 : g_orig_max64;
		auto &o32 = IS_MIN ? g_orig_min32 : g_orig_max32;
		if (IsD(args[0]) && ssz == sizeof(MinMaxD) && OptionalLayoutIs(fn, r.get(), sizeof(double))) {
			if (!o64) o64 = fn.GetStateClusterUpdateCallback();
			fn.SetStateClusterUpdateCallback(IS_MIN ? MojoMin64 : MojoMax64);
		} else if (IsF(args[0]) && ssz == sizeof(MinMaxF) && OptionalLayoutIs(fn, r.get(), sizeof(float))) {
			if (!o32) o32 = fn.GetStateClusterUpdateCallback();
			fn.SetStateClusterUpdateCallback(IS_MIN ? MojoMin32 : MojoMax32);
		}
	}
	return r;
}

static const LogicalType &Arg(const SimpleFunction &f, idx_t i) {
	return f.GetSignature().GetParameter(i).GetType();
}
static idx_t ArgCount(const SimpleFunction &f) {
	return f.GetSignature().GetParameterCount();
}

// DuckDB 2.0: catalog function sets hold immutable shared_ptr<const T> overloads.
// ApplyToFunctions replaces each overload with a modified copy, which is the
// supported way to change them in place (before any query has bound them).
static void WrapMinMaxBind(Catalog &cat, ClientContext &ctx, const char *name, bind_aggregate_function_t wrapper,
                           bind_aggregate_function_t &orig_slot) {
	auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	e.functions.ApplyToFunctions([&](AggregateFunction &f) {
		// the scalar templated overload: ANY -> ANY (skip the ANY -> ANY[] list variant)
		if (f.GetReturnType().id() == LogicalTypeId::ANY && f.GetBindCallback()) {
			orig_slot = f.GetBindCallback();
			f.SetBindCallback(wrapper);
		}
	});
}

// sum/avg over DECIMAL are bind-dispatched too (one DECIMAL overload with a bind
// callback). Swap that overload's bind so the concrete INT128 resolution is rerouted.
static void WrapDecimalAggBind(Catalog &cat, ClientContext &ctx, const char *name,
                               bind_aggregate_function_t wrapper, bind_aggregate_function_t &orig_slot) {
	auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	e.functions.ApplyToFunctions([&](AggregateFunction &f) {
		if (ArgCount(f) > 0 && Arg(f, 0).id() == LogicalTypeId::DECIMAL && f.GetBindCallback()) {
			orig_slot = f.GetBindCallback();
			f.SetBindCallback(wrapper);
		}
	});
}

static void OverrideScalar(Catalog &cat, ClientContext &ctx, const char *name, scalar_function_t wrapper,
                           scalar_function_t &orig_slot) {
	auto &e = cat.GetEntry<ScalarFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	e.functions.ApplyToFunctions([&](ScalarFunction &f) {
		if (ArgCount(f) == 1 && IsD(Arg(f, 0)) && IsD(f.GetReturnType())) {
			orig_slot = f.GetFunctionCallback();
			f.SetFunctionCallback(wrapper);
		}
	});
}

// ---------------- array distance / similarity folds ----------------
// array_distance / array_inner_product / array_cosine_* fold two arrays to a
// scalar per row via a serial single-accumulator scalar loop in stock DuckDB.
// We swap the per-overload `function` pointer for a wrapper that mirrors
// ArrayGenericFold exactly (a NULL row gives NULL, a NULL child element throws,
// constant-vector result when count==1) and only swaps the inner reduction for a
// SIMD kernel. The caller-side POST transform derives negative_inner_product
// (-x) and cosine_distance (1 - x) from the dot / cosine_sim kernels.
enum class FoldPost { IDENTITY, ONE_MINUS, NEGATE };

template <class T, T (*KERNEL)(const T *, const T *, int64_t), FoldPost POST>
static void MojoArrayFold(DataChunk &args, ExpressionState &state, Vector &result) {
	const auto &lstate = state.Cast<ExecuteFunctionState>();
	const auto &expr = lstate.expr.Cast<BoundFunctionExpression>();
	const auto &func_name = expr.Function().GetName();

	const auto count = args.size();
	auto &lhs_child = ArrayVector::GetChild(args.data[0]);
	auto &rhs_child = ArrayVector::GetChild(args.data[1]);

	const auto &lhs_child_validity = FlatVector::Validity(lhs_child);
	const auto &rhs_child_validity = FlatVector::Validity(rhs_child);

	UnifiedVectorFormat lhs_format;
	UnifiedVectorFormat rhs_format;
	args.data[0].ToUnifiedFormat(lhs_format);
	args.data[1].ToUnifiedFormat(rhs_format);

	auto lhs_data = FlatVector::GetData<T>(lhs_child);
	auto rhs_data = FlatVector::GetData<T>(rhs_child);
	auto res_data = FlatVector::GetDataMutable<T>(result);

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
static bool ArrayChildFlat(const Vector &v) {
	return ArrayVector::GetChild(v).GetVectorType() == VectorType::FLAT_VECTOR;
}

// scalar_function_t is a std::function in DuckDB 2.0, so the wrapper can capture the
// original overload and fall back to it when an array child is not a FLAT vector.
static scalar_function_t WithArrayFallback(scalar_function_t fast, scalar_function_t orig) {
	return [fast, orig](DataChunk &args, ExpressionState &state, Vector &result) {
		if (ArrayChildFlat(args.data[0]) && ArrayChildFlat(args.data[1])) {
			fast(args, state, result);
		} else {
			orig(args, state, result);
		}
	};
}

static void OverrideArrayFold(Catalog &cat, ClientContext &ctx, const char *name, scalar_function_t f32_wrap,
                              scalar_function_t f64_wrap) {
	auto &e = cat.GetEntry<ScalarFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
	e.functions.ApplyToFunctions([&](ScalarFunction &f) {
		if (ArgCount(f) == 2 && Arg(f, 0).id() == LogicalTypeId::ARRAY && Arg(f, 1).id() == LogicalTypeId::ARRAY) {
			const auto child = ArrayType::GetChildType(Arg(f, 0)).id();
			if (child == LogicalTypeId::FLOAT) {
				f.SetFunctionCallback(WithArrayFallback(f32_wrap, f.GetFunctionCallback()));
			} else if (child == LogicalTypeId::DOUBLE) {
				f.SetFunctionCallback(WithArrayFallback(f64_wrap, f.GetFunctionCallback()));
			}
		}
	});
}

// ======================== fused sum/avg(transcendental) ========================
// An OptimizerExtension rewrites ungrouped sum(f(col)) / avg(f(col)) for the
// transcendental functions f into a custom aggregate that applies f and reduces
// in one pass (no intermediate vector). The CPU gain is small (about 1.1 to 1.3x;
// fusion mostly helps on the GPU), but it sets up the optimizer-rewrite code that
// is reused to route query shapes to CPU/GPU backends. Disable with
// MOJO_OVERRIDES_NO_FUSE.

struct FusedState {
	double value;
	uint64_t count;
	bool isset;
};

typedef double (*plain_k_t)(const double *, int64_t);
typedef void (*masked_k_t)(const double *, const uint64_t *, int64_t, double *, int64_t *);
typedef double (*scalar_f_t)(double);

static double sf_sqrt(double x) { return std::sqrt(x); }
static double sf_sin(double x) { return std::sin(x); }
static double sf_cos(double x) { return std::cos(x); }
static double sf_ln(double x) { return std::log(x); }
static double sf_exp(double x) { return std::exp(x); }
static double sf_log10(double x) { return std::log10(x); }

template <plain_k_t PLAIN, masked_k_t MASKED, scalar_f_t SF, bool IS_AVG>
struct FusedAgg {
	static idx_t Size(AggregateStateInput &) { return sizeof(FusedState); }
	static void Init(AggregateStateInput &, data_ptr_t *states, idx_t count) {
		for (idx_t i = 0; i < count; i++) {
			auto &st = *reinterpret_cast<FusedState *>(states[i]);
			st.value = 0;
			st.count = 0;
			st.isset = false;
		}
	}
	// cluster_update: one run over rows 0..n-1 (ungrouped) takes the SIMD path; any
	// other run shape is applied row by row.
	static void ClusterUpdate(Vector in[], AggregateInputData &, idx_t, const ClusteredAggr &cl, idx_t n) {
		auto sp = SingleRunState(cl, n);
		if (!sp) {
			UnifiedVectorFormat id;
			in[0].ToUnifiedFormat(id);
			auto d = UnifiedVectorFormat::GetData<double>(id);
			for (auto &run : cl.runs()) {
				auto &st = *reinterpret_cast<FusedState *>(run.state);
				for (idx_t j = 0; j < run.count; j++) {
					auto ix = id.sel->get_index(run.sel ? run.sel[j] : j);
					if (!id.validity.RowIsValid(ix)) { continue; }
					st.value += SF(d[ix]);
					st.count++;
					st.isset = true;
				}
			}
			return;
		}
		auto &st = *reinterpret_cast<FusedState *>(sp);
		if (n > 0 && in[0].GetVectorType() == VectorType::FLAT_VECTOR) {
			auto d = FlatVector::GetData<double>(in[0]);
			if (FlatVector::Validity(in[0]).CannotHaveNull()) {
				st.value += PLAIN(d, (int64_t)n);
				st.count += n;
				st.isset = true;
				return;
			}
			double s;
			int64_t c;
			MASKED(d, ValidWords(in[0]), (int64_t)n, &s, &c);
			st.value += s;
			st.count += (uint64_t)c;
			if (c > 0) { st.isset = true; }
			return;
		}
		UnifiedVectorFormat id;
		in[0].ToUnifiedFormat(id);
		auto d = UnifiedVectorFormat::GetData<double>(id);
		for (idx_t i = 0; i < n; i++) {
			auto ix = id.sel->get_index(i);
			if (!id.validity.RowIsValid(ix)) { continue; }
			st.value += SF(d[ix]);
			st.count++;
			st.isset = true;
		}
	}
	static void Update(Vector in[], AggregateInputData &, idx_t, Vector &states, idx_t count) {
		UnifiedVectorFormat id, sd;
		in[0].ToUnifiedFormat(id);
		states.ToUnifiedFormat(sd);
		auto d = UnifiedVectorFormat::GetData<double>(id);
		auto sp = UnifiedVectorFormat::GetData<data_ptr_t>(sd);
		for (idx_t i = 0; i < count; i++) {
			auto ix = id.sel->get_index(i);
			if (!id.validity.RowIsValid(ix)) { continue; }
			auto &st = *reinterpret_cast<FusedState *>(sp[sd.sel->get_index(i)]);
			st.value += SF(d[ix]);
			st.count++;
			st.isset = true;
		}
	}
	static void Combine(Vector &state, Vector &combined, AggregateInputData &, idx_t count) {
		UnifiedVectorFormat sd;
		state.ToUnifiedFormat(sd);
		auto sp = UnifiedVectorFormat::GetData<data_ptr_t>(sd);
		auto cp = FlatVector::GetData<data_ptr_t>(combined);
		for (idx_t i = 0; i < count; i++) {
			auto &s = *reinterpret_cast<FusedState *>(sp[sd.sel->get_index(i)]);
			auto &c = *reinterpret_cast<FusedState *>(cp[i]);
			c.value += s.value;
			c.count += s.count;
			c.isset = c.isset || s.isset;
		}
	}
	static void Finalize(Vector &state, AggregateFinalizeInputData &, Vector &result, idx_t count, idx_t offset) {
		UnifiedVectorFormat sd;
		state.ToUnifiedFormat(sd);
		auto sp = UnifiedVectorFormat::GetData<data_ptr_t>(sd);
		auto r = FlatVector::GetDataMutable<double>(result);
		auto &rm = FlatVector::ValidityMutable(result);
		for (idx_t i = 0; i < count; i++) {
			auto &st = *reinterpret_cast<FusedState *>(sp[sd.sel->get_index(i)]);
			idx_t ri = i + offset;
			if (IS_AVG) {
				if (st.count == 0) {
					rm.SetInvalid(ri);
				} else {
					r[ri] = st.value / (double)st.count;
				}
			} else {
				if (!st.isset) {
					rm.SetInvalid(ri);
				} else {
					r[ri] = st.value;
				}
			}
		}
	}
	static AggregateFunction Get() {
		return AggregateFunction({LogicalType::DOUBLE}, LogicalType::DOUBLE, Size, Init, Update, Combine, Finalize,
		                         FunctionNullHandling::DEFAULT_NULL_HANDLING, ClusterUpdate);
	}
};

static bool IsFusableTranscendental(const std::string &f) {
	return f == "sqrt" || f == "sin" || f == "cos" || f == "ln" || f == "exp" || f == "log10";
}

// Build the fused aggregate, taking ownership of `inner` (the transcendental's
// argument). Only called when IsFusableTranscendental(fname), so it always returns
// a result.
static unique_ptr<Expression> BuildFusedAgg(const std::string &fname, bool is_avg, unique_ptr<Expression> inner,
                                            const std::string &alias) {
#define MOJO_FUSE_CASE(NM, PLAIN, MASKED, SF)                                                                          \
	if (fname == NM) {                                                                                                 \
		auto fused = is_avg ? FusedAgg<PLAIN, MASKED, SF, true>::Get() : FusedAgg<PLAIN, MASKED, SF, false>::Get();    \
		fused.SetName(Identifier(std::string(is_avg ? "__mojo_favg_" : "__mojo_fsum_") + NM));                                 \
		vector<unique_ptr<Expression>> nch;                                                                            \
		nch.push_back(std::move(inner));                                                                               \
		auto repl = make_uniq<BoundAggregateExpression>(BoundAggregateFunction(std::move(fused)), std::move(nch),      \
		                                                nullptr, nullptr, AggregateType::NON_DISTINCT);                \
		repl->SetAlias(Identifier(alias));                                                                             \
		return repl;                                                                                                  \
	}
	MOJO_FUSE_CASE("sqrt", mojo_fsum_sqrt_f64, mojo_fsum_sqrt_f64_masked, sf_sqrt)
	MOJO_FUSE_CASE("sin", mojo_fsum_sin_f64, mojo_fsum_sin_f64_masked, sf_sin)
	MOJO_FUSE_CASE("cos", mojo_fsum_cos_f64, mojo_fsum_cos_f64_masked, sf_cos)
	MOJO_FUSE_CASE("ln", mojo_fsum_ln_f64, mojo_fsum_ln_f64_masked, sf_ln)
	MOJO_FUSE_CASE("exp", mojo_fsum_exp_f64, mojo_fsum_exp_f64_masked, sf_exp)
	MOJO_FUSE_CASE("log10", mojo_fsum_log10_f64, mojo_fsum_log10_f64_masked, sf_log10)
#undef MOJO_FUSE_CASE
	return nullptr;
}

static void TryRewriteFusedAgg(unique_ptr<Expression> &expr) {
	if (expr->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return; }
	auto &agg = expr->Cast<BoundAggregateExpression>();
	if (agg.IsDistinct() || agg.GetFilter() || agg.GetOrderBys()) { return; }
	if (agg.GetChildren().size() != 1) { return; }
	const std::string agg_name = agg.Function().GetName().GetIdentifierName();
	bool is_avg = agg_name == "avg";
	if (agg_name != "sum" && !is_avg) { return; }
	if (agg.GetReturnType().id() != LogicalTypeId::DOUBLE) { return; }
	auto &child = *agg.GetChildrenMutable()[0];
	if (child.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) { return; }
	auto &fn = child.Cast<BoundFunctionExpression>();
	if (fn.GetChildren().size() != 1) { return; }
	if (fn.GetReturnType().id() != LogicalTypeId::DOUBLE) { return; }
	if (fn.GetChildren()[0]->GetReturnType().id() != LogicalTypeId::DOUBLE) { return; }
	const std::string fn_name = fn.Function().GetName().GetIdentifierName();
	if (!IsFusableTranscendental(fn_name)) { return; }
	auto repl = BuildFusedAgg(fn_name, is_avg, std::move(fn.GetChildrenMutable()[0]), agg.GetAlias().GetIdentifierName());
	if (repl) { expr = std::move(repl); }
}

static void FuseWalk(LogicalOperator &op) {
	if (op.type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
		auto &agg = op.Cast<LogicalAggregate>();
		if (agg.groups.empty()) {  // ungrouped only (the simple_update fast path)
			for (auto &e : agg.expressions) { TryRewriteFusedAgg(e); }
		}
	}
	for (auto &c : op.children) { FuseWalk(*c); }
}

static void MojoFuseOptimize(OptimizerExtensionInput &, unique_ptr<LogicalOperator> &plan) {
	if (plan) { FuseWalk(*plan); }
}

// ============================ mojo_knn table function ============================
// Blocked multi-query brute-force kNN over two FLOAT[K] ARRAY columns:
//   SELECT * FROM mojo_knn('emb','v','queries','qv', 10, metric:='cosine');
// returns (query_rowid BIGINT, rowid BIGINT, dist FLOAT). query_rowid / rowid are
// the 0-based positional row indices in the query / embedding tables. It runs on
// the CPU with SIMD and has no extra dependencies. It covers the batch
// (multi-query) case that the single-row array overrides can't speed up. Results
// are exact except for floating-point ties at rank k.

static const int64_t MOJO_KNN_KMAX = 4096;

static void MaterializeFloatArrayColumn(ClientContext &context, const std::string &table, const std::string &column,
                                        const char *who, std::vector<float> &host, idx_t &K, idx_t &n_rows) {
	Connection con(*context.db);
	auto res = con.Query("SELECT " + column + " FROM " + table);
	if (res->HasError()) { throw InvalidInputException(std::string(who) + ": " + res->GetError()); }
	if (res->GetTypes()[0].id() != LogicalTypeId::ARRAY) {
		throw InvalidInputException(std::string(who) + ": column must be FLOAT[K] (ARRAY), got " +
		                            res->GetTypes()[0].ToString());
	}
	if (ArrayType::GetChildType(res->GetTypes()[0]).id() != LogicalTypeId::FLOAT) {
		throw InvalidInputException(std::string(who) + ": array element type must be FLOAT");
	}
	K = ArrayType::GetSize(res->GetTypes()[0]);
	n_rows = 0;
	while (true) {
		auto chunk = res->Fetch();
		if (!chunk || chunk->size() == 0) { break; }
		auto n = chunk->size();
		chunk->data[0].Flatten(n);
		if (!FlatVector::Validity(chunk->data[0]).CannotHaveNull()) {
			throw InvalidInputException(std::string(who) + ": NULL vectors are not supported");
		}
		auto &child = ArrayVector::GetChild(chunk->data[0]);
		const float *cd = FlatVector::GetData<float>(child);
		host.insert(host.end(), cd, cd + n * K);
		n_rows += n;
	}
}

struct MojoKnnBindData : public TableFunctionData {
	std::vector<int64_t> query_rowids;
	std::vector<int64_t> ids;
	std::vector<float> dists;
	idx_t n_emitted = 0;
};
struct MojoKnnState : public GlobalTableFunctionState {
	idx_t offset = 0;
};

static unique_ptr<FunctionData> MojoKnnBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<Identifier> &names) {
	auto emb_table = input.inputs[0].GetValue<string>();
	auto emb_col = input.inputs[1].GetValue<string>();
	auto q_table = input.inputs[2].GetValue<string>();
	auto q_col = input.inputs[3].GetValue<string>();
	auto k = input.inputs[4].GetValue<int64_t>();
	if (k < 1 || k > MOJO_KNN_KMAX) {
		throw InvalidInputException("mojo_knn: k must be in [1, %lld], got %lld", (long long)MOJO_KNN_KMAX,
		                            (long long)k);
	}

	int metric = 0;  // 0=cosine 1=l2 2=ip
	auto mp = input.named_parameters.find("metric");
	if (mp != input.named_parameters.end() && !mp->second.IsNull()) {
		auto m = StringUtil::Lower(mp->second.GetValue<string>());
		if (m == "cosine" || m == "array_cosine_distance") {
			metric = 0;
		} else if (m == "l2" || m == "euclidean" || m == "array_distance") {
			metric = 1;
		} else if (m == "ip" || m == "inner_product" || m == "dot" || m == "array_negative_inner_product") {
			metric = 2;
		} else {
			throw InvalidInputException("mojo_knn: unknown metric '%s' (expected 'cosine', 'l2', or 'ip')", m);
		}
	}

	std::vector<float> emb, qry;
	idx_t eK = 0, N = 0, qK = 0, M = 0;
	MaterializeFloatArrayColumn(context, emb_table, emb_col, "mojo_knn(emb)", emb, eK, N);
	MaterializeFloatArrayColumn(context, q_table, q_col, "mojo_knn(query)", qry, qK, M);
	if (eK != qK) {
		throw InvalidInputException("mojo_knn: query dim %lld != emb dim %lld", (long long)qK, (long long)eK);
	}

	return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::FLOAT};
	names = {"query_rowid", "rowid", "dist"};
	auto bd = make_uniq<MojoKnnBindData>();
	if (N == 0 || M == 0 || eK == 0) { return std::move(bd); }

	// Per-vector norms: L2 norm for cosine, squared norm for l2, unused (0) for ip.
	std::vector<float> enrm(N, 0.0f), qnrm(M, 0.0f);
	if (metric != 2) {
		for (idx_t i = 0; i < N; i++) {
			float ss = mojo_array_dot_f32(emb.data() + i * eK, emb.data() + i * eK, (int64_t)eK);
			enrm[i] = metric == 0 ? std::sqrt(ss) : ss;
		}
		for (idx_t i = 0; i < M; i++) {
			float ss = mojo_array_dot_f32(qry.data() + i * qK, qry.data() + i * qK, (int64_t)qK);
			qnrm[i] = metric == 0 ? std::sqrt(ss) : ss;
		}
	}

	std::vector<int64_t> out_ids((size_t)M * (size_t)k);
	std::vector<float> out_dists((size_t)M * (size_t)k);
	auto run = metric == 0 ? mojo_knn_cosine_f32 : (metric == 1 ? mojo_knn_l2_f32 : mojo_knn_ip_f32);
	run(qry.data(), qnrm.data(), (int64_t)M, emb.data(), enrm.data(), (int64_t)N, (int64_t)eK, k, out_ids.data(),
	    out_dists.data());

	for (idx_t mi = 0; mi < M; mi++) {
		for (idx_t j = 0; j < (idx_t)k; j++) {
			idx_t pos = mi * (idx_t)k + j;
			if (out_ids[pos] < 0) { continue; }  // padding when N < k
			bd->query_rowids.push_back((int64_t)mi);
			bd->ids.push_back(out_ids[pos]);
			bd->dists.push_back(out_dists[pos]);
		}
	}
	bd->n_emitted = bd->ids.size();
	return std::move(bd);
}

static unique_ptr<GlobalTableFunctionState> MojoKnnInit(ClientContext &, TableFunctionInitInput &) {
	return make_uniq<MojoKnnState>();
}

static void MojoKnnFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
	auto &bd = data.bind_data->Cast<MojoKnnBindData>();
	auto &gs = data.global_state->Cast<MojoKnnState>();
	idx_t n = MinValue<idx_t>(bd.n_emitted - gs.offset, STANDARD_VECTOR_SIZE);
	if (n == 0) { output.SetCardinality(0); return; }
	auto qrow = FlatVector::GetDataMutable<int64_t>(output.data[0]);
	auto rowid = FlatVector::GetDataMutable<int64_t>(output.data[1]);
	auto dist = FlatVector::GetDataMutable<float>(output.data[2]);
	for (idx_t i = 0; i < n; i++) {
		qrow[i] = bd.query_rowids[gs.offset + i];
		rowid[i] = bd.ids[gs.offset + i];
		dist[i] = bd.dists[gs.offset + i];
	}
	output.SetCardinality(n);
	gs.offset += n;
}

static void RegisterMojoKnn(Catalog &cat, ClientContext &ctx) {
	TableFunction tf("mojo_knn",
	                 {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
	                  LogicalType::BIGINT},
	                 MojoKnnFunc, MojoKnnBind, MojoKnnInit);
	tf.GetSignature().AddKeywordOnly("metric", LogicalType::VARCHAR, Value(LogicalType::VARCHAR));
	CreateTableFunctionInfo info(tf);
	info.on_conflict = OnCreateConflict::IGNORE_ON_CONFLICT;
	cat.CreateTableFunction(ctx, info);
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

	// Register the fused sum/avg(transcendental) optimizer rewrite (on unless disabled).
	if (!std::getenv("MOJO_OVERRIDES_NO_FUSE")) {
		OptimizerExtension ext;
		ext.optimize_function = MojoFuseOptimize;
		OptimizerExtension::Register(DBConfig::GetConfig(db), std::move(ext));
	}

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

		// sum/avg have concrete per-type overloads in the catalog, so override simple_update directly.
		auto agg = [&](const char *name, aggregate_cluster_update_t wrap, aggregate_cluster_update_t &orig,
		               size_t mirror, std::function<bool(const BoundAggregateFunction &)> layout_ok) {
			auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
			e.functions.ApplyToFunctions([&](AggregateFunction &f) {
				if (ArgCount(f) != 1 || !IsD(Arg(f, 0)) || !f.HasStateClusterUpdateCallback()) { return; }
				BoundAggregateFunction bf(f);
				if (StateSize(bf, nullptr) == mirror && layout_ok(bf)) {
					orig = f.GetStateClusterUpdateCallback();
					f.SetStateClusterUpdateCallback(wrap);
				}
			});
		};
		agg("sum", MojoSum, g_orig_sum, sizeof(SumStateM),
		    [](const BoundAggregateFunction &bf) { return OptionalLayoutIs(bf, nullptr, sizeof(double)); });
		agg("avg", MojoAvg, g_orig_avg, sizeof(AvgStateM),
		    [](const BoundAggregateFunction &bf) { return AvgLayoutIs(bf, nullptr, sizeof(uint64_t)); });

		// HUGEINT sum/avg: concrete per-type overload, so override simple_update directly.
		auto aggI128 = [&](const char *name, aggregate_cluster_update_t wrap, aggregate_cluster_update_t &orig,
		                   size_t mirror, std::function<bool(const BoundAggregateFunction &)> layout_ok) {
			auto &e = cat.GetEntry<AggregateFunctionCatalogEntry>(ctx, DEFAULT_SCHEMA, name);
			e.functions.ApplyToFunctions([&](AggregateFunction &f) {
				if (ArgCount(f) != 1 || Arg(f, 0).id() != LogicalTypeId::HUGEINT ||
				    !f.HasStateClusterUpdateCallback()) {
					return;
				}
				BoundAggregateFunction bf(f);
				if (StateSize(bf, nullptr) == mirror && layout_ok(bf)) {
					if (!orig) orig = f.GetStateClusterUpdateCallback();
					f.SetStateClusterUpdateCallback(wrap);
				}
			});
		};
		aggI128("sum", MojoSumHugeint, g_orig_sum_i128, sizeof(SumStateHugeint),
		        [](const BoundAggregateFunction &bf) { return OptionalLayoutIs(bf, nullptr, sizeof(hugeint_t)); });
		aggI128("avg", MojoAvgHugeint, g_orig_avg_i128, sizeof(AvgStateHugeint),
		        [](const BoundAggregateFunction &bf) { return AvgLayoutIs(bf, nullptr, sizeof(uint64_t)); });

		// DECIMAL(19..38) sum/avg resolve to the same INT128 state at bind, so wrap the bind.
		WrapDecimalAggBind(cat, ctx, "sum", MojoSumDecimalBind, g_orig_sum_dec_bind);
		WrapDecimalAggBind(cat, ctx, "avg", MojoAvgDecimalBind, g_orig_avg_dec_bind);

		// min/max are bind-dispatched (ANY->ANY), so wrap the bind to swap f64/f32 simple_update.
		WrapMinMaxBind(cat, ctx, "min", MojoMinMaxBind<true>, g_orig_min_bind);
		WrapMinMaxBind(cat, ctx, "max", MojoMinMaxBind<false>, g_orig_max_bind);

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

		// blocked multi-query kNN table function.
		RegisterMojoKnn(cat, ctx);
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
