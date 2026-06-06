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

// ---------------- scalar (double -> double) wrappers via X-macro ----------------
#define MOJO_SCALAR(NAME)                                                                                              \
	static scalar_function_t g_orig_##NAME = nullptr;                                                                  \
	static mojo_un64_t g_k_##NAME = nullptr;                                                                           \
	static void Mojo_##NAME(DataChunk &args, ExpressionState &state, Vector &result) {                                 \
		auto &in = args.data[0];                                                                                       \
		if (g_k_##NAME && in.GetVectorType() == VectorType::FLAT_VECTOR && FlatVector::Validity(in).AllValid()) {      \
			result.SetVectorType(VectorType::FLAT_VECTOR);                                                             \
			g_k_##NAME(FlatVector::GetData<double>(in), FlatVector::GetData<double>(result), (int64_t)args.size());    \
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

static void MojoSum(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_sum && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<SumStateM *>(sp);
		st.value += g_k_sum(FlatVector::GetData<double>(in[0]), (int64_t)n);
		st.isset = true;
		return;
	}
	g_orig_sum(in, aid, ic, sp, n);
}
static void MojoAvg(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_sum && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<AvgStateM *>(sp);
		st.value += g_k_sum(FlatVector::GetData<double>(in[0]), (int64_t)n);
		st.count += n;
		return;
	}
	g_orig_avg(in, aid, ic, sp, n);
}
static void MojoMin64(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_min64 && n > 0 && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		double c = g_k_min64(FlatVector::GetData<double>(in[0]), (int64_t)n);
		if (!st.isset || c < st.value) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_min64(in, aid, ic, sp, n);
}
static void MojoMax64(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_max64 && n > 0 && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<MinMaxD *>(sp);
		double c = g_k_max64(FlatVector::GetData<double>(in[0]), (int64_t)n);
		if (!st.isset || c > st.value) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_max64(in, aid, ic, sp, n);
}
static void MojoMin32(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_min32 && n > 0 && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		float c = g_k_min32(FlatVector::GetData<float>(in[0]), (int64_t)n);
		if (!st.isset || c < st.value) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_min32(in, aid, ic, sp, n);
}
static void MojoMax32(Vector in[], AggregateInputData &aid, idx_t ic, data_ptr_t sp, idx_t n) {
	if (g_k_max32 && n > 0 && FlatValid(in[0])) {
		auto &st = *reinterpret_cast<MinMaxF *>(sp);
		float c = g_k_max32(FlatVector::GetData<float>(in[0]), (int64_t)n);
		if (!st.isset || c > st.value) { st.value = c; st.isset = true; }
		return;
	}
	g_orig_max32(in, aid, ic, sp, n);
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

		// min/max are bind-dispatched (ANY->ANY) → wrap the bind to swap f64/f32 simple_update.
		WrapMinMaxBind(cat, ctx, "min", MojoMinBind, g_orig_min_bind);
		WrapMinMaxBind(cat, ctx, "max", MojoMaxBind, g_orig_max_bind);
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
