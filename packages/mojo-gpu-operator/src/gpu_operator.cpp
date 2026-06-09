// mojo-gpu-operator: transparent GPU offload for DuckDB.
//
// An OptimizerExtension matches eligible plan shapes (array_cosine_distance, and
// TPC-H Q1/Q3/Q5/Q6/Q14) and rewrites them to custom GPU PhysicalOperators backed
// by Mojo kernels (gpu_kernels.mojo, linked in over a C-ABI). The same engines are
// also exposed as `gpu_*` table functions. Translate-or-fallback: anything that
// doesn't match, or any runtime GPU error, runs on stock DuckDB CPU. See README.md.

#include "duckdb.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "duckdb/optimizer/optimizer_extension.hpp"
#include "duckdb/planner/operator/logical_extension_operator.hpp"
#include "duckdb/planner/operator/logical_projection.hpp"
#include "duckdb/planner/operator/logical_aggregate.hpp"
#include "duckdb/planner/operator/logical_get.hpp"
#include "duckdb/planner/operator/logical_comparison_join.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/catalog/catalog_entry/table_catalog_entry.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/planner/filter/constant_filter.hpp"
#include "duckdb/planner/filter/conjunction_filter.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/execution/physical_plan_generator.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/types/date.hpp"

#include "raw_plan.h"

#include <algorithm>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

// ---------------------------------------------------------------------------
// Mojo kernel C-ABI (gpu_kernels.mojo -> gpu_kernels.o, linked into this .so).
// init returns the handle as an integer address (0 == failure).
// ---------------------------------------------------------------------------
extern "C" {
// Force the one-time (~32 ms) shared DeviceContext creation now, so it's paid
// at extension LOAD time instead of on the first pin of the first query.
void mojo_gpu_ctx_init();
int64_t mojo_gpu_cosine_init(const float *q, int64_t K, int64_t capacity_rows);
int32_t mojo_gpu_cosine_run(void *handle, const float *emb, int64_t n_rows, float *out);
void mojo_gpu_cosine_free(void *handle);
// Pin-resident engine: pin the whole column once, query many times.
int64_t mojo_gpu_pin(const float *emb, int64_t n_rows, int64_t K);
int32_t mojo_gpu_pin_query(void *handle, const float *q, float *out);
void mojo_gpu_pin_free(void *handle);
// TPC-H Q6 engine: pin the 4 lineitem columns, run filter+exact-decimal-sum.
int64_t mojo_q6_pin(const int32_t *ship, const int64_t *disc, const int64_t *ext,
                    const int64_t *qty, int64_t n_rows, int32_t timing);
int32_t mojo_q6_query(void *handle, int32_t ship_lo, int32_t ship_hi, int64_t disc_lo,
                      int64_t disc_hi, int64_t qty_hi, int64_t *out);
void mojo_q6_free(void *handle);
// Streaming pin (Option B): C++ streams the result, Mojo owns buffer alloc/fill.
int64_t mojo_q6_pin_begin(int64_t n_rows);
int32_t mojo_q6_pin_chunk(void *handle, const int32_t *ship, const int64_t *disc,
                          const int64_t *ext, const int64_t *qty, int64_t n,
                          int64_t offset);
int32_t mojo_q6_pin_end(void *handle);
// TPC-H Q1 engine: pin 6 columns (group id + qty/ext/disc/tax/shipdate), run a
// grouped exact-decimal aggregation. `out` holds n_groups*12 int64 (per group,
// 6 int128 quantities as low,high limb pairs).
int64_t mojo_q1_pin(const uint8_t *gid, const int64_t *qty, const int64_t *ext,
                    const int64_t *disc, const int64_t *tax, const int32_t *ship,
                    int64_t n_rows, int64_t n_groups);
int32_t mojo_q1_query(void *handle, int32_t ship_hi, int64_t *out);
void mojo_q1_free(void *handle);
// Q1 pinned-HostBuffer staging: alloc the 6 resident DeviceBuffers + 6 pinned
// HostBuffers (n_groups unknown yet); C++ computes gid per row into gid_h and
// memcpys the 5 numeric/date columns; mojo_q1_pin_upload(handle, n_groups) does
// one DMA per column.
int64_t mojo_q1_pin_alloc(int64_t n_rows, uint8_t **gid_h, int64_t **qty_h,
                          int64_t **ext_h, int64_t **disc_h, int64_t **tax_h,
                          int32_t **ship_h);
int32_t mojo_q1_pin_upload(void *handle, int64_t n_groups, int32_t timing);
// TPC-H Q14 engine: GPU hash-probe FK join (lineitem -> part) + probe-side
// exact-decimal aggregation. The C++ side builds the open-addressing hash table
// (host) and passes keys[]+promo[] (size = pow2) plus the 4 probe columns.
int64_t mojo_q14_pin(const int64_t *ht_keys, const uint8_t *ht_promo, int64_t ht_size,
                     const int64_t *lpartkey, const int32_t *ship, const int64_t *ext,
                     const int64_t *disc, int64_t n_rows);
int32_t mojo_q14_query(void *handle, int32_t ship_lo, int32_t ship_hi,
                       int64_t *out_total, int64_t *out_promo);
void mojo_q14_free(void *handle);
// Q14 pinned-HostBuffer staging: upload the small hash table immediately, return
// pinned host pointers for the 4 big probe columns (C++ memcpys chunks in), then
// mojo_q14_pin_upload does one DMA per probe column.
int64_t mojo_q14_pin_alloc(const int64_t *ht_keys, const uint8_t *ht_promo,
                           int64_t ht_size, int64_t n_rows, int64_t **lpk_h,
                           int32_t **ship_h, int64_t **ext_h, int64_t **disc_h);
int32_t mojo_q14_pin_upload(void *handle, int32_t timing);
// TPC-H Q3 engine: GPU multi-way-join probe over lineitem + per-order revenue
// accumulation. The C++ side collapses the customer<-orders<-lineitem joins on
// the host into a dense order_pass[o_orderkey] flag, pins it + the 4 probe
// columns, runs the GPU probe (join+filter+exact decimal product), and the
// kernel lib sums per-order on the host (Apple GPU lacks int64 atomics) into a
// dense int64 accumulator (size max_orderkey+1).
int64_t mojo_q3_pin(const uint8_t *order_pass, const int64_t *lorderkey,
                    const int32_t *ship, const int64_t *ext, const int64_t *disc,
                    int64_t n_rows, int64_t max_orderkey);
int32_t mojo_q3_query(void *handle, int32_t ship_cutoff, int64_t *out_revenue);
void mojo_q3_free(void *handle);
// Q3 v2: on-GPU high-cardinality group-by via sort + segmented reduction (one
// warp per order segment, no int64 atomics, no host per-order sum). The C++ side
// materializes lineitem ORDERED BY l_orderkey, builds the distinct-orderkey list
// (seg_key) + seg_offset[] (start row of each order, seg_offset[n_seg]=n_rows) in
// one pass, pins them + order_pass + the sorted probe columns. query2 returns one
// int64 revenue per segment (scale-4); the host maps s -> orderkey via seg_key.
int64_t mojo_q3_pin2(const uint8_t *order_pass, const int64_t *seg_offset,
                     const int64_t *seg_key, const int32_t *ship, const int64_t *ext,
                     const int64_t *disc, int64_t n_rows, int64_t n_seg,
                     int64_t max_orderkey);
int32_t mojo_q3_query2(void *handle, int32_t ship_cutoff, int64_t *out_seg_rev);
void mojo_q3_free2(void *handle);
// Q3 v2 pinned-HostBuffer staging (two-phase: n_seg/max_orderkey are discovered
// while scanning the sorted lineitem stream). pin_alloc2 returns pinned host
// pointers for the 3 sorted probe columns (ship/ext/disc); l_orderkey stays on
// the host to build the segmentation; pin_upload2 allocs op/soff/skey/srev and
// uploads them + the 3 pinned columns.
int64_t mojo_q3_pin_alloc2(int64_t n_rows, int32_t **ship_h, int64_t **ext_h,
                           int64_t **disc_h);
int32_t mojo_q3_pin_upload2(void *handle, const uint8_t *order_pass,
                            const int64_t *seg_offset, const int64_t *seg_key,
                            int64_t n_seg, int64_t max_orderkey, int32_t timing);
// TPC-H Q5 engine: GPU 6-table-join probe over lineitem + per-nation exact
// aggregation. The C++ side collapses the 5 dimension joins on the host into
// dense per-key arrays (order_pass / order_cust_nation by o_orderkey,
// supp_nation by l_suppkey, nation_in_asia by nationkey; the o_orderdate filter
// is baked into order_pass), pins them + the 4 probe columns, runs the GPU probe
// (join + correlated condition cn==sn + ASIA filter + exact decimal product)
// and groups by nation via per-block partials (no int64 atomics on Apple GPU),
// reduced to int128 per nation on the host. out_revenue holds n_nations int128
// values as low,high limb pairs.
int64_t mojo_q5_pin(const uint8_t *order_pass, const int32_t *order_cust_nation,
                    const int32_t *supp_nation, const uint8_t *nation_in_asia,
                    const int64_t *lorderkey, const int64_t *lsuppkey,
                    const int64_t *ext, const int64_t *disc, int64_t n_rows,
                    int64_t max_orderkey, int64_t max_suppkey, int64_t n_nations);
int32_t mojo_q5_query(void *handle, int64_t *out_revenue);
void mojo_q5_free(void *handle);
// Q5 pinned-HostBuffer staging: upload the small dense dimension lookups
// immediately, return pinned host pointers for the 4 big probe columns
// (l_orderkey/l_suppkey/ext/disc), then mojo_q5_pin_upload does one DMA per col.
int64_t mojo_q5_pin_alloc(const uint8_t *order_pass, const int32_t *order_cust_nation,
                          const int32_t *supp_nation, const uint8_t *nation_in_asia,
                          int64_t n_rows, int64_t max_orderkey, int64_t max_suppkey,
                          int64_t n_nations, int64_t **lok_h, int64_t **lsk_h,
                          int64_t **ext_h, int64_t **disc_h);
int32_t mojo_q5_pin_upload(void *handle, int32_t timing);

// RawPlan -> descriptor boundary (descriptor.mojo). Stage-1 shadow validation:
// C++ flattens a matched LogicalAggregate subtree into the RawPlan wire form and
// Mojo parses+classifies it. The handle is an opaque int64 pointer (0 = reject).
int64_t mojo_gpu_build_descriptor(const int64_t *tape, int64_t tape_len,
                                  const uint8_t *blob, int64_t blob_len);
void mojo_gpu_desc_free(void *handle);
int64_t mojo_gpu_desc_kind(void *handle);
int64_t mojo_gpu_desc_strategy(void *handle);
int64_t mojo_gpu_desc_n_dims(void *handle);
int64_t mojo_gpu_desc_n_aggs(void *handle);
int64_t mojo_gpu_desc_fact_table(void *handle, uint8_t *out, int64_t cap);

// Stage-2 execution shuttle (descriptor drives execution). See RAW_PLAN_CONTRACT.md.
int64_t mojo_gpu_desc_group_index(void *handle);      // IDX_NONE if ungrouped
int64_t mojo_gpu_desc_aggregate_index(void *handle);
int64_t mojo_gpu_desc_out_arity(void *handle);
int64_t mojo_gpu_desc_out_type(void *handle, int64_t i, int64_t *tag, int64_t *scale, int64_t *width);
int64_t mojo_gpu_desc_materialize_count(void *handle);
int64_t mojo_gpu_desc_materialize_sql(void *handle, int64_t i, uint8_t *out, int64_t cap); // full byte len
int64_t mojo_gpu_pin_begin(void *handle);             // 0=WARM, 1=COLD
int64_t mojo_gpu_feed_column(void *handle, int64_t req_i, int64_t col_j, void *ptr,
                             int64_t n_rows, int64_t type_tag);   // 0 ok
int64_t mojo_gpu_pin_finalize(void *handle);          // 0 ok
int64_t mojo_gpu_result_rows(void *handle);
int64_t mojo_gpu_result_i128(void *handle, int64_t row, int64_t col, int64_t *lo, int64_t *hi);
int64_t mojo_gpu_result_i64(void *handle, int64_t row, int64_t col);
double  mojo_gpu_result_f64(void *handle, int64_t row, int64_t col);
int64_t mojo_gpu_result_str(void *handle, int64_t row, int64_t col, uint8_t *out, int64_t cap);
}

namespace duckdb {

namespace {

constexpr const char *COSINE_FN = "array_cosine_distance";

// CPU fallback: cosine distance = 1 - dot/(||v|| * ||q||).
void cpu_cosine(const float *emb, idx_t n, idx_t K, const float *q, float qnorm, float *out) {
  for (idx_t r = 0; r < n; r++) {
    const float *row = emb + r * K;
    float dot = 0, na = 0;
    for (idx_t i = 0; i < K; i++) {
      float a = row[i];
      dot += a * q[i];
      na += a * a;
    }
    float denom = std::sqrt(na) * qnorm;
    out[r] = denom != 0 ? 1.0f - dot / denom : 0.0f;
  }
}

// ---------------------------------------------------------------------------
// Physical operator
// ---------------------------------------------------------------------------
struct GpuCosineGlobalState : public GlobalOperatorState {
  void *handle = nullptr;
  explicit GpuCosineGlobalState(const vector<float> &query, idx_t K) {
    handle = reinterpret_cast<void *>(
        mojo_gpu_cosine_init(query.data(), NumericCast<int64_t>(K),
                             NumericCast<int64_t>(idx_t(STANDARD_VECTOR_SIZE))));
  }
  ~GpuCosineGlobalState() override {
    if (handle) { mojo_gpu_cosine_free(handle); }
  }
};

class PhysicalGpuCosine : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalGpuCosine(PhysicalPlan &plan, vector<LogicalType> types, idx_t v_index,
                    vector<float> query, idx_t K, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        v_index(v_index), query(std::move(query)), K(K) {
    qnorm = 0;
    for (auto f : this->query) { qnorm += f * f; }
    qnorm = std::sqrt(qnorm);
  }

  idx_t v_index;
  vector<float> query;
  idx_t K;
  float qnorm;

  unique_ptr<GlobalOperatorState> GetGlobalOperatorState(ClientContext &) const override {
    return make_uniq<GpuCosineGlobalState>(query, K);
  }

  OperatorResultType Execute(ExecutionContext &, DataChunk &input, DataChunk &chunk,
                             GlobalOperatorState &gstate_p, OperatorState &) const override {
    auto &gstate = gstate_p.Cast<GpuCosineGlobalState>();
    auto n = input.size();

    // Flatten the array column so its child data is a contiguous n*K float block.
    input.data[v_index].Flatten(n);
    auto &child = ArrayVector::GetEntry(input.data[v_index]);
    const float *emb = FlatVector::GetData<float>(child);

    chunk.SetCardinality(n);
    chunk.data[0].SetVectorType(VectorType::FLAT_VECTOR);
    float *out = FlatVector::GetData<float>(chunk.data[0]);

    int32_t rc = -1;
    if (gstate.handle) {
      rc = mojo_gpu_cosine_run(gstate.handle, emb, NumericCast<int64_t>(n), out);
    }
    if (rc != 0) {
      // GPU unavailable / errored / over capacity -> CPU fallback.
      cpu_cosine(emb, n, K, query.data(), qnorm, out);
    }
    return OperatorResultType::NEED_MORE_INPUT;
  }

  bool ParallelOperator() const override { return false; }

  string GetName() const override { return "GPU_COSINE"; }
};

// ---------------------------------------------------------------------------
// Logical extension operator (sits where the matched LogicalProjection was).
// ---------------------------------------------------------------------------
class LogicalGpuCosine : public LogicalExtensionOperator {
public:
  LogicalGpuCosine(idx_t table_index, unique_ptr<Expression> cosine_expr)
      : table_index(table_index) {
    expressions.push_back(std::move(cosine_expr));
    types.push_back(LogicalType::FLOAT);
  }

  idx_t table_index;

  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(table_index, 0)};
  }

  void ResolveTypes() override { types = {LogicalType::FLOAT}; }

  string GetExtensionName() const override { return "mojo_gpu_cosine"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    // Child plan (the scan).
    auto &child_plan = planner.CreatePlan(*children[0]);

    // After column-binding resolution, the cosine expression's first child is a
    // BoundReferenceExpression (physical index of v in the input chunk); the
    // second is the constant query array.
    auto &fn = expressions[0]->Cast<BoundFunctionExpression>();
    auto &ref = fn.children[0]->Cast<BoundReferenceExpression>();
    idx_t v_index = ref.index;

    auto &cst = fn.children[1]->Cast<BoundConstantExpression>();
    auto &kids = ArrayValue::GetChildren(cst.value);
    idx_t K = kids.size();
    vector<float> query;
    query.reserve(K);
    for (auto &v : kids) { query.push_back(v.GetValue<float>()); }

    auto &op = planner.Make<PhysicalGpuCosine>(vector<LogicalType>{LogicalType::FLOAT},
                                               v_index, std::move(query), K,
                                               estimated_cardinality);
    op.children.push_back(child_plan);
    return op;
  }
};

// ---------------------------------------------------------------------------
// Pattern match + plan rewrite
// ---------------------------------------------------------------------------

// Matches a projection of exactly one expression: array_cosine_distance(<colref>,
// <constant FLOAT[K]>). Returns the cosine expression (moved out) or nullptr.
bool MatchCosineProjection(LogicalProjection &proj) {
  if (proj.expressions.size() != 1) { return false; }
  auto &e = proj.expressions[0];
  if (e->type != ExpressionType::BOUND_FUNCTION) { return false; }
  auto &fn = e->Cast<BoundFunctionExpression>();
  if (fn.function.name != COSINE_FN || fn.children.size() != 2) { return false; }
  // second arg must be a constant array
  if (fn.children[1]->type != ExpressionType::VALUE_CONSTANT) { return false; }
  auto &cst = fn.children[1]->Cast<BoundConstantExpression>();
  if (cst.value.type().id() != LogicalTypeId::ARRAY) { return false; }
  return true;
}

// Stage-1 shadow validation (defined after the join-tree helpers). Serializes a
// LogicalAggregate to the RawPlan wire form, hands it to the Mojo descriptor
// builder, and logs the classification. Pure side-effect (stderr): NEVER mutates
// the plan, so it cannot change what the optimizer emits.
void ShadowValidateAggregate(LogicalAggregate &agg);

// Generic-operator routing (defined after RawPlanBuilder/SerializeMatchedPlan
// + the LogicalGpuAgg/PhysicalGpuAgg classes). On by default; returns true and
// replaces *node with a descriptor-driven LogicalGpuAgg when the matched plan's
// descriptor kind is buildable. GPU_OP_GENERIC=off disables all GPU aggregate
// offload -> the node is left untouched and runs on stock DuckDB CPU.
bool TryRouteGeneric(unique_ptr<LogicalOperator> &node);

void OptimizeNode(unique_ptr<LogicalOperator> &node) {
  if (!node) { return; }
  // Recurse first (depth-first), so children are rewritten before parents.
  for (auto &child : node->children) { OptimizeNode(child); }

  // Shadow validation (zero behavior change): for every aggregate node, when
  // GPU_OP_SHADOW is set, build a RawPlan descriptor and log its classification
  // BEFORE the real matchers run. All exceptions swallowed; plan untouched.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY &&
      std::getenv("GPU_OP_SHADOW")) {
    try {
      ShadowValidateAggregate(node->Cast<LogicalAggregate>());
    } catch (...) {
      // never let shadow validation affect optimization
    }
  }

  // Generic GPU aggregate operator (on by default): when the node's descriptor
  // class is buildable, route it through the descriptor-driven LogicalGpuAgg.
  // GPU_OP_GENERIC=off (or a non-buildable class) is a no-op -> the node falls
  // through to stock DuckDB CPU execution (the bespoke MatchQ* path is gone).
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (TryRouteGeneric(node)) { return; }
    } catch (...) {
      // leave node untouched -> stock DuckDB CPU execution
    }
  }

  if (node->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    auto &proj = node->Cast<LogicalProjection>();
    if (MatchCosineProjection(proj)) {
      auto cosine_expr = std::move(proj.expressions[0]);
      auto repl = make_uniq<LogicalGpuCosine>(proj.table_index, std::move(cosine_expr));
      repl->children.push_back(std::move(proj.children[0]));
      repl->estimated_cardinality = proj.estimated_cardinality;
      node = std::move(repl);
    }
  }
}

// Pre/optimize hook coordination: the pre-optimizer hook disables DuckDB's
// string-dictionary compression pass (COMPRESSED_MATERIALIZATION) so grouped
// GROUP BY keys stay raw VARCHAR (no __internal_compress/decompress projections,
// which a source operator can't reproduce). The paired optimize hook restores
// the original disabled-optimizer set so the change never leaks into later CPU
// queries. We remember the original set per-thread (the binder runs both hooks
// on the same thread for one plan), keyed by context pointer for re-entrancy
// safety (the generic operator's pin self-query runs nested on the same thread).
struct SavedDisabled {
  bool valid = false;
  std::set<OptimizerType> original;
};
thread_local std::unordered_map<const ClientContext *, SavedDisabled> g_saved_disabled;

void GpuPreOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &) {
  try {
    auto &context = input.context;
    auto &opts = DBConfig::GetConfig(context).options;
    SavedDisabled saved;
    saved.valid = true;
    saved.original = opts.disabled_optimizers;
    g_saved_disabled[&context] = std::move(saved);
    // Keep Q1 group keys as raw VARCHAR so the transparent matcher sees a plain
    // LogicalAggregate(2 varchar group refs, 8 aggregates) over a filtered GET.
    opts.disabled_optimizers.insert(OptimizerType::COMPRESSED_MATERIALIZATION);
  } catch (...) {
    // If anything goes wrong, leave the optimizer config untouched.
  }
}

void GpuCosineOptimize(OptimizerExtensionInput &input, unique_ptr<LogicalOperator> &plan) {
  // Restore the original disabled-optimizer set first so our pre-hook's change
  // does not affect later queries / nested CPU work.
  try {
    auto &context = input.context;
    auto it = g_saved_disabled.find(&context);
    if (it != g_saved_disabled.end() && it->second.valid) {
      DBConfig::GetConfig(context).options.disabled_optimizers = it->second.original;
      g_saved_disabled.erase(it);
    }
  } catch (...) {
    // best-effort restore
  }
  try {
    OptimizeNode(plan);
  } catch (...) {
    // Never let a rewrite failure break the query; fall back to stock DuckDB.
  }
}

void RegisterGpuOperator(DatabaseInstance &db) {
  auto &config = DBConfig::GetConfig(db);
  OptimizerExtension ext;
  ext.pre_optimize_function = GpuPreOptimize;
  ext.optimize_function = GpuCosineOptimize;
  OptimizerExtension::Register(config, std::move(ext));
}

// ---------------------------------------------------------------------------
// gpu_cosine() table function: pin-resident column cache.
//
//   SELECT * FROM gpu_cosine('emb', 'v', [..]::FLOAT[K]);  -> (rowid, dist)
//
// The first call materializes <column> from <table>, uploads it to a resident
// GPU buffer, and caches the handle keyed by "table.column". Subsequent calls
// (any query vector) reuse the resident buffer — the upload is paid once. This
// exposes the pin-resident win from SQL, correctly under any threading (the TF
// controls its own scan), unlike folding pinning into the streaming operator.
// ---------------------------------------------------------------------------
struct PinEntry {
  void *handle;
  idx_t n_rows;
  idx_t K;
};

std::mutex g_pin_mu;
std::unordered_map<std::string, PinEntry> g_pins;  // process-lifetime cache

// Materialize + pin a column if not already cached. Returns the cache entry.
PinEntry EnsurePinned(ClientContext &context, const std::string &table, const std::string &column) {
  std::string key = table + "." + column;
  std::lock_guard<std::mutex> g(g_pin_mu);
  auto it = g_pins.find(key);
  if (it != g_pins.end()) { return it->second; }

  Connection con(*context.db);
  auto res = con.Query("SELECT " + column + " FROM " + table);
  if (res->HasError()) { throw InvalidInputException("gpu_cosine: " + res->GetError()); }
  if (res->types[0].id() != LogicalTypeId::ARRAY) {
    throw InvalidInputException("gpu_cosine: column must be FLOAT[K] (ARRAY), got " +
                                res->types[0].ToString());
  }
  idx_t K = ArrayType::GetSize(res->types[0]);

  vector<float> host;
  idx_t n_rows = 0;
  while (true) {
    auto chunk = res->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    auto &child = ArrayVector::GetEntry(chunk->data[0]);
    const float *cd = FlatVector::GetData<float>(child);
    host.insert(host.end(), cd, cd + n * K);
    n_rows += n;
  }

  void *handle = reinterpret_cast<void *>(
      mojo_gpu_pin(host.data(), NumericCast<int64_t>(n_rows), NumericCast<int64_t>(K)));
  PinEntry e{handle, n_rows, K};
  g_pins[key] = e;
  return e;
}

struct GpuCosineBindData : public TableFunctionData {
  vector<float> distances;
  idx_t n_rows = 0;
};

struct GpuCosineTFGlobalState : public GlobalTableFunctionState {
  idx_t offset = 0;
  idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> GpuCosineBind(ClientContext &context, TableFunctionBindInput &input,
                                       vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  auto &qkids = ListValue::GetChildren(input.inputs[2]);

  auto pe = EnsurePinned(context, table, column);
  if (!pe.handle) { throw InvalidInputException("gpu_cosine: GPU pin failed"); }
  if (qkids.size() != pe.K) {
    throw InvalidInputException("gpu_cosine: query length " + std::to_string(qkids.size()) +
                                " != column K " + std::to_string(pe.K));
  }
  vector<float> q;
  q.reserve(pe.K);
  for (auto &v : qkids) { q.push_back(v.GetValue<float>()); }

  auto bd = make_uniq<GpuCosineBindData>();
  bd->n_rows = pe.n_rows;
  bd->distances.resize(pe.n_rows);
  int32_t rc = mojo_gpu_pin_query(pe.handle, q.data(), bd->distances.data());
  if (rc != 0) { throw InvalidInputException("gpu_cosine: GPU query failed (rc " + std::to_string(rc) + ")"); }

  return_types = {LogicalType::BIGINT, LogicalType::FLOAT};
  names = {"rowid", "dist"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> GpuCosineInit(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<GpuCosineTFGlobalState>();
}

void GpuCosineFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuCosineBindData>();
  auto &gs = data.global_state->Cast<GpuCosineTFGlobalState>();
  idx_t n = MinValue<idx_t>(bd.n_rows - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  auto rowid = FlatVector::GetData<int64_t>(output.data[0]);
  auto dist = FlatVector::GetData<float>(output.data[1]);
  for (idx_t i = 0; i < n; i++) {
    rowid[i] = NumericCast<int64_t>(gs.offset + i);
    dist[i] = bd.distances[gs.offset + i];
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuCosineTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_cosine",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::FLOAT)},
                   GpuCosineFunc, GpuCosineBind, GpuCosineInit);
  loader.RegisterFunction(tf);
}


// Resolve a BoundColumnRef against a set of GETs (by table_index) to its table
// column name + which table it belongs to. Returns "" if it matches no GET.
struct ColResolve { std::string table_name; std::string col_name; };
ColResolve ResolveJoinColref(const BoundColumnRefExpression &ref,
                             const std::vector<LogicalGet *> &gets) {
  for (auto *g : gets) {
    if (ref.binding.table_index != g->table_index) { continue; }
    const auto &col_ids = g->GetColumnIds();
    idx_t pos = ref.binding.column_index;
    if (pos >= col_ids.size()) { return {}; }
    auto te = g->GetTable();
    return {te ? te->name : std::string(), g->GetColumnName(col_ids[pos])};
  }
  return {};
}

// True if `e` is the promo predicate over p_type: prefix(p_type,'PROMO') or
// a LIKE/~~ (p_type LIKE 'PROMO%'). Verifies the colref is part's p_type.
bool IsPromoPredicate(const Expression &e, const std::vector<LogicalGet *> &gets) {
  if (e.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) { return false; }
  auto &fn = e.Cast<BoundFunctionExpression>();
  const std::string &nm = fn.function.name;
  bool is_prefix = (nm == "prefix");
  bool is_like = (nm == "~~" || nm == "like");
  if (!is_prefix && !is_like) { return false; }
  if (fn.children.size() != 2) { return false; }
  if (fn.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
  if (fn.children[1]->GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) { return false; }
  auto col = ResolveJoinColref(fn.children[0]->Cast<BoundColumnRefExpression>(), gets);
  if (col.col_name != "p_type") { return false; }
  auto &cval = fn.children[1]->Cast<BoundConstantExpression>().value;
  if (cval.type().id() != LogicalTypeId::VARCHAR || cval.IsNull()) { return false; }
  std::string pat = cval.GetValue<std::string>();
  if (is_prefix) { return pat == "PROMO"; }
  return pat == "PROMO%";  // like
}
// ===========================================================================
// Shared join-tree introspection for the transparent Q3 / Q5 matchers.
//
// Both queries optimize to a LogicalAggregate over a left-deep tree of
// LOGICAL_COMPARISON_JOIN(INNER) nodes whose leaves are LOGICAL_GETs (sometimes
// wrapped in a stats-derived LOGICAL_FILTER, e.g. `c_custkey<=149999`). We walk
// the tree collecting every GET and every equi-condition (as resolved
// table.column name pairs), then the per-query matcher checks the exact set of
// tables / conditions / filters and bails (-> CPU) on any deviation.
// ===========================================================================

// One equi-join condition resolved to (table,col) on each side.
struct JoinEq {
  std::string lt, lc, rt, rc;  // left table/col, right table/col
};

// Recursively collect GETs + equi-conditions from an INNER-join tree. Descends
// through LOGICAL_COMPARISON_JOIN(INNER) and LOGICAL_FILTER; a LOGICAL_GET is a
// leaf. Any other node type -> return false (unsupported shape). Conditions are
// resolved lazily (we keep raw refs here; resolution happens after all GETs are
// known so ResolveJoinColref can see every table_index).
struct JoinTree {
  std::vector<LogicalGet *> gets;
  std::vector<std::pair<const Expression *, const Expression *>> raw_conds;  // (left,right) of each '='
};
bool CollectJoinTree(LogicalOperator *op, JoinTree &out) {
  if (!op) { return false; }
  switch (op->type) {
  case LogicalOperatorType::LOGICAL_GET:
    out.gets.push_back(&op->Cast<LogicalGet>());
    return true;
  case LogicalOperatorType::LOGICAL_FILTER:
    if (op->children.size() != 1) { return false; }
    return CollectJoinTree(op->children[0].get(), out);
  case LogicalOperatorType::LOGICAL_COMPARISON_JOIN: {
    auto &join = op->Cast<LogicalComparisonJoin>();
    if (join.join_type != JoinType::INNER) { return false; }
    if (join.children.size() != 2) { return false; }
    for (auto &cond : join.conditions) {
      if (cond.comparison != ExpressionType::COMPARE_EQUAL) { return false; }
      out.raw_conds.emplace_back(cond.left.get(), cond.right.get());
    }
    return CollectJoinTree(join.children[0].get(), out) &&
           CollectJoinTree(join.children[1].get(), out);
  }
  default:
    return false;
  }
}

// Resolve all collected raw conditions to (table,col) name pairs. Returns false
// if any side is not a plain colref into one of the collected GETs.
bool ResolveJoinConds(const JoinTree &jt, std::vector<JoinEq> &out) {
  for (auto &rc : jt.raw_conds) {
    if (rc.first->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
    if (rc.second->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
    auto l = ResolveJoinColref(rc.first->Cast<BoundColumnRefExpression>(), jt.gets);
    auto r = ResolveJoinColref(rc.second->Cast<BoundColumnRefExpression>(), jt.gets);
    if (l.col_name.empty() || r.col_name.empty()) { return false; }
    out.push_back({l.table_name, l.col_name, r.table_name, r.col_name});
  }
  return true;
}

// True if the set of resolved conditions contains an equi-condition between
// table.col == table2.col2 (either ordering).
bool HasCond(const std::vector<JoinEq> &eqs, const char *t1, const char *c1,
             const char *t2, const char *c2) {
  for (auto &e : eqs) {
    bool fwd = (e.lt == t1 && e.lc == c1 && e.rt == t2 && e.rc == c2);
    bool rev = (e.lt == t2 && e.lc == c2 && e.rt == t1 && e.rc == c1);
    if (fwd || rev) { return true; }
  }
  return false;
}

// Find the GET for a named table among the collected GETs (nullptr if absent).
LogicalGet *FindGet(const JoinTree &jt, const char *table_name) {
  for (auto *g : jt.gets) {
    auto te = g->GetTable();
    if (te && te->name == table_name) { return g; }
  }
  return nullptr;
}

// Resolve a group-by BoundColumnRef (points into a join output, which forwards a
// GET binding) to its table column name via the collected GETs.
std::string ResolveGroupColref(const Expression &e, const JoinTree &jt) {
  if (e.GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return std::string(); }
  return ResolveJoinColref(e.Cast<BoundColumnRefExpression>(), jt.gets).col_name;
}

// True if `e` is the Q3/Q5 revenue arg: l_extendedprice * (1 - l_discount).
// We verify it references l_extendedprice and l_discount and nothing else
// (the engine hardcodes the exact scale-4 formula). Robust to operand ordering.
bool IsRevenueExpr(const Expression &e, const JoinTree &jt) {
  if (e.GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) { return false; }
  auto &mul = e.Cast<BoundFunctionExpression>();
  if (mul.function.name != "*" && mul.function.name != "multiply") { return false; }
  if (mul.children.size() != 2) { return false; }
  // One child is l_extendedprice colref; the other is (1 - l_discount).
  const Expression *price = nullptr;
  const Expression *sub = nullptr;
  for (idx_t i = 0; i < 2; i++) {
    auto &c = *mul.children[i];
    if (c.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
        ResolveGroupColref(c, jt) == "l_extendedprice") {
      price = &c;
    } else {
      sub = &c;
    }
  }
  if (!price || !sub) { return false; }
  if (sub->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) { return false; }
  auto &subfn = sub->Cast<BoundFunctionExpression>();
  if (subfn.function.name != "-" && subfn.function.name != "subtract") { return false; }
  if (subfn.children.size() != 2) { return false; }
  // children: a constant (1 / 1.00) and l_discount colref (either order).
  bool seen_disc = false, seen_const = false;
  for (idx_t i = 0; i < 2; i++) {
    auto &c = *subfn.children[i];
    if (c.GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) { seen_const = true; }
    else if (c.GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
             ResolveGroupColref(c, jt) == "l_discount") { seen_disc = true; }
    else { return false; }
  }
  return seen_disc && seen_const;
}

// ===========================================================================
// RawPlan serializer (Stage-1 shadow validation).
//
// Flattens a matched DuckDB LogicalAggregate subtree into the ABI-neutral
// RawPlan wire form (a flat int64 "tape" + a uint8 "blob" of interned strings,
// sections in the fixed order from RAW_PLAN_CONTRACT.md), so the Mojo side
// (descriptor.mojo) can parse + classify it. This emits ONLY the descriptor;
// it never touches the plan. See raw_plan.h for the tag constants.
// ===========================================================================
namespace rp = mojo_gpu_rawplan;

// Accumulates the tape + blob. The string table and const pool are kept as the
// canonical lists so ids (indices) stay stable as sections are appended.
struct RawPlanBuilder {
  // STRING_TABLE: (blob_offset, byte_len) per interned string + dedup map.
  std::vector<std::pair<int64_t, int64_t>> strings;
  std::unordered_map<std::string, int64_t> string_ids;
  std::vector<uint8_t> blob;

  // CONSTS pool: each entry is (type_tag, scale, width, val_lo, val_hi, str_id).
  struct ConstEntry {
    int64_t type_tag, scale, width, val_lo, val_hi, str_id;
  };
  std::vector<ConstEntry> consts;

  // OUT_TYPES: (type_tag, scale, width).
  struct OutType { int64_t type_tag, scale, width; };
  std::vector<OutType> out_types;

  // GETS.
  struct Filter { int64_t col_strid, cmp_tag, const_id; };
  struct Get { int64_t table_strid, est_cardinality; std::vector<Filter> filters; };
  std::vector<Get> gets;

  // JOINS.
  struct Cond { int64_t lt, lc, rt, rc; };
  struct Join { int64_t join_type_tag; std::vector<Cond> conds; };
  std::vector<Join> joins;

  // GROUP_KEYS.
  struct GroupKey { int64_t table_strid, col_strid; };
  std::vector<GroupKey> group_keys;

  // AGGREGATES.
  struct Op { int64_t op_tag, a, b; };
  struct Agg {
    int64_t kind_tag, ret_type_tag, ret_scale, ret_width, ret_is_int128;
    std::vector<Op> program;
  };
  std::vector<Agg> aggregates;

  // HEADER.
  int64_t group_index = rp::IDX_NONE;
  int64_t aggregate_index = 0;

  // Dedup a string into the blob; return its string id (index into STRING_TABLE).
  int64_t intern(const std::string &s) {
    auto it = string_ids.find(s);
    if (it != string_ids.end()) { return it->second; }
    int64_t off = (int64_t)blob.size();
    int64_t len = (int64_t)s.size();
    blob.insert(blob.end(), s.begin(), s.end());
    int64_t id = (int64_t)strings.size();
    strings.emplace_back(off, len);
    string_ids[s] = id;
    return id;
  }

  // Push a constant into the pool; return its const id.
  int64_t add_const(int64_t type_tag, int64_t scale, int64_t width,
                    int64_t val_lo, int64_t val_hi, int64_t str_id) {
    int64_t id = (int64_t)consts.size();
    consts.push_back({type_tag, scale, width, val_lo, val_hi, str_id});
    return id;
  }

  // Serialize all sections, in the exact fixed order from the contract.
  std::vector<int64_t> finalize() const {
    std::vector<int64_t> tape;
    // HEADER
    tape.push_back(rp::MAGIC);
    tape.push_back(group_index);
    tape.push_back(aggregate_index);
    // STRING_TABLE
    tape.push_back((int64_t)strings.size());
    for (auto &s : strings) { tape.push_back(s.first); tape.push_back(s.second); }
    // OUT_TYPES
    tape.push_back((int64_t)out_types.size());
    for (auto &o : out_types) {
      tape.push_back(o.type_tag); tape.push_back(o.scale); tape.push_back(o.width);
    }
    // CONSTS
    tape.push_back((int64_t)consts.size());
    for (auto &c : consts) {
      tape.push_back(c.type_tag); tape.push_back(c.scale); tape.push_back(c.width);
      tape.push_back(c.val_lo); tape.push_back(c.val_hi); tape.push_back(c.str_id);
    }
    // GETS
    tape.push_back((int64_t)gets.size());
    for (auto &g : gets) {
      tape.push_back(g.table_strid);
      tape.push_back(g.est_cardinality);
      tape.push_back((int64_t)g.filters.size());
      for (auto &f : g.filters) {
        tape.push_back(f.col_strid); tape.push_back(f.cmp_tag); tape.push_back(f.const_id);
      }
    }
    // JOINS
    tape.push_back((int64_t)joins.size());
    for (auto &j : joins) {
      tape.push_back(j.join_type_tag);
      tape.push_back((int64_t)j.conds.size());
      for (auto &c : j.conds) {
        tape.push_back(c.lt); tape.push_back(c.lc);
        tape.push_back(c.rt); tape.push_back(c.rc);
      }
    }
    // GROUP_KEYS
    tape.push_back((int64_t)group_keys.size());
    for (auto &gk : group_keys) {
      tape.push_back(gk.table_strid); tape.push_back(gk.col_strid);
    }
    // AGGREGATES
    tape.push_back((int64_t)aggregates.size());
    for (auto &a : aggregates) {
      tape.push_back(a.kind_tag);
      tape.push_back(a.ret_type_tag); tape.push_back(a.ret_scale);
      tape.push_back(a.ret_width); tape.push_back(a.ret_is_int128);
      tape.push_back((int64_t)a.program.size());
      for (auto &op : a.program) {
        tape.push_back(op.op_tag); tape.push_back(op.a); tape.push_back(op.b);
      }
    }
    return tape;
  }
};

// Map a DuckDB LogicalType to the contract's (type_tag, scale, width).
void MapType(const LogicalType &t, int64_t &tag, int64_t &scale, int64_t &width) {
  scale = 0;
  width = 0;
  switch (t.id()) {
  case LogicalTypeId::BOOLEAN:  tag = rp::TYPE_BOOL; break;
  case LogicalTypeId::TINYINT:  tag = rp::TYPE_TINYINT; break;
  case LogicalTypeId::SMALLINT: tag = rp::TYPE_SMALLINT; break;
  case LogicalTypeId::INTEGER:  tag = rp::TYPE_INTEGER; break;
  case LogicalTypeId::BIGINT:   tag = rp::TYPE_BIGINT; break;
  case LogicalTypeId::HUGEINT:  tag = rp::TYPE_HUGEINT; break;
  case LogicalTypeId::FLOAT:    tag = rp::TYPE_FLOAT; break;
  case LogicalTypeId::DOUBLE:   tag = rp::TYPE_DOUBLE; break;
  case LogicalTypeId::DATE:     tag = rp::TYPE_DATE; break;
  case LogicalTypeId::VARCHAR:  tag = rp::TYPE_VARCHAR; break;
  case LogicalTypeId::DECIMAL:
    tag = rp::TYPE_DECIMAL;
    scale = DecimalType::GetScale(t);
    width = DecimalType::GetWidth(t);
    break;
  default:
    tag = rp::TYPE_INVALID; break;
  }
}

// Map a DuckDB filter comparison ExpressionType to a CMP_* tag (0 if unknown).
int64_t MapCmp(ExpressionType cmp) {
  switch (cmp) {
  case ExpressionType::COMPARE_EQUAL:              return rp::CMP_EQ;
  case ExpressionType::COMPARE_NOTEQUAL:           return rp::CMP_NE;
  case ExpressionType::COMPARE_LESSTHAN:           return rp::CMP_LT;
  case ExpressionType::COMPARE_LESSTHANOREQUALTO:  return rp::CMP_LE;
  case ExpressionType::COMPARE_GREATERTHAN:        return rp::CMP_GT;
  case ExpressionType::COMPARE_GREATERTHANOREQUALTO: return rp::CMP_GE;
  default: return 0;
  }
}

// Best-effort: add a const for a DuckDB Value, emitting raw integer + scale for
// decimals, days for dates, str_id for varchar. Stage-1 only checks structure,
// not exact constant values, so approximations here are acceptable.
int64_t AddValueConst(RawPlanBuilder &b, const Value &v) {
  int64_t tag, scale, width;
  MapType(v.type(), tag, scale, width);
  int64_t lo = 0, hi = 0, str_id = -1;
  if (v.IsNull()) { return b.add_const(tag, scale, width, 0, 0, -1); }
  switch (v.type().id()) {
  case LogicalTypeId::DATE:
    lo = v.GetValue<date_t>().days; break;
  case LogicalTypeId::VARCHAR:
    str_id = b.intern(v.GetValue<std::string>()); break;
  case LogicalTypeId::DECIMAL: {
    // Raw unscaled integer at `scale`. INT128-backed decimals split into limbs.
    if (v.type().InternalType() == PhysicalType::INT128) {
      hugeint_t h = v.GetValueUnsafe<hugeint_t>();
      lo = (int64_t)h.lower; hi = h.upper;
    } else {
      int64_t raw = (int64_t)llround(v.GetValue<double>() * std::pow(10.0, (double)scale));
      lo = raw; hi = raw < 0 ? -1 : 0;
    }
    break;
  }
  case LogicalTypeId::TINYINT:
  case LogicalTypeId::SMALLINT:
  case LogicalTypeId::INTEGER:
  case LogicalTypeId::BIGINT:
    lo = v.GetValue<int64_t>(); hi = lo < 0 ? -1 : 0; break;
  case LogicalTypeId::DOUBLE:
  case LogicalTypeId::FLOAT: {
    int64_t raw = (int64_t)llround(v.GetValue<double>());
    lo = raw; hi = raw < 0 ? -1 : 0; break;
  }
  default: break;
  }
  return b.add_const(tag, scale, width, lo, hi, str_id);
}

// Emit a postfix (RPN) program for an aggregate-argument expression into `prog`.
// Reuses the IsRevenueExpr / IsPromoPredicate grammar. Best-effort: unfamiliar
// sub-expressions emit a placeholder PUSH_CONST(0) rather than failing — Stage-1
// only validates structure, not exact program fidelity.
void EmitProgram(const Expression &e, const JoinTree &jt, RawPlanBuilder &b,
                 std::vector<RawPlanBuilder::Op> &prog,
                 optional_ptr<LogicalProjection> proj = nullptr) {
  auto cls = e.GetExpressionClass();
  if (cls == ExpressionClass::BOUND_COLUMN_REF) {
    // If the colref points into the arithmetic projection (Q1: the aggregate's
    // argument lives in the inner PROJECTION, not the GET), substitute that
    // projection expression and recurse — it may itself be arithmetic
    // (l_extendedprice*(1-l_discount)) or a plain GET colref (l_quantity).
    auto &ref = e.Cast<BoundColumnRefExpression>();
    if (proj && ref.binding.table_index == proj->table_index) {
      idx_t idx = ref.binding.column_index;
      if (idx < proj->expressions.size()) {
        EmitProgram(*proj->expressions[idx], jt, b, prog, proj);
        return;
      }
    }
    auto col = ResolveJoinColref(ref, jt.gets);
    int64_t t = b.intern(col.table_name);
    int64_t c = b.intern(col.col_name);
    prog.push_back({rp::OP_LOAD_COL, t, c});
    return;
  }
  if (cls == ExpressionClass::BOUND_CONSTANT) {
    int64_t cid = AddValueConst(b, e.Cast<BoundConstantExpression>().value);
    prog.push_back({rp::OP_PUSH_CONST, cid, 0});
    return;
  }
  if (cls == ExpressionClass::BOUND_FUNCTION) {
    auto &fn = e.Cast<BoundFunctionExpression>();
    const std::string &nm = fn.function.name;
    int64_t binop = 0;
    if (nm == "*" || nm == "multiply") { binop = rp::OP_MUL; }
    else if (nm == "-" || nm == "subtract") { binop = rp::OP_SUB; }
    else if (nm == "+" || nm == "add") { binop = rp::OP_ADD; }
    if (binop && fn.children.size() == 2) {
      EmitProgram(*fn.children[0], jt, b, prog, proj);
      EmitProgram(*fn.children[1], jt, b, prog, proj);
      prog.push_back({binop, 0, 0});
      return;
    }
  }
  if (cls == ExpressionClass::BOUND_CASE) {
    // Q14 promo CASE: PROMO_PRED(p_type); <then-program>; PUSH_CONST(0); SELECT.
    auto &ce = e.Cast<BoundCaseExpression>();
    if (ce.case_checks.size() == 1) {
      auto &chk = ce.case_checks[0];
      // PROMO predicate -> OP_PROMO_PRED(p_type table_strid, col_strid).
      if (IsPromoPredicate(*chk.when_expr, jt.gets)) {
        auto &pf = chk.when_expr->Cast<BoundFunctionExpression>();
        auto col = ResolveJoinColref(pf.children[0]->Cast<BoundColumnRefExpression>(), jt.gets);
        prog.push_back({rp::OP_PROMO_PRED, b.intern(col.table_name), b.intern(col.col_name)});
      } else {
        EmitProgram(*chk.when_expr, jt, b, prog, proj);
      }
      EmitProgram(*chk.then_expr, jt, b, prog, proj);
      int64_t zero = b.add_const(rp::TYPE_BIGINT, 0, 0, 0, 0, -1);
      prog.push_back({rp::OP_PUSH_CONST, zero, 0});
      prog.push_back({rp::OP_SELECT, 0, 0});
      return;
    }
  }
  // Unfamiliar: best-effort placeholder so the whole serialize doesn't fail.
  int64_t cid = b.add_const(rp::TYPE_BIGINT, 0, 0, 0, 0, -1);
  prog.push_back({rp::OP_PUSH_CONST, cid, 0});
}

// Map a DuckDB aggregate function name to an AggKind tag (0 if unsupported).
int64_t MapAggKind(const std::string &name) {
  if (name == "sum" || name == "sum_no_overflow") { return rp::AGG_SUM; }
  if (name == "avg") { return rp::AGG_AVG; }
  if (name == "count_star") { return rp::AGG_COUNT_STAR; }
  if (name == "min") { return rp::AGG_MIN; }
  if (name == "max") { return rp::AGG_MAX; }
  return 0;
}

// Resolve a (possibly through-projection) group-by colref to (table, col).
// Returns false if it doesn't resolve to a collected GET column.
bool ResolveGroupTableCol(const Expression &e, const JoinTree &jt,
                          optional_ptr<LogicalProjection> proj,
                          std::string &table, std::string &col) {
  const Expression *cur = &e;
  if (proj && cur->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
    auto &ref = cur->Cast<BoundColumnRefExpression>();
    if (ref.binding.table_index == proj->table_index) {
      idx_t idx = ref.binding.column_index;
      if (idx >= proj->expressions.size()) { return false; }
      auto &pe = proj->expressions[idx];
      if (pe->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
      cur = pe.get();
    }
  }
  if (cur->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
  auto c = ResolveJoinColref(cur->Cast<BoundColumnRefExpression>(), jt.gets);
  if (c.col_name.empty()) { return false; }
  table = c.table_name;
  col = c.col_name;
  return true;
}

// Walk the supported LogicalAggregate class generically and fill the builder.
// Returns false on anything outside the class (-> caller logs "unsupported").
bool SerializeMatchedPlan(LogicalAggregate &agg, RawPlanBuilder &out) {
  if (agg.children.size() != 1) { return false; }
  if (!agg.grouping_functions.empty()) { return false; }

  // 1. HEADER.
  out.group_index = agg.groups.empty() ? rp::IDX_NONE : (int64_t)agg.group_index;
  out.aggregate_index = (int64_t)agg.aggregate_index;

  // 2. Descend the child: skip an optional PROJECTION, then a single GET or a
  //    COMPARISON_JOIN tree.
  LogicalOperator *below = agg.children[0].get();
  optional_ptr<LogicalProjection> proj;
  if (below->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    proj = &below->Cast<LogicalProjection>();
    if (proj->children.size() != 1) { return false; }
    below = proj->children[0].get();
  }

  JoinTree jt;
  std::vector<JoinEq> eqs;
  bool single_get = false;
  if (below->type == LogicalOperatorType::LOGICAL_GET) {
    jt.gets.push_back(&below->Cast<LogicalGet>());
    single_get = true;
  } else if (below->type == LogicalOperatorType::LOGICAL_COMPARISON_JOIN) {
    if (!CollectJoinTree(below, jt)) { return false; }
    if (!ResolveJoinConds(jt, eqs)) { return false; }
  } else {
    return false;
  }
  if (jt.gets.empty()) { return false; }

  // 3. GETS.
  for (auto *g : jt.gets) {
    auto te = g->GetTable();
    if (!te) { return false; }
    RawPlanBuilder::Get ge;
    ge.table_strid = out.intern(te->name);
    ge.est_cardinality = (int64_t)g->estimated_cardinality;
    // Filters: keyed by table column index into get->names. Map -> name, cmp, const.
    auto add_filter = [&](idx_t col_idx, const ConstantFilter &cf) -> bool {
      if (col_idx >= g->names.size()) { return false; }
      int64_t cmp = MapCmp(cf.comparison_type);
      if (cmp == 0) { return false; }
      int64_t cid = AddValueConst(out, cf.constant);
      ge.filters.push_back({out.intern(g->names[col_idx]), cmp, cid});
      return true;
    };
    for (auto &kv : g->table_filters.filters) {
      idx_t col_idx = kv.first;
      TableFilter &tf = *kv.second;
      if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
        if (!add_filter(col_idx, tf.Cast<ConstantFilter>())) { return false; }
      } else if (tf.filter_type == TableFilterType::CONJUNCTION_AND) {
        auto &conj = tf.Cast<ConjunctionAndFilter>();
        for (auto &cfp : conj.child_filters) {
          if (cfp->filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
          if (!add_filter(col_idx, cfp->Cast<ConstantFilter>())) { return false; }
        }
      } else {
        return false;  // unmodeled filter shape
      }
    }
    out.gets.push_back(std::move(ge));
  }

  // 4. JOINS (one INNER entry with the resolved conds; none if single GET).
  if (!single_get) {
    RawPlanBuilder::Join jn;
    jn.join_type_tag = rp::JOIN_INNER;
    for (auto &e : eqs) {
      jn.conds.push_back({out.intern(e.lt), out.intern(e.lc),
                          out.intern(e.rt), out.intern(e.rc)});
    }
    out.joins.push_back(std::move(jn));
  }

  // Identify the fact table (max-cardinality GET) so a group key bound to a
  // dimension column via an equi-join equivalence can be re-attributed to its
  // fact-side column — mirroring what the engine actually emits, and what the
  // Mojo strategy picker keys on (integer fact group key -> SORT_SEGREDUCE).
  std::string fact_table;
  {
    int64_t best = -1;
    for (auto *g : jt.gets) {
      auto te = g->GetTable();
      if (te && (int64_t)g->estimated_cardinality > best) {
        best = (int64_t)g->estimated_cardinality;
        fact_table = te->name;
      }
    }
  }

  // 5. GROUP_KEYS (resolved (table,col); none when ungrouped).
  for (idx_t i = 0; i < agg.groups.size(); i++) {
    std::string table, col;
    if (!ResolveGroupTableCol(*agg.groups[i], jt, proj, table, col)) { return false; }
    // If this key is on a dimension but equi-joins to the fact table, prefer the
    // fact side (the optimizer substitutes either side via the equivalence).
    if (table != fact_table) {
      for (auto &e : eqs) {
        if (e.lt == table && e.lc == col && e.rt == fact_table) {
          table = e.rt; col = e.rc; break;
        }
        if (e.rt == table && e.rc == col && e.lt == fact_table) {
          table = e.lt; col = e.lc; break;
        }
      }
    }
    out.group_keys.push_back({out.intern(table), out.intern(col)});
  }

  // 6. OUT_TYPES: group columns first (group-key order), then aggregate columns.
  for (idx_t i = 0; i < agg.groups.size(); i++) {
    int64_t tag, scale, width;
    MapType(agg.groups[i]->return_type, tag, scale, width);
    out.out_types.push_back({tag, scale, width});
  }
  for (idx_t i = 0; i < agg.expressions.size(); i++) {
    int64_t tag, scale, width;
    MapType(agg.expressions[i]->return_type, tag, scale, width);
    out.out_types.push_back({tag, scale, width});
  }

  // 7. AGGREGATES.
  for (idx_t i = 0; i < agg.expressions.size(); i++) {
    if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) {
      return false;
    }
    auto &ag = agg.expressions[i]->Cast<BoundAggregateExpression>();
    if (ag.IsDistinct() || ag.filter) { return false; }
    int64_t kind = MapAggKind(ag.function.name);
    if (kind == 0) { return false; }
    RawPlanBuilder::Agg ae;
    ae.kind_tag = kind;
    int64_t tag, scale, width;
    MapType(ag.return_type, tag, scale, width);
    ae.ret_type_tag = tag;
    ae.ret_scale = scale;
    ae.ret_width = width;
    ae.ret_is_int128 = (ag.return_type.InternalType() == PhysicalType::INT128) ? 1 : 0;
    // Program: empty for COUNT_STAR; else the single argument expression.
    if (kind != rp::AGG_COUNT_STAR && ag.children.size() == 1) {
      EmitProgram(*ag.children[0], jt, out, ae.program, proj);
    }
    out.aggregates.push_back(std::move(ae));
  }

  return true;
}

// Shadow validation entry: serialize -> build descriptor -> log classification.
void ShadowValidateAggregate(LogicalAggregate &agg) {
  RawPlanBuilder b;
  bool ok = false;
  try {
    ok = SerializeMatchedPlan(agg, b);
  } catch (...) {
    ok = false;
  }
  if (!ok) {
    fprintf(stderr, "[gpu-shadow] unsupported\n");
    return;
  }
  std::vector<int64_t> tape = b.finalize();
  void *handle = reinterpret_cast<void *>(
      mojo_gpu_build_descriptor(tape.data(), (int64_t)tape.size(),
                                b.blob.data(), (int64_t)b.blob.size()));
  if (!handle) {
    fprintf(stderr, "[gpu-shadow] unsupported\n");
    return;
  }
  int64_t kind = mojo_gpu_desc_kind(handle);
  int64_t strat = mojo_gpu_desc_strategy(handle);
  int64_t dims = mojo_gpu_desc_n_dims(handle);
  int64_t aggs = mojo_gpu_desc_n_aggs(handle);
  char fact[256] = {0};
  int64_t flen = mojo_gpu_desc_fact_table(handle, reinterpret_cast<uint8_t *>(fact),
                                          (int64_t)sizeof(fact) - 1);
  if (flen < 0) { flen = 0; }
  if (flen > (int64_t)sizeof(fact) - 1) { flen = (int64_t)sizeof(fact) - 1; }
  fact[flen] = '\0';
  fprintf(stderr, "[gpu-shadow] kind=%lld strat=%lld dims=%lld aggs=%lld fact=%s\n",
          (long long)kind, (long long)strat, (long long)dims, (long long)aggs, fact);
  mojo_gpu_desc_free(handle);
}

// ===========================================================================
// Stage-2 generic descriptor-driven operator (LogicalGpuAgg / PhysicalGpuAgg).
//
// A single source operator that executes a query described entirely by a Mojo
// descriptor handle (built from the RawPlan wire form). It is class-agnostic:
// output schema, the SQL to materialize, column feeding and result extraction
// are all driven by the shuttle ABI. This is the default (and only) GPU
// aggregate path; GPU_OP_GENERIC=off disables it -> stock DuckDB CPU.
// ===========================================================================

// Contract TypeTag (scale/width) -> DuckDB LogicalType. Inverse of MapType.
LogicalType TagToLogicalType(int64_t tag, int64_t scale, int64_t width) {
  switch (tag) {
  case rp::TYPE_BOOL:     return LogicalType::BOOLEAN;
  case rp::TYPE_TINYINT:  return LogicalType::TINYINT;
  case rp::TYPE_SMALLINT: return LogicalType::SMALLINT;
  case rp::TYPE_INTEGER:  return LogicalType::INTEGER;
  case rp::TYPE_BIGINT:   return LogicalType::BIGINT;
  case rp::TYPE_HUGEINT:  return LogicalType::HUGEINT;
  case rp::TYPE_FLOAT:    return LogicalType::FLOAT;
  case rp::TYPE_DOUBLE:   return LogicalType::DOUBLE;
  case rp::TYPE_DATE:     return LogicalType::DATE;
  case rp::TYPE_VARCHAR:  return LogicalType::VARCHAR;
  case rp::TYPE_DECIMAL:  return LogicalType::DECIMAL((uint8_t)width, (uint8_t)scale);
  default:
    throw InvalidInputException("GPU_AGG: unsupported descriptor type tag " +
                                std::to_string(tag));
  }
}

// DuckDB LogicalType -> contract TypeTag (for the feed_column type_tag argument).
int64_t LogicalTypeToTag(const LogicalType &t) {
  int64_t tag, scale, width;
  MapType(t, tag, scale, width);
  return tag;
}

struct GpuAggSourceGlobalState : public GlobalSourceState {
  void *handle = nullptr;   // descriptor handle (owned by PhysicalGpuAgg, not freed here)
  idx_t n_rows = 0;
  idx_t emitted = 0;
  bool done = false;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalGpuAgg : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalGpuAgg(PhysicalPlan &plan, vector<LogicalType> types, void *desc_handle,
                 idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        desc_handle(desc_handle) {}

  ~PhysicalGpuAgg() override {
    if (desc_handle) { mojo_gpu_desc_free(desc_handle); }
  }

  void *desc_handle;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<GpuAggSourceGlobalState>();
    gs->handle = desc_handle;
    void *h = desc_handle;

    int64_t n_req = mojo_gpu_desc_materialize_count(h);
    bool cold = (mojo_gpu_pin_begin(h) == 1);

    if (cold) {
      for (int64_t i = 0; i < n_req; i++) {
        // SQL: call once for the byte length, resize, call again to fill.
        int64_t len = mojo_gpu_desc_materialize_sql(h, i, nullptr, 0);
        if (len < 0) { throw InvalidInputException("GPU_AGG: materialize_sql failed"); }
        std::string sql;
        sql.resize((size_t)len);
        if (len > 0) {
          mojo_gpu_desc_materialize_sql(h, i, reinterpret_cast<uint8_t *>(&sql[0]), len);
        }

        Connection con(*context.db);
        auto res = con.Query(sql);
        if (res->HasError()) {
          throw InvalidInputException("GPU_AGG: materialize query failed: " + res->GetError());
        }
        idx_t total_rows = res->RowCount();
        idx_t n_cols = res->types.size();

        // Gather every output column CONTIGUOUSLY across all chunks into one flat
        // buffer (feed_column overwrites per (req,col), so a single contiguous feed
        // per column is required), then feed it once. Read chunks lazily into the
        // per-column buffers so the result is walked once.
        // Per-column staging buffers, typed by the column's physical layout.
        std::vector<std::vector<int32_t>> buf_i32(n_cols);
        std::vector<std::vector<int64_t>> buf_i64(n_cols);
        std::vector<std::vector<hugeint_t>> buf_i128(n_cols);
        std::vector<std::vector<double>> buf_f64(n_cols);
        // VARCHAR columns (e.g. Q1 group keys, Q14 p_type) are fed as a CONTIGUOUS
        // array of string_t (16 bytes each). A non-inlined string_t (length > 12)
        // is a POINTER into the chunk's per-scan string heap, which DuckDB reuses
        // across Fetch() calls — so we must capture the bytes WHILE the chunk is
        // alive, not rely on the pointer surviving. We deep-copy each string's
        // CONTENT into a persistent per-column std::string store (`buf_strdata`),
        // then after the scan rebuild self-contained string_t pointing into that
        // stable storage. (1-char group keys are inlined, so the copy is moot for
        // them, but doing it uniformly is correct for arbitrary-length VARCHAR.)
        std::vector<std::vector<string_t>> buf_str(n_cols);
        std::vector<std::vector<std::string>> buf_strdata(n_cols);
        for (idx_t c = 0; c < n_cols; c++) {
          const LogicalType &ct = res->types[c];
          if (ct.id() == LogicalTypeId::VARCHAR) {
            buf_str[c].reserve(total_rows);
            buf_strdata[c].reserve(total_rows);
            continue;
          }
          switch (ct.InternalType()) {
          case PhysicalType::INT32:  buf_i32[c].reserve(total_rows); break;
          case PhysicalType::INT64:  buf_i64[c].reserve(total_rows); break;
          case PhysicalType::INT128: buf_i128[c].reserve(total_rows); break;
          case PhysicalType::DOUBLE: buf_f64[c].reserve(total_rows); break;
          default:
            throw InvalidInputException(
                "GPU_AGG: unsupported materialized column physical type for " +
                ct.ToString());
          }
        }

        while (true) {
          auto chunk = res->Fetch();
          if (!chunk || chunk->size() == 0) { break; }
          auto n = chunk->size();
          for (idx_t c = 0; c < n_cols; c++) {
            chunk->data[c].Flatten(n);
            const LogicalType &ct = res->types[c];
            if (ct.id() == LogicalTypeId::VARCHAR) {
              // Capture each string's CONTENT now (chunk is alive); the string_t
              // structs are rebuilt after the scan from this stable storage.
              const string_t *p = FlatVector::GetData<string_t>(chunk->data[c]);
              for (idx_t r = 0; r < n; r++) {
                buf_strdata[c].emplace_back(p[r].GetData(), p[r].GetSize());
              }
              continue;
            }
            switch (ct.InternalType()) {
            case PhysicalType::INT32: {
              const int32_t *p = FlatVector::GetData<int32_t>(chunk->data[c]);
              buf_i32[c].insert(buf_i32[c].end(), p, p + n);
              break;
            }
            case PhysicalType::INT64: {
              const int64_t *p = FlatVector::GetData<int64_t>(chunk->data[c]);
              buf_i64[c].insert(buf_i64[c].end(), p, p + n);
              break;
            }
            case PhysicalType::INT128: {
              const hugeint_t *p = FlatVector::GetData<hugeint_t>(chunk->data[c]);
              buf_i128[c].insert(buf_i128[c].end(), p, p + n);
              break;
            }
            case PhysicalType::DOUBLE: {
              const double *p = FlatVector::GetData<double>(chunk->data[c]);
              buf_f64[c].insert(buf_f64[c].end(), p, p + n);
              break;
            }
            default:
              throw InvalidInputException("GPU_AGG: unsupported column physical type");
            }
          }
        }

        // Rebuild self-contained string_t for every VARCHAR column, pointing into
        // the now-complete (address-stable, reserved) buf_strdata storage. Mojo's
        // feed deep-copies these again into its own heap, but stable pointers here
        // are required so that deep copy reads valid bytes.
        for (idx_t c = 0; c < n_cols; c++) {
          if (res->types[c].id() != LogicalTypeId::VARCHAR) { continue; }
          for (auto &s : buf_strdata[c]) {
            buf_str[c].emplace_back(s.data(), (uint32_t)s.size());
          }
        }

        // Feed each column once (the contract TypeTag describes the element type).
        for (idx_t c = 0; c < n_cols; c++) {
          const LogicalType &ct = res->types[c];
          int64_t tag = LogicalTypeToTag(ct);
          void *ptr = nullptr;
          if (ct.id() == LogicalTypeId::VARCHAR) {
            // Feed the contiguous string_t array; element stride is sizeof(string_t)==16.
            ptr = buf_str[c].data();
            tag = rp::TYPE_VARCHAR;
          } else {
            switch (ct.InternalType()) {
            case PhysicalType::INT32:  ptr = buf_i32[c].data(); break;
            case PhysicalType::INT64:  ptr = buf_i64[c].data(); break;
            case PhysicalType::INT128: ptr = buf_i128[c].data(); break;
            case PhysicalType::DOUBLE: ptr = buf_f64[c].data(); break;
            default: break;
            }
          }
          int64_t rc = mojo_gpu_feed_column(h, i, (int64_t)c, ptr,
                                            (int64_t)total_rows, tag);
          if (rc != 0) {
            throw InvalidInputException("GPU_AGG: feed_column failed (rc " +
                                        std::to_string(rc) + ")");
          }
        }
      }
    }

    int64_t fin_rc = mojo_gpu_pin_finalize(h);
    if (fin_rc != 0) {
      throw InvalidInputException("GPU_AGG: pin_finalize failed (rc " +
                                  std::to_string(fin_rc) + ")");
    }
    gs->n_rows = (idx_t)mojo_gpu_result_rows(h);
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<GpuAggSourceGlobalState>();
    if (gs.done || gs.emitted >= gs.n_rows) {
      chunk.SetCardinality(0);
      gs.done = true;
      return SourceResultType::FINISHED;
    }
    void *h = gs.handle;
    idx_t n_cols = chunk.ColumnCount();
    idx_t remaining = gs.n_rows - gs.emitted;
    idx_t this_chunk = std::min<idx_t>(remaining, STANDARD_VECTOR_SIZE);

    for (idx_t c = 0; c < n_cols; c++) {
      chunk.data[c].SetVectorType(VectorType::FLAT_VECTOR);
      const LogicalType &ct = chunk.data[c].GetType();
      for (idx_t r = 0; r < this_chunk; r++) {
        int64_t row = (int64_t)(gs.emitted + r);
        switch (ct.id()) {
        case LogicalTypeId::DECIMAL:
        case LogicalTypeId::HUGEINT: {
          int64_t lo = 0, hi = 0;
          mojo_gpu_result_i128(h, row, (int64_t)c, &lo, &hi);
          // hugeint_t is {uint64_t lower; int64_t upper}; lo/hi are the int128 limbs.
          hugeint_t hv;
          hv.lower = (uint64_t)lo;
          hv.upper = hi;
          FlatVector::GetData<hugeint_t>(chunk.data[c])[r] = hv;
          break;
        }
        case LogicalTypeId::DOUBLE: {
          FlatVector::GetData<double>(chunk.data[c])[r] = mojo_gpu_result_f64(h, row, (int64_t)c);
          break;
        }
        case LogicalTypeId::DATE:
        case LogicalTypeId::INTEGER: {
          int64_t v = mojo_gpu_result_i64(h, row, (int64_t)c);
          FlatVector::GetData<int32_t>(chunk.data[c])[r] = (int32_t)v;
          break;
        }
        case LogicalTypeId::BIGINT: {
          FlatVector::GetData<int64_t>(chunk.data[c])[r] = mojo_gpu_result_i64(h, row, (int64_t)c);
          break;
        }
        case LogicalTypeId::VARCHAR: {
          int64_t len = mojo_gpu_result_str(h, row, (int64_t)c, nullptr, 0);
          std::string s;
          s.resize((size_t)(len < 0 ? 0 : len));
          if (len > 0) {
            mojo_gpu_result_str(h, row, (int64_t)c, reinterpret_cast<uint8_t *>(&s[0]), len);
          }
          FlatVector::GetData<string_t>(chunk.data[c])[r] =
              StringVector::AddString(chunk.data[c], s);
          break;
        }
        default:
          throw InvalidInputException("GPU_AGG: unsupported output type " + ct.ToString());
        }
      }
    }
    chunk.SetCardinality(this_chunk);
    gs.emitted += this_chunk;
    if (gs.emitted >= gs.n_rows) { gs.done = true; }
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_AGG"; }
};

// Logical op driven entirely by the Mojo descriptor handle. Owns the handle.
class LogicalGpuAgg : public LogicalExtensionOperator {
public:
  explicit LogicalGpuAgg(void *desc_handle) : desc_handle(desc_handle) {
    BuildTypes();
  }

  void *desc_handle;

  void BuildTypes() {
    types.clear();
    int64_t arity = mojo_gpu_desc_out_arity(desc_handle);
    for (int64_t i = 0; i < arity; i++) {
      int64_t tag = 0, scale = 0, width = 0;
      mojo_gpu_desc_out_type(desc_handle, i, &tag, &scale, &width);
      types.push_back(TagToLogicalType(tag, scale, width));
    }
  }

  vector<ColumnBinding> GetColumnBindings() override {
    vector<ColumnBinding> result;
    int64_t group_index = mojo_gpu_desc_group_index(desc_handle);
    int64_t aggregate_index = mojo_gpu_desc_aggregate_index(desc_handle);
    int64_t arity = mojo_gpu_desc_out_arity(desc_handle);
    if (group_index == rp::IDX_NONE) {
      // Ungrouped: all outputs are aggregate columns.
      for (int64_t i = 0; i < arity; i++) {
        result.emplace_back((idx_t)aggregate_index, (idx_t)i);
      }
    } else {
      // Grouped: group cols first (at group_index), then agg cols (at aggregate_index).
      // OUT_TYPES order is group cols then agg cols; bindings mirror LogicalQ1.
      int64_t n_groups = 0;
      // Group count is the number of leading group columns; derive it from the
      // aggregate count (arity - n_aggs) so we don't need a separate getter.
      int64_t n_aggs = mojo_gpu_desc_n_aggs(desc_handle);
      n_groups = arity - n_aggs;
      if (n_groups < 0) { n_groups = 0; }
      for (int64_t i = 0; i < n_groups; i++) {
        result.emplace_back((idx_t)group_index, (idx_t)i);
      }
      for (int64_t i = 0; i < arity - n_groups; i++) {
        result.emplace_back((idx_t)aggregate_index, (idx_t)i);
      }
    }
    return result;
  }

  void ResolveTypes() override { BuildTypes(); }

  string GetName() const override { return "GPU_AGG"; }
  string GetExtensionName() const override { return "mojo_gpu_agg"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    void *h = desc_handle;
    desc_handle = nullptr;  // transfer ownership to the physical op
    return planner.Make<PhysicalGpuAgg>(types, h, estimated_cardinality);
  }
};

// Serialize the matched aggregate, build a descriptor, and if its kind is
// buildable replace *node with a LogicalGpuAgg.
bool TryRouteGeneric(unique_ptr<LogicalOperator> &node) {
  // Default-ON: the descriptor-driven generic engine handles all supported
  // classes unless explicitly disabled with GPU_OP_GENERIC=off|none. A non-empty
  // value other than off/none restricts routing to the named kinds (substring
  // match, e.g. "q3 q5"). Whenever this returns false the node is left untouched
  // and runs on stock DuckDB CPU (the bespoke MatchQ* path no longer exists).
  const char *gen = std::getenv("GPU_OP_GENERIC");
  bool all = (gen == nullptr);
  if (gen && (std::strcmp(gen, "off") == 0 || std::strcmp(gen, "none") == 0)) {
    return false;
  }
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }

  RawPlanBuilder b;
  if (!SerializeMatchedPlan(node->Cast<LogicalAggregate>(), b)) { return false; }
  std::vector<int64_t> tape = b.finalize();
  void *h = reinterpret_cast<void *>(
      mojo_gpu_build_descriptor(tape.data(), (int64_t)tape.size(),
                                b.blob.data(), (int64_t)b.blob.size()));
  if (!h) { return false; }

  int64_t kind = mojo_gpu_desc_kind(h);
  bool enabled = false;
  if (kind == rp::KIND_Q6 && (all || std::strstr(gen, "q6"))) { enabled = true; }
  if (kind == rp::KIND_Q1 && (all || std::strstr(gen, "q1"))) { enabled = true; }
  if (kind == rp::KIND_Q14 && (all || std::strstr(gen, "q14"))) { enabled = true; }
  if (kind == rp::KIND_Q3 && (all || std::strstr(gen, "q3"))) { enabled = true; }
  if (kind == rp::KIND_Q5 && (all || std::strstr(gen, "q5"))) { enabled = true; }

  if (!enabled) { mojo_gpu_desc_free(h); return false; }

  // LogicalGpuAgg takes ownership of the handle (NOT freed on the success path).
  auto repl = make_uniq<LogicalGpuAgg>(h);
  repl->estimated_cardinality = node->estimated_cardinality;
  node = std::move(repl);
  return true;
}

void LoadInternal(ExtensionLoader &loader) {
  mojo_gpu_ctx_init();                                 // pay the ~32 ms DeviceContext init once, at LOAD
  RegisterGpuOperator(loader.GetDatabaseInstance());  // transparent cosine operator
  RegisterGpuCosineTableFunction(loader);             // pin-resident cosine TF
}

}  // namespace
}  // namespace duckdb

// ---------------------------------------------------------------------------
// Extension entry points (CPP ABI), mirroring packages/mojo-kernel-overrides.
// ---------------------------------------------------------------------------
extern "C" {
__attribute__((visibility("default"))) void mojo_gpu_operator_duckdb_cpp_init(
    duckdb::ExtensionLoader &loader) {
  duckdb::LoadInternal(loader);
}
__attribute__((visibility("default"))) const char *mojo_gpu_operator_version() {
  return duckdb::DuckDB::LibraryVersion();
}
// Embedder entry: register on an existing connection's database.
__attribute__((visibility("default"))) void register_mojo_gpu_operator(
    duckdb_connection connection) {
  auto con = reinterpret_cast<duckdb::Connection *>(connection);
  duckdb::RegisterGpuOperator(*con->context->db);
}
}
