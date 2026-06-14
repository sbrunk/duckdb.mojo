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
// Storage-internal headers for the GPU-direct native-storage decode path
// (Phase A reachability + Phase C end-to-end slice). These are DuckDB-internal
// (CPP ABI), version-locked to the exact DuckDB this extension is built against.
#include "duckdb/catalog/catalog_entry/duck_table_entry.hpp"
#include "duckdb/storage/data_table.hpp"
#include "duckdb/storage/table/row_group_collection.hpp"
#include "duckdb/storage/table/row_group.hpp"
#include "duckdb/storage/table/column_data.hpp"
#include "duckdb/storage/table/column_segment.hpp"
#include "duckdb/storage/table_storage_info.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/storage/buffer/buffer_handle.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/block_manager.hpp"
#include "duckdb/storage/storage_index.hpp"
#include "duckdb/common/enums/compression_type.hpp"
#include "duckdb/common/enum_util.hpp"
#include "duckdb/main/attached_database.hpp"

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
// GPU-direct native-storage segment decode (Phase C). Uploads `seg_bytes`
// (seg_nbytes raw bytes), decodes `n_rows` values on the GPU, writes them into
// the host `out_ptr`. codec: 0=UNCOMPRESSED, 1=BITPACKING (CONSTANT/FOR);
// type_code: 0=int32, 1=int64. rc: 0 ok; 1 bad args; 2 unsupported; 3 internal.
int32_t mojo_gpu_decode_segment(const uint8_t *seg_bytes, int64_t seg_nbytes,
                                int64_t n_rows, int64_t codec, int64_t type_code,
                                void *out_ptr);
// Pin-resident engine: pin the whole column once, query many times.
int64_t mojo_gpu_pin(const float *emb, int64_t n_rows, int64_t K);
int32_t mojo_gpu_pin_query(void *handle, const float *q, float *out);
// Pin-resident top-k: returns the k smallest cosine distances ascending,
// tie-break (dist, then rowid). out_ids/out_dists are caller-allocated, length k.
// Padded slots (fewer than k valid rows) carry id = -1.
// rc: 0 ok; 1 null handle; 2 bad k (k<=0 || k>1024); 3 internal error.
int32_t mojo_gpu_pin_query_topk(void *handle, const float *q, int64_t k,
                                int64_t *out_ids, float *out_dists);
void mojo_gpu_pin_free(void *handle);
// fp16 pin-resident engine: same contract as the fp32 trio above, but the
// resident matrix is stored as float16 (~1.6-1.8x faster top-k, half the VRAM,
// recall >=0.99 on normalized embeddings). fp16 handles MUST be freed with
// mojo_gpu_pin_free_f16, never the fp32 free.
int64_t mojo_gpu_pin_f16(const float *emb, int64_t n_rows, int64_t K);
int32_t mojo_gpu_pin_query_topk_f16(void *handle, const float *q, int64_t k,
                                    int64_t *out_ids, float *out_dists);
void mojo_gpu_pin_free_f16(void *handle);
// TRUE batched exact top-k: score M query vectors (row-major M*K floats) against
// the resident matrix, reading that matrix ONCE per query-tile (not once per
// query). out_ids/out_dists are caller-allocated, length M*k, row-major
// [query*k + slot]; padded slots carry id = -1. Returns the SAME exact (ids,
// dists) as M single-query calls. rc: 0 ok; 1 null handle; 2 bad k/M; 3 internal.
int32_t mojo_gpu_pin_query_topk_batch(void *handle, const float *qs, int64_t M,
                                      int64_t k, int64_t *out_ids, float *out_dists);
int32_t mojo_gpu_pin_query_topk_batch_f16(void *handle, const float *qs, int64_t M,
                                          int64_t k, int64_t *out_ids, float *out_dists);
// Batched fp16 top-k with an explicit metric: 0 = cosine (array_cosine_distance),
// 1 = L2 / squared-euclidean (array_distance), 2 = inner-product
// (array_negative_inner_product / -dot). metric != 0 is ONLY implemented on the
// fused tensor-core path (GPU_OP_TENSORCORE on an NVIDIA build, supported K/k);
// otherwise it returns rc != 0 so the caller can fall back to stock DuckDB.
int32_t mojo_gpu_pin_query_topk_batch_f16_metric(void *handle, const float *qs, int64_t M,
                                                 int64_t k, int64_t metric,
                                                 int64_t *out_ids, float *out_dists);
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

// Pre/optimize hook coordination: the pre-optimizer hook disables a small set of
// DuckDB optimizers whose output the transparent matcher cannot serialize, then
// the paired optimize hook restores the original disabled-optimizer set so the
// change never leaks into later CPU queries. Each disable below is justified by a
// concrete plan shape the matcher would otherwise miss or mis-handle:
//
//   COMPRESSED_MATERIALIZATION - string-dictionary compression rewrites grouped
//     GROUP BY keys into __internal_compress/decompress projections that a source
//     operator can't reproduce; disabling it keeps Q1's keys raw VARCHAR so the
//     matcher sees LogicalAggregate(2 varchar group refs, 8 aggregates) over a
//     filtered GET.
//
//   STATISTICS_PROPAGATION - on a multi-table FK-join (our Q3/Q5 class) it derives
//     a redundant range filter from the join-key statistics (e.g. `c_custkey<=N`
//     when customer is joined to orders) and leaves it as a residual LOGICAL_FILTER
//     operator INSIDE the join tree, above the dimension scan. CollectJoinTree
//     descends the join tree but only serializes GET.table_filters, never a
//     standalone LogicalFilter's predicate -- so (post the CollectJoinTree guard
//     that now bails on any non-empty residual filter rather than silently drop it)
//     such a tree is rejected and the offload is lost, even though the filter is a
//     tautology implied by the join. Disabling the pass removes the redundant
//     filter, restoring a clean Aggregate-over-INNER-join-tree the matcher can
//     serialize. Verified: a Q3-shaped GROUP BY o_shippriority (DENSE_GROUP, NOT
//     cost-declined) serializes only with this pass disabled. NOTE: this pass also
//     folds an *ungrouped* MIN/MAX over an unfiltered column into a constant
//     (EXPRESSION_GET + DUMMY_SCAN, no Aggregate node) -- but that fold is a free
//     zone-map read that beats any GPU scan, so we deliberately do NOT fight it;
//     the SUM-based scalar aggregates we offload (Q6/Q14) never fold.
//
// IN_CLAUSE and LATE_MATERIALIZATION are deliberately left ENABLED: an `x IN (...)`
// filter rewrites to either a residual OR-filter or a MARK-join + CHUNK_GET, both of
// which CollectJoinTree already rejects (non-INNER join / non-GET leaf / residual
// filter) -> safe CPU fallback, identical with or without the pass; and
// LATE_MATERIALIZATION only fires on LIMIT/TOP_N/SAMPLE roots over PROJECTION/FILTER/
// GET chains (an Aggregate breaks the chain), so it can never rewrite inside our
// matched aggregate subtree -- the ORDER BY/LIMIT always sits ABOVE the aggregate and
// stays with DuckDB. (Sirius disables all four because its rebind path executes the
// ENTIRE plan on GPU; we only replace the aggregate subtree and leave the rest to CPU.)
//
// We remember the original set per-thread (the binder runs both hooks on the same
// thread for one plan), keyed by context pointer for re-entrancy safety (the generic
// operator's pin self-query runs nested on the same thread).
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
    // Stop the FK-join statistics pass from injecting redundant range filters
    // (e.g. `c_custkey<=N`) as residual LogicalFilter operators inside the join
    // tree, which CollectJoinTree must reject (it can't serialize them) -- see the
    // block comment above. Keeps the Q3/Q5 join trees clean and serializable.
    opts.disabled_optimizers.insert(OptimizerType::STATISTICS_PROPAGATION);
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

// ===-------------------------------------------------------------------===//
// Bounded, LRU-evicting resident pin registry (feature #4).
//
// The kNN embedding pins are the biggest GPU residents in the extension (~3 GB
// at 1M x 768 fp32). Previously they were a process-lifetime, never-evicted
// `unordered_map`, so pinning many embedding matrices (or many tables/columns)
// leaked VRAM until the GPU OOM'd. This registry caps total resident bytes at a
// configurable budget and evicts the least-recently-used *evictable* entry to
// make room, freeing its DeviceBuffers via the Mojo free entry points.
//
// Correctness: eviction is always safe. A pin is pure cache — a query that finds
// its key missing simply rebuilds it (materialize + upload = the COLD path,
// slower but identical results). The ONE thing eviction must never do is free a
// buffer a query is actively reading: a `Bind` fetches the handle under the lock,
// then runs the GPU query OUTSIDE the lock, so a concurrent evict could otherwise
// free the buffer mid-query (use-after-free). We guard that with a per-entry
// in-use refcount (`PinLease`, RAII): an in-use entry (refcount > 0) is never
// evicted; it stays resident until its query finishes, then becomes evictable.
// ===-------------------------------------------------------------------===//
enum class PinKind { FP32, FP16 };

struct ResidentPin {
  void *handle = nullptr;
  idx_t n_rows = 0;
  idx_t K = 0;
  PinKind kind = PinKind::FP32;
  size_t bytes = 0;          // approximate resident VRAM footprint
  uint64_t last_use = 0;     // monotonic tick of last access (LRU ordering)
  int refcount = 0;          // > 0 => a query is in flight; do not evict
};

std::mutex g_pin_mu;
std::unordered_map<std::string, ResidentPin> g_pins;  // keyed "table.column[#f16]"
size_t g_pin_bytes_resident = 0;                      // sum of g_pins[*].bytes
uint64_t g_pin_tick = 0;                              // monotonic LRU clock

// Free an entry's GPU buffers via the right Mojo entry point. Caller holds the
// lock and has already removed it from the map + decremented g_pin_bytes_resident.
void FreeResidentPin(const ResidentPin &e) {
  if (!e.handle) { return; }
  if (e.kind == PinKind::FP16) {
    mojo_gpu_pin_free_f16(e.handle);
  } else {
    mojo_gpu_pin_free(e.handle);
  }
}

// Resident-byte budget. 0 == unbounded (legacy never-evict behavior). Default is
// a fixed conservative cap; GPU_OP_PIN_BUDGET_MB overrides (in MB). Read once and
// cached (the budget is a process-wide policy, not a per-query knob).
size_t PinBudgetBytes() {
  static const size_t budget = [] {
    const char *env = std::getenv("GPU_OP_PIN_BUDGET_MB");
    if (env && *env) {
      char *end = nullptr;
      long long mb = std::strtoll(env, &end, 10);
      if (end != env && mb >= 0) { return static_cast<size_t>(mb) * 1024ull * 1024ull; }
    }
    return static_cast<size_t>(4096) * 1024ull * 1024ull;  // 4 GiB default cap
  }();
  return budget;
}

// Approximate resident footprint of a pinned column: the embedding matrix
// dominates (n*K * 4 fp32 / 2 fp16); add the per-pin scratch (q=K, dist=n_rows,
// plus the top-k candidate buffers). The candidate scratch sizing mirrors the
// Mojo `cand_cap = _topk_nblocks(n_rows, TOPK_MAX) * TOPK_MAX`; we approximate it
// conservatively so the budget never under-counts what is actually resident.
size_t PinFootprintBytes(idx_t n_rows, idx_t K, PinKind kind) {
  size_t elem = (kind == PinKind::FP16) ? 2 : 4;
  size_t emb = static_cast<size_t>(n_rows) * static_cast<size_t>(K) * elem;
  size_t scratch = static_cast<size_t>(K) * 4               // q_dev (fp32)
                 + static_cast<size_t>(n_rows) * 4;          // out_dev (fp32)
  // Top-k candidate scratch: nblocks (<= ceil(n_rows / something) but capped) at
  // TOPK_MAX=1024; cand_dist (4B) + cand_id (8B). Bound nblocks by ceil(n/256)+1
  // and TOPK_MAX so the estimate is an upper bound across kernels.
  static const size_t TOPK_MAX = 1024;
  size_t nblocks = static_cast<size_t>(n_rows) / 256 + 1;
  size_t cand = nblocks * TOPK_MAX * (4 + 8);
  return emb + scratch + cand;
}

// Evict the least-recently-used EVICTABLE (refcount == 0) entry. Returns true if
// one was freed, false if no evictable entry remains. Caller holds g_pin_mu.
bool EvictOneLRU() {
  auto victim = g_pins.end();
  uint64_t best = UINT64_MAX;
  for (auto it = g_pins.begin(); it != g_pins.end(); ++it) {
    if (it->second.refcount > 0) { continue; }  // in flight; never evict
    if (it->second.last_use < best) { best = it->second.last_use; victim = it; }
  }
  if (victim == g_pins.end()) { return false; }
  ResidentPin e = victim->second;
  g_pin_bytes_resident -= e.bytes;
  g_pins.erase(victim);
  FreeResidentPin(e);
  return true;
}

// RAII lease: marks a resident pin in-use for the lifetime of a query so a
// concurrent evict can't free its buffers. Constructed by EnsurePinned* (which
// increments refcount under the lock); the destructor decrements it under the
// lock and refreshes the LRU tick (the pin was just used). Move-only.
struct PinLease {
  std::string key;
  PinEntry entry{nullptr, 0, 0};
  PinLease() = default;
  PinLease(std::string k, PinEntry e) : key(std::move(k)), entry(e) {}
  PinLease(const PinLease &) = delete;
  PinLease &operator=(const PinLease &) = delete;
  PinLease(PinLease &&o) noexcept : key(std::move(o.key)), entry(o.entry) { o.entry.handle = nullptr; o.key.clear(); }
  PinLease &operator=(PinLease &&o) noexcept {
    if (this != &o) { release(); key = std::move(o.key); entry = o.entry; o.entry.handle = nullptr; o.key.clear(); }
    return *this;
  }
  ~PinLease() { release(); }
  void release() {
    if (key.empty()) { return; }
    std::lock_guard<std::mutex> g(g_pin_mu);
    auto it = g_pins.find(key);
    if (it != g_pins.end() && it->second.refcount > 0) {
      it->second.refcount--;
      it->second.last_use = ++g_pin_tick;  // refresh LRU: just used
    }
    key.clear();
    entry.handle = nullptr;
  }
};

// Materialize <column> from <table> into a contiguous n*K float host buffer.
// Validates the column is FLOAT[K] (ARRAY) and reports K + n_rows. Shared by the
// fp32 and fp16 pin paths so both upload identical host data. `who` is the
// caller name used in error messages.
void MaterializeFloatColumn(ClientContext &context, const std::string &table,
                            const std::string &column, const char *who,
                            vector<float> &host, idx_t &K, idx_t &n_rows) {
  Connection con(*context.db);
  auto res = con.Query("SELECT " + column + " FROM " + table);
  if (res->HasError()) { throw InvalidInputException(std::string(who) + ": " + res->GetError()); }
  if (res->types[0].id() != LogicalTypeId::ARRAY) {
    throw InvalidInputException(std::string(who) + ": column must be FLOAT[K] (ARRAY), got " +
                                res->types[0].ToString());
  }
  K = ArrayType::GetSize(res->types[0]);
  n_rows = 0;
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
}

// Pin a freshly-materialized host buffer, evicting LRU entries to stay within
// budget and retrying the device allocation if it OOMs. Returns the new handle
// (0 on failure even after evicting everything). Caller holds g_pin_mu.
void *PinWithBudget(const std::string &who, PinKind kind, const float *host,
                    idx_t n_rows, idx_t K, size_t &out_bytes) {
  size_t need = PinFootprintBytes(n_rows, K, kind);
  size_t budget = PinBudgetBytes();
  // Make room up-front: evict LRU evictable entries until this pin fits under the
  // budget (or nothing more is evictable — then we still try, OOM-retry catches
  // the actual device limit). budget == 0 means unbounded: skip pre-eviction.
  if (budget != 0) {
    while (g_pin_bytes_resident + need > budget) {
      if (!EvictOneLRU()) { break; }  // only in-use entries left; proceed and rely on OOM-retry
    }
  }
  auto do_pin = [&]() -> void * {
    if (kind == PinKind::FP16) {
      return reinterpret_cast<void *>(
          mojo_gpu_pin_f16(host, NumericCast<int64_t>(n_rows), NumericCast<int64_t>(K)));
    }
    return reinterpret_cast<void *>(
        mojo_gpu_pin(host, NumericCast<int64_t>(n_rows), NumericCast<int64_t>(K)));
  };
  void *handle = do_pin();
  // OOM-retry: a null handle means the device allocation failed. Evict the LRU
  // evictable entry and retry until it succeeds or nothing more can be evicted.
  while (!handle && EvictOneLRU()) { handle = do_pin(); }
  out_bytes = need;
  (void)who;
  return handle;
}

// Materialize + pin a column (fp32 or fp16) if not already resident, returning a
// PinLease that keeps it in-use (uneviGCtable) for the query's lifetime. Keyed by
// "table.column" (fp32) / "table.column#f16" (fp16) in the single LRU registry —
// the suffix keeps the two precisions from colliding. A column queried at both
// precisions ends up with two resident copies (accepted: the cost of supporting
// both without a re-upload). fp32 handles free with mojo_gpu_pin_free, fp16 with
// mojo_gpu_pin_free_f16 (FreeResidentPin dispatches by kind on eviction/unpin).
PinLease EnsurePinnedLeased(ClientContext &context, const std::string &table,
                            const std::string &column, PinKind kind, const char *who) {
  std::string key = (kind == PinKind::FP16) ? (table + "." + column + "#f16")
                                            : (table + "." + column);
  {
    std::lock_guard<std::mutex> g(g_pin_mu);
    auto it = g_pins.find(key);
    if (it != g_pins.end()) {
      it->second.refcount++;                  // in-use guard for this query
      it->second.last_use = ++g_pin_tick;     // LRU refresh
      return PinLease(key, PinEntry{it->second.handle, it->second.n_rows, it->second.K});
    }
  }
  // COLD: materialize OUTSIDE the lock (it issues a nested Connection::Query),
  // then re-lock to install. A concurrent pin of the same key may have raced us;
  // if so, drop our buffer and use theirs.
  vector<float> host;
  idx_t K = 0, n_rows = 0;
  MaterializeFloatColumn(context, table, column, who, host, K, n_rows);

  std::lock_guard<std::mutex> g(g_pin_mu);
  auto it = g_pins.find(key);
  if (it != g_pins.end()) {  // lost the race; reuse the winner
    it->second.refcount++;
    it->second.last_use = ++g_pin_tick;
    return PinLease(key, PinEntry{it->second.handle, it->second.n_rows, it->second.K});
  }
  size_t bytes = 0;
  void *handle = PinWithBudget(who, kind, host.data(), n_rows, K, bytes);
  if (!handle) { return PinLease(); }  // null lease -> caller throws
  ResidentPin e;
  e.handle = handle;
  e.n_rows = n_rows;
  e.K = K;
  e.kind = kind;
  e.bytes = bytes;
  e.last_use = ++g_pin_tick;
  e.refcount = 1;  // leased to the caller's query
  g_pin_bytes_resident += bytes;
  g_pins[key] = e;
  return PinLease(key, PinEntry{handle, n_rows, K});
}

// Backwards-compatible thin wrappers used by the cosine table functions. Each
// returns a PinLease; the caller MUST keep it alive until the GPU query finishes
// (the lease is the in-use guard against concurrent eviction).
PinLease EnsurePinned(ClientContext &context, const std::string &table, const std::string &column) {
  return EnsurePinnedLeased(context, table, column, PinKind::FP32, "gpu_cosine");
}

PinLease EnsurePinnedF16(ClientContext &context, const std::string &table,
                         const std::string &column) {
  return EnsurePinnedLeased(context, table, column, PinKind::FP16, "gpu_cosine_topk");
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

  // The lease keeps the resident pin in-use (un-evictable) until it goes out of
  // scope at the end of Bind — i.e. for the whole GPU query below.
  auto lease = EnsurePinned(context, table, column);
  auto &pe = lease.entry;
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

// ---------------------------------------------------------------------------
// gpu_cosine_topk() table function: pin-resident column cache + GPU top-k.
//
//   SELECT * FROM gpu_cosine_topk('emb', 'v', [..]::FLOAT[K], k);  -> (rowid, dist)
//
// Returns only the k nearest rows by cosine distance (ascending), exploiting the
// GPU top-k kernel so only k rows cross PCIe instead of all N. Reuses the same
// resident pin cache as gpu_cosine (keyed "table.column"); rowid is the scan
// position 0..N-1, matching gpu_cosine's numbering.
// ---------------------------------------------------------------------------
static constexpr int64_t GPU_TOPK_MAX = 1024;  // kernel's TOPK_MAX

struct GpuCosineTopkBindData : public TableFunctionData {
  vector<int64_t> ids;
  vector<float> dists;
  idx_t n_valid = 0;  // <= k, after dropping padded (id == -1) slots
};

unique_ptr<FunctionData> GpuCosineTopkBind(ClientContext &context, TableFunctionBindInput &input,
                                           vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  auto &qkids = ListValue::GetChildren(input.inputs[2]);
  auto k = input.inputs[3].GetValue<int64_t>();

  if (k < 1 || k > GPU_TOPK_MAX) {
    throw InvalidInputException("gpu_cosine_topk: k must be in [1, " + std::to_string(GPU_TOPK_MAX) +
                                "], got " + std::to_string(k));
  }

  // precision named parameter (default 'fp16'): 'fp16' -> half-precision resident
  // path (~1.6-1.8x faster, half the VRAM, recall >=0.99 on normalized
  // embeddings); 'fp32'/'exact' -> the full-precision EnsurePinned path. Unknown
  // values are rejected.
  bool use_fp16 = true;
  auto np = input.named_parameters.find("precision");
  if (np != input.named_parameters.end() && !np->second.IsNull()) {
    std::string prec = StringUtil::Lower(np->second.GetValue<string>());
    if (prec == "fp16" || prec == "half") {
      use_fp16 = true;
    } else if (prec == "fp32" || prec == "exact" || prec == "f32") {
      use_fp16 = false;
    } else {
      throw InvalidInputException("gpu_cosine_topk: unknown precision '" + prec +
                                  "' (expected 'fp16' or 'fp32')");
    }
  }

  PinLease lease = use_fp16 ? EnsurePinnedF16(context, table, column)
                            : EnsurePinned(context, table, column);
  auto &pe = lease.entry;
  if (!pe.handle) { throw InvalidInputException("gpu_cosine_topk: GPU pin failed"); }
  if (qkids.size() != pe.K) {
    throw InvalidInputException("gpu_cosine_topk: query length " + std::to_string(qkids.size()) +
                                " != column K " + std::to_string(pe.K));
  }
  vector<float> q;
  q.reserve(pe.K);
  for (auto &v : qkids) { q.push_back(v.GetValue<float>()); }

  auto bd = make_uniq<GpuCosineTopkBindData>();
  vector<int64_t> out_ids(NumericCast<idx_t>(k));
  vector<float> out_dists(NumericCast<idx_t>(k));
  int32_t rc = use_fp16
                   ? mojo_gpu_pin_query_topk_f16(pe.handle, q.data(), k, out_ids.data(), out_dists.data())
                   : mojo_gpu_pin_query_topk(pe.handle, q.data(), k, out_ids.data(), out_dists.data());
  if (rc != 0) {
    throw InvalidInputException("gpu_cosine_topk: GPU top-k query failed (rc " + std::to_string(rc) + ")");
  }
  // Drop padded slots (id == -1) when n_rows < k. Rows are already ascending.
  for (idx_t i = 0; i < NumericCast<idx_t>(k); i++) {
    if (out_ids[i] < 0) { continue; }
    bd->ids.push_back(out_ids[i]);
    bd->dists.push_back(out_dists[i]);
  }
  bd->n_valid = bd->ids.size();

  return_types = {LogicalType::BIGINT, LogicalType::FLOAT};
  names = {"rowid", "dist"};
  return std::move(bd);
}

void GpuCosineTopkFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuCosineTopkBindData>();
  auto &gs = data.global_state->Cast<GpuCosineTFGlobalState>();
  // n_valid <= k <= 1024 < STANDARD_VECTOR_SIZE, so this emits in a single chunk.
  idx_t n = MinValue<idx_t>(bd.n_valid - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  auto rowid = FlatVector::GetData<int64_t>(output.data[0]);
  auto dist = FlatVector::GetData<float>(output.data[1]);
  for (idx_t i = 0; i < n; i++) {
    rowid[i] = bd.ids[gs.offset + i];
    dist[i] = bd.dists[gs.offset + i];
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuCosineTopkTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_cosine_topk",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::LIST(LogicalType::FLOAT),
                    LogicalType::BIGINT},
                   GpuCosineTopkFunc, GpuCosineTopkBind, GpuCosineInit);
  // Optional precision selector: default 'fp16' (resident half-precision path);
  // 'fp32'/'exact' for full-precision exact distances.
  tf.named_parameters["precision"] = LogicalType::VARCHAR;
  loader.RegisterFunction(tf);
}

// ---------------------------------------------------------------------------
// gpu_cosine_topk_batch() table function: TRUE batched kNN. vss_join-style — the
// QUERY SET comes from a table column (also FLOAT[K] ARRAY), so M queries are
// scored against the resident emb matrix in ONE batched kernel call that reads
// the matrix once per query-tile (the regime where the GPU decisively beats a
// per-query index: HNSW can't use its index for batched queries at all).
//
//   SELECT * FROM gpu_cosine_topk_batch('emb','v','queries','qv', k [, precision]);
//     -> (query_rowid BIGINT, rowid BIGINT, dist FLOAT)
//
// query_rowid is the scan position of the query row in `query_table` (0..M-1);
// rowid is the scan position of the emb row (0..N-1), matching gpu_cosine_topk.
// Emits up to M*k rows (k nearest emb rows per query; fewer if N < k). The emb
// matrix is pinned + cached exactly like gpu_cosine_topk (fp16 default).
// ---------------------------------------------------------------------------
struct GpuCosineTopkBatchBindData : public TableFunctionData {
  vector<int64_t> query_rowids;  // row-major, one per emitted row
  vector<int64_t> ids;
  vector<float> dists;
  idx_t n_emitted = 0;
};

unique_ptr<FunctionData> GpuCosineTopkBatchBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  auto emb_table = input.inputs[0].GetValue<string>();
  auto emb_col = input.inputs[1].GetValue<string>();
  auto query_table = input.inputs[2].GetValue<string>();
  auto query_col = input.inputs[3].GetValue<string>();
  auto k = input.inputs[4].GetValue<int64_t>();

  if (k < 1 || k > GPU_TOPK_MAX) {
    throw InvalidInputException("gpu_cosine_topk_batch: k must be in [1, " + std::to_string(GPU_TOPK_MAX) +
                                "], got " + std::to_string(k));
  }

  bool use_fp16 = true;
  auto np = input.named_parameters.find("precision");
  if (np != input.named_parameters.end() && !np->second.IsNull()) {
    std::string prec = StringUtil::Lower(np->second.GetValue<string>());
    if (prec == "fp16" || prec == "half") {
      use_fp16 = true;
    } else if (prec == "fp32" || prec == "exact" || prec == "f32") {
      use_fp16 = false;
    } else {
      throw InvalidInputException("gpu_cosine_topk_batch: unknown precision '" + prec +
                                  "' (expected 'fp16' or 'fp32')");
    }
  }

  // Optional metric named param. Default cosine (0). L2 (1) = array_distance /
  // squared euclidean; ip (2) = inner-product (array_negative_inner_product /
  // -dot). Non-cosine is ONLY implemented on the FUSED tensor-core path
  // (fp16 + GPU_OP_TENSORCORE on an NVIDIA build + supported K/k); requesting it
  // with fp32 precision is a usage error (there is no scalar non-cosine path).
  int64_t metric = 0;
  auto mp = input.named_parameters.find("metric");
  if (mp != input.named_parameters.end() && !mp->second.IsNull()) {
    std::string m = StringUtil::Lower(mp->second.GetValue<string>());
    if (m == "cosine" || m == "array_cosine_distance") {
      metric = 0;
    } else if (m == "l2" || m == "euclidean" || m == "array_distance") {
      metric = 1;
    } else if (m == "ip" || m == "inner_product" || m == "dot" ||
               m == "array_negative_inner_product") {
      metric = 2;
    } else {
      throw InvalidInputException("gpu_cosine_topk_batch: unknown metric '" + m +
                                  "' (expected 'cosine', 'l2', or 'ip')");
    }
  }
  if (metric != 0 && !use_fp16) {
    throw InvalidInputException(
        "gpu_cosine_topk_batch: non-cosine metric requires precision 'fp16' "
        "(the fused tensor-core path); fp32 is cosine-only");
  }

  // Pin (or reuse the cached) resident emb matrix, same as gpu_cosine_topk. The
  // lease keeps it un-evictable for the whole batched query below.
  PinLease lease = use_fp16 ? EnsurePinnedF16(context, emb_table, emb_col)
                            : EnsurePinned(context, emb_table, emb_col);
  auto &pe = lease.entry;
  if (!pe.handle) { throw InvalidInputException("gpu_cosine_topk_batch: GPU pin failed"); }

  // Materialize the M query vectors from the query table column (FLOAT[K] ARRAY).
  vector<float> qs;
  idx_t qK = 0, M = 0;
  MaterializeFloatColumn(context, query_table, query_col, "gpu_cosine_topk_batch", qs, qK, M);
  if (qK != pe.K) {
    throw InvalidInputException("gpu_cosine_topk_batch: query dim " + std::to_string(qK) +
                                " != emb column K " + std::to_string(pe.K));
  }
  if (M == 0) {
    auto bd = make_uniq<GpuCosineTopkBatchBindData>();
    return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::FLOAT};
    names = {"query_rowid", "rowid", "dist"};
    return std::move(bd);
  }

  // ONE batched kernel call: M*k results row-major [query*k + slot].
  vector<int64_t> out_ids(NumericCast<idx_t>(M) * NumericCast<idx_t>(k));
  vector<float> out_dists(NumericCast<idx_t>(M) * NumericCast<idx_t>(k));
  int32_t rc;
  if (metric != 0) {
    // Non-cosine: fused tensor-core path only (fp16, enforced above).
    rc = mojo_gpu_pin_query_topk_batch_f16_metric(
        pe.handle, qs.data(), NumericCast<int64_t>(M), k, metric,
        out_ids.data(), out_dists.data());
  } else {
    rc = use_fp16
             ? mojo_gpu_pin_query_topk_batch_f16(pe.handle, qs.data(),
                                                 NumericCast<int64_t>(M), k,
                                                 out_ids.data(), out_dists.data())
             : mojo_gpu_pin_query_topk_batch(pe.handle, qs.data(),
                                             NumericCast<int64_t>(M), k,
                                             out_ids.data(), out_dists.data());
  }
  if (rc != 0) {
    if (metric != 0) {
      throw InvalidInputException(
          "gpu_cosine_topk_batch: non-cosine metric requires the fused "
          "tensor-core path (set GPU_OP_TENSORCORE=1 on an NVIDIA build with a "
          "supported K in {384,768,1024,1536} and k<=64); rc " +
          std::to_string(rc));
    }
    throw InvalidInputException("gpu_cosine_topk_batch: GPU batched top-k failed (rc " +
                                std::to_string(rc) + ")");
  }

  auto bd = make_uniq<GpuCosineTopkBatchBindData>();
  for (idx_t m = 0; m < M; m++) {
    for (idx_t j = 0; j < NumericCast<idx_t>(k); j++) {
      idx_t pos = m * NumericCast<idx_t>(k) + j;
      if (out_ids[pos] < 0) { continue; }  // padded slot (N < k)
      bd->query_rowids.push_back(NumericCast<int64_t>(m));
      bd->ids.push_back(out_ids[pos]);
      bd->dists.push_back(out_dists[pos]);
    }
  }
  bd->n_emitted = bd->ids.size();

  return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::FLOAT};
  names = {"query_rowid", "rowid", "dist"};
  return std::move(bd);
}

void GpuCosineTopkBatchFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuCosineTopkBatchBindData>();
  auto &gs = data.global_state->Cast<GpuCosineTFGlobalState>();
  idx_t n = MinValue<idx_t>(bd.n_emitted - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  auto qrow = FlatVector::GetData<int64_t>(output.data[0]);
  auto rowid = FlatVector::GetData<int64_t>(output.data[1]);
  auto dist = FlatVector::GetData<float>(output.data[2]);
  for (idx_t i = 0; i < n; i++) {
    qrow[i] = bd.query_rowids[gs.offset + i];
    rowid[i] = bd.ids[gs.offset + i];
    dist[i] = bd.dists[gs.offset + i];
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuCosineTopkBatchTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_cosine_topk_batch",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::BIGINT},
                   GpuCosineTopkBatchFunc, GpuCosineTopkBatchBind, GpuCosineInit);
  tf.named_parameters["precision"] = LogicalType::VARCHAR;
  // metric: 'cosine' (default), 'l2'/'euclidean' (array_distance), or 'ip'
  // (array_negative_inner_product). Non-cosine routes to the fused tensor-core
  // path (NVIDIA + GPU_OP_TENSORCORE + supported K/k); see GpuCosineTopkBatchBind.
  tf.named_parameters["metric"] = LogicalType::VARCHAR;
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
    // A LOGICAL_FILTER operator inside the join tree carries residual predicates
    // that are NOT pushed into any GET's table_filters (e.g. a cross-column
    // disjunction `p_size>40 OR p_retailprice<10`, or the OR-conjunction the
    // IN_CLAUSE rewrite leaves above a scan). SerializeMatchedPlan only reads
    // GET.table_filters and never serializes a LogicalFilter's expressions, so
    // descending past a non-empty filter would SILENTLY DROP its predicate and
    // produce a wrong aggregate. Bail to stock DuckDB CPU unless the filter is a
    // pure pass-through (no expressions). Verified: a Q14-shape join with a
    // residual `(l_quantity>30 OR l_extendedprice<1000)` builds a routable
    // descriptor with the filter dropped before this guard.
    if (!op->Cast<LogicalFilter>().expressions.empty()) { return false; }
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

// ===-------------------------------------------------------------------===//
// GPU-DIRECT NATIVE-STORAGE DECODE (debug-only entry points).
//
// Phase A: gpu_native_segment_info(table, column) -- run the in-process
//          table->bytes reachability call chain and emit per-segment metadata
//          (compression_type, mode, segment_start/count, block_id, and for the
//          first group: frame + width parsed straight out of the pinned bytes).
//          Proves byte reachability + that our byte-layout reading matches.
//
// Phase C: gpu_native_decode_check(table, column) -- pin each segment, decode
//          it on the GPU via mojo_gpu_decode_segment into a contiguous output at
//          segment_start, then compare every value against
//          Connection::Query("SELECT <column> FROM <table>") (unfiltered scan
//          order). Asserts every value is BIT-IDENTICAL.
//
// Both are additive, parallel to the existing CPU-materialize cold path; they
// do not touch it. They use DuckDB-internal storage APIs (CPP ABI).
// ===-------------------------------------------------------------------===//

// Resolve the {DuckTableEntry, storage index, logical type} for table.column.
struct NativeColumnRef {
  DuckTableEntry *table = nullptr;
  StorageIndex storage_index;
  LogicalType type;
  idx_t physical_type_size = 0;
};

NativeColumnRef ResolveNativeColumn(ClientContext &context, const std::string &table_name,
                                    const std::string &column_name) {
  auto &entry = Catalog::GetEntry<TableCatalogEntry>(context, INVALID_CATALOG, DEFAULT_SCHEMA, table_name);
  auto &duck_table = entry.Cast<DuckTableEntry>();

  std::string col = column_name;
  auto logical = entry.GetColumnIndex(col, /*if_exists=*/true);
  if (!logical.IsValid()) {
    throw InvalidInputException("gpu_native: column '" + column_name + "' not found in '" + table_name + "'");
  }
  auto storage_index = entry.GetStorageIndex(ColumnIndex(logical.index));
  const auto &cdef = entry.GetColumn(logical);

  NativeColumnRef ref;
  ref.table = &duck_table;
  ref.storage_index = storage_index;
  ref.type = cdef.GetType();
  ref.physical_type_size = GetTypeIdSize(ref.type.InternalType());
  return ref;
}

// One decoded-ready segment: the codec + a host pointer to the segment's raw
// bytes (kept alive by the BufferHandle held in the caller's vector).
struct PinnedSegment {
  ColumnSegmentInfo info;        // metadata (compression_type, mode string, ...)
  const_data_ptr_t base = nullptr;  // pointer to the segment's first byte
  idx_t seg_bytes = 0;           // SegmentSize() of the persistent segment
  unique_ptr<ColumnSegment> segment;  // owns nothing block-wise; keeps type alive
  BufferHandle handle;           // KEEP ALIVE: the pinned block backing `base`
};

// Map a DuckDB CompressionType to the Mojo decode `codec` arg (0/1) or -1 if
// this codec is not handled by the Phase-B kernels.
int CodecToMojo(CompressionType ct) {
  switch (ct) {
  case CompressionType::COMPRESSION_UNCOMPRESSED:
    return 0;
  case CompressionType::COMPRESSION_BITPACKING:
    return 1;
  default:
    return -1;
  }
}

// type_code for the Mojo export: 0=int32 (4B), 1=int64 (8B). -1 if unsupported.
int TypeCodeForSize(idx_t type_size) {
  if (type_size == 4) return 0;
  if (type_size == 8) return 1;
  return -1;
}

// Enumerate + pin every persistent data segment of a column, in scan order.
// Calls `fn(seg_index, PinnedSegment&)` for each. The BufferHandle inside each
// PinnedSegment stays alive only within this call, so `fn` must consume `base`
// before returning (decode / parse inline).
template <typename FN>
void ForEachColumnSegment(ClientContext &context, NativeColumnRef &ref, FN &&fn) {
  auto &storage = ref.table->GetStorage();
  auto &row_groups = *storage.GetRowGroupCollection();
  auto &attached = storage.GetAttached();
  auto &db = attached.GetDatabase();
  auto &block_manager = attached.GetStorageManager().GetBlockManager();
  auto &buffer_manager = BufferManager::GetBufferManager(context);

  idx_t global_seg_index = 0;
  idx_t rg_count = row_groups.GetRowGroupCount();
  for (idx_t rg = 0; rg < rg_count; rg++) {
    auto row_group = row_groups.GetRowGroup(NumericCast<int64_t>(rg));
    if (!row_group) continue;
    auto &column_data = row_group->GetRawColumnData(ref.storage_index);

    vector<ColumnSegmentInfo> seg_infos;
    column_data.GetColumnSegmentInfo(QueryContext(context), rg, {ref.storage_index.GetPrimaryIndex()},
                                     seg_infos);

    for (auto &si : seg_infos) {
      PinnedSegment ps;
      ps.info = si;
      // CONSTANT segments live in-memory (block_id < 0): no on-disk bytes to
      // pin. Skip them here -- they are decoded purely from stats by the
      // bitpacking CONSTANT path only when bitpacked; a top-level CONSTANT
      // compression segment has no packed bytes. Hand them to `fn` with a null
      // base so the caller can handle (Phase A prints; Phase C is told).
      auto ct = EnumUtil::FromString<CompressionType>(si.compression_type.c_str());
      if (si.persistent && si.block_id >= 0) {
        auto segment = ColumnSegment::CreatePersistentSegment(
            db, block_manager, si.block_id, si.block_offset, ref.type,
            si.segment_count, ct, BaseStatistics::CreateEmpty(ref.type),
            /*segment_state=*/nullptr);
        ps.seg_bytes = segment->SegmentSize();
        ps.handle = buffer_manager.Pin(segment->block);
        ps.base = ps.handle.Ptr() + segment->GetBlockOffset();
        ps.segment = std::move(segment);
      }
      fn(global_seg_index, ps);
      global_seg_index++;
    }
  }
}

// --- Phase A: gpu_native_segment_info(table, column) --------------------------
struct GpuNativeSegInfoBindData : public TableFunctionData {
  struct Row {
    idx_t segment_index;
    string compression_type;
    string mode;            // group-0 mode string (e.g. "FOR") or segment_info
    idx_t segment_start;
    idx_t segment_count;
    int64_t block_id;
    int64_t frame0;         // group-0 frame (FOR) or constant value, else 0
    int64_t width0;         // group-0 bit width (FOR), else -1
  };
  vector<Row> rows;
};

struct GpuNativeTFState : public GlobalTableFunctionState {
  idx_t offset = 0;
  idx_t MaxThreads() const override { return 1; }
};

// Parse one metadata group's mode/frame/width/data_off directly out of the
// pinned bytes, matching the Mojo kernel's reader exactly (used by the debug
// entry points to prove byte agreement). `group_idx` selects the group.
void ParseBitpackingGroup(const_data_ptr_t base, idx_t seg_bytes, idx_t type_size, idx_t group_idx,
                          string &out_mode, int64_t &out_frame, int64_t &out_width,
                          int64_t &out_data_off) {
  out_mode = "?"; out_frame = 0; out_width = -1; out_data_off = -1;
  if (!base || seg_bytes < 8) return;
  uint64_t metadata_end = 0;
  std::memcpy(&metadata_end, base, sizeof(uint64_t));
  if (metadata_end < 8 + (group_idx + 1) * 4 || metadata_end > seg_bytes) { out_mode = "BAD_META"; return; }
  uint32_t encoded = 0;
  std::memcpy(&encoded, base + metadata_end - (group_idx + 1) * 4, sizeof(uint32_t));
  uint32_t data_off = encoded & 0x00FFFFFFu;
  uint32_t pmode = (encoded >> 24) & 0xFFu;
  out_data_off = static_cast<int64_t>(data_off);
  // BitpackingMode: 0 INVALID,1 AUTO,2 CONSTANT,3 CONSTANT_DELTA,4 DELTA_FOR,5 FOR
  static const char *kModeNames[] = {"INVALID", "AUTO", "CONSTANT", "CONSTANT_DELTA",
                                     "DELTA_FOR", "FOR"};
  out_mode = (pmode <= 5) ? kModeNames[pmode] : "UNKNOWN";
  if (data_off + 2 * type_size > metadata_end) return;
  auto rd = [&](idx_t off) -> int64_t {
    int64_t v = 0;
    if (type_size == 4) { int32_t t; std::memcpy(&t, base + off, 4); v = t; }
    else { std::memcpy(&v, base + off, 8); }
    return v;
  };
  if (pmode == 5 /*FOR*/) {
    out_frame = rd(data_off);
    out_width = static_cast<int64_t>(base[data_off + type_size]);  // width byte
  } else if (pmode == 2 /*CONSTANT*/) {
    out_frame = rd(data_off);
  }
}

void ParseBitpackingGroup0(const_data_ptr_t base, idx_t seg_bytes, idx_t type_size,
                           string &out_mode, int64_t &out_frame, int64_t &out_width) {
  int64_t doff;
  ParseBitpackingGroup(base, seg_bytes, type_size, 0, out_mode, out_frame, out_width, doff);
}

unique_ptr<FunctionData> GpuNativeSegInfoBind(ClientContext &context, TableFunctionBindInput &input,
                                              vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  auto ref = ResolveNativeColumn(context, table, column);

  auto bd = make_uniq<GpuNativeSegInfoBindData>();
  ForEachColumnSegment(context, ref, [&](idx_t idx, PinnedSegment &ps) {
    GpuNativeSegInfoBindData::Row r;
    r.segment_index = idx;
    r.compression_type = ps.info.compression_type;
    r.segment_start = ps.info.segment_start;
    r.segment_count = ps.info.segment_count;
    r.block_id = ps.info.block_id;
    r.frame0 = 0; r.width0 = -1; r.mode = ps.info.segment_info;
    if (ps.base && ps.info.compression_type == "BitPacking") {
      string m; int64_t f, w;
      ParseBitpackingGroup0(ps.base, ps.seg_bytes, ref.physical_type_size, m, f, w);
      r.mode = m; r.frame0 = f; r.width0 = w;
    }
    bd->rows.push_back(std::move(r));
  });

  return_types = {LogicalType::BIGINT, LogicalType::VARCHAR, LogicalType::VARCHAR,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT};
  names = {"segment_index", "compression_type", "mode", "segment_start",
           "segment_count", "block_id", "frame0", "width0"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> GpuNativeTFInit(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<GpuNativeTFState>();
}

void GpuNativeSegInfoFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuNativeSegInfoBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  idx_t n = MinValue<idx_t>(bd.rows.size() - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  auto c_idx = FlatVector::GetData<int64_t>(output.data[0]);
  auto c_start = FlatVector::GetData<int64_t>(output.data[3]);
  auto c_count = FlatVector::GetData<int64_t>(output.data[4]);
  auto c_block = FlatVector::GetData<int64_t>(output.data[5]);
  auto c_frame = FlatVector::GetData<int64_t>(output.data[6]);
  auto c_width = FlatVector::GetData<int64_t>(output.data[7]);
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    c_idx[i] = NumericCast<int64_t>(r.segment_index);
    output.data[1].SetValue(i, Value(r.compression_type));
    output.data[2].SetValue(i, Value(r.mode));
    c_start[i] = NumericCast<int64_t>(r.segment_start);
    c_count[i] = NumericCast<int64_t>(r.segment_count);
    c_block[i] = r.block_id;
    c_frame[i] = r.frame0;
    c_width[i] = r.width0;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuNativeSegInfoTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_native_segment_info", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                   GpuNativeSegInfoFunc, GpuNativeSegInfoBind, GpuNativeTFInit);
  loader.RegisterFunction(tf);
}

// --- Debug: gpu_native_group_dump(table, column) ------------------------------
// One row per 2048-row metadata group of every DATA segment, parsed straight
// from the pinned bytes (mode/width/frame/data_off + seg_bytes). Used to pin
// down per-group decode discrepancies.
struct GpuNativeGroupDumpBindData : public TableFunctionData {
  struct Row {
    idx_t seg_index, group_idx, group_rows, global_start, seg_bytes;
    string mode; int64_t frame, width, data_off;
  };
  vector<Row> rows;
};

unique_ptr<FunctionData> GpuNativeGroupDumpBind(ClientContext &context, TableFunctionBindInput &input,
                                                vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  auto ref = ResolveNativeColumn(context, table, column);
  auto bd = make_uniq<GpuNativeGroupDumpBindData>();
  idx_t global_row = 0;
  ForEachColumnSegment(context, ref, [&](idx_t idx, PinnedSegment &ps) {
    if (ps.info.column_path.find(',') != std::string::npos) return;  // data only
    idx_t count = ps.info.segment_count;
    idx_t seg_global = global_row;
    global_row += count;
    if (!ps.base || ps.info.compression_type != "BitPacking") return;
    idx_t n_groups = (count + 2048 - 1) / 2048;
    for (idx_t g = 0; g < n_groups; g++) {
      idx_t grows = (g + 1 < n_groups) ? 2048 : count - g * 2048;
      GpuNativeGroupDumpBindData::Row r;
      r.seg_index = idx; r.group_idx = g; r.group_rows = grows;
      r.global_start = seg_global + g * 2048; r.seg_bytes = ps.seg_bytes;
      string m; int64_t f, w, doff;
      ParseBitpackingGroup(ps.base, ps.seg_bytes, ref.physical_type_size, g, m, f, w, doff);
      r.mode = m; r.frame = f; r.width = w; r.data_off = doff;
      bd->rows.push_back(std::move(r));
    }
  });
  return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
  names = {"seg_index", "group_idx", "group_rows", "global_start", "mode", "frame", "width", "data_off"};
  return std::move(bd);
}

void GpuNativeGroupDumpFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuNativeGroupDumpBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  idx_t n = MinValue<idx_t>(bd.rows.size() - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    FlatVector::GetData<int64_t>(output.data[0])[i] = NumericCast<int64_t>(r.seg_index);
    FlatVector::GetData<int64_t>(output.data[1])[i] = NumericCast<int64_t>(r.group_idx);
    FlatVector::GetData<int64_t>(output.data[2])[i] = NumericCast<int64_t>(r.group_rows);
    FlatVector::GetData<int64_t>(output.data[3])[i] = NumericCast<int64_t>(r.global_start);
    output.data[4].SetValue(i, Value(r.mode));
    FlatVector::GetData<int64_t>(output.data[5])[i] = r.frame;
    FlatVector::GetData<int64_t>(output.data[6])[i] = r.width;
    FlatVector::GetData<int64_t>(output.data[7])[i] = r.data_off;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuNativeGroupDumpTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_native_group_dump", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                   GpuNativeGroupDumpFunc, GpuNativeGroupDumpBind, GpuNativeTFInit);
  loader.RegisterFunction(tf);
}

// --- Phase C: gpu_native_decode_check(table, column) --------------------------
// Decodes every supported segment on the GPU and compares against the CPU scan.
struct GpuNativeDecodeCheckBindData : public TableFunctionData {
  int64_t total_rows = 0;
  int64_t checked_rows = 0;
  int64_t mismatches = 0;
  int64_t skipped_segments = 0;
  int64_t first_mismatch_row = -1;
  int64_t first_got = 0;
  int64_t first_want = 0;
  int64_t deferred_rows = 0;   // rows in DELTA_FOR / CONSTANT_DELTA / unknown groups (not yet implemented)
  string status;
};

unique_ptr<FunctionData> GpuNativeDecodeCheckBind(ClientContext &context, TableFunctionBindInput &input,
                                                  vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  auto ref = ResolveNativeColumn(context, table, column);

  auto bd = make_uniq<GpuNativeDecodeCheckBindData>();
  int type_code = TypeCodeForSize(ref.physical_type_size);
  if (type_code < 0) {
    bd->status = "unsupported physical type size " + std::to_string(ref.physical_type_size);
    return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                    LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                    LogicalType::BIGINT};
    names = {"status", "checked_rows", "mismatches", "first_mismatch_row", "skipped_segments",
             "first_got", "first_want", "deferred_rows"};
    return std::move(bd);
  }

  // CPU reference: materialize the column in unfiltered scan order as the
  // physical storage integer (int32 for 4B types like DATE/INTEGER, int64 for
  // 8B). We compare against the storage representation the kernel produces.
  Connection con(*context.db);
  auto res = con.Query("SELECT " + column + " FROM " + table);
  if (res->HasError()) { throw InvalidInputException("gpu_native_decode_check: " + res->GetError()); }
  vector<int64_t> reference;  // store as int64 regardless; compare per-width
  while (true) {
    auto chunk = res->Fetch();
    if (!chunk || chunk->size() == 0) break;
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    if (type_code == 0) {
      // 4B physical: DATE -> int32 days, INTEGER -> int32. Read via the column's
      // physical layout. We re-Flatten to a typed pointer of the right width.
      auto pt = ref.type.InternalType();
      if (pt == PhysicalType::INT32) {
        auto d = FlatVector::GetData<int32_t>(chunk->data[0]);
        for (idx_t i = 0; i < n; i++) reference.push_back(d[i]);
      } else {
        throw InvalidInputException("gpu_native_decode_check: 4B but not INT32 physical");
      }
    } else {
      auto pt = ref.type.InternalType();
      if (pt == PhysicalType::INT64) {
        auto d = FlatVector::GetData<int64_t>(chunk->data[0]);
        for (idx_t i = 0; i < n; i++) reference.push_back(d[i]);
      } else {
        throw InvalidInputException("gpu_native_decode_check: 8B but not INT64 physical");
      }
    }
  }
  bd->total_rows = NumericCast<int64_t>(reference.size());

  // GPU decode per segment into a contiguous output at the GLOBAL row offset,
  // comparing each decoded value against the reference.
  //
  // Two subtleties (verified against duckdb storage/table/column_data.cpp):
  //   * GetColumnSegmentInfo recurses into the validity child, which appears as
  //     a separate ColumnSegmentInfo with column_path "[N, 0]" (the data column
  //     is "[N]", no comma). We decode ONLY the data column's segments.
  //   * `segment_start` is row-group-relative (it resets each row group), so we
  //     track our own running GLOBAL offset across accepted data segments.
  vector<int32_t> dec32;
  vector<int64_t> dec64;
  idx_t global_row = 0;
  ForEachColumnSegment(context, ref, [&](idx_t /*idx*/, PinnedSegment &ps) {
    // Skip validity (or any nested-child) segments: only the top-level data
    // column path "[N]" has no comma.
    if (ps.info.column_path.find(',') != std::string::npos) { return; }
    int codec = CodecToMojo(EnumUtil::FromString<CompressionType>(ps.info.compression_type.c_str()));
    idx_t count = ps.info.segment_count;
    idx_t start = global_row;     // GLOBAL output offset (running)
    global_row += count;          // advance regardless, so offsets stay aligned
    if (!ps.base || codec < 0) {
      bd->skipped_segments++;
      return;  // not a pinnable/handled segment (e.g. top-level CONSTANT)
    }
    // For BITPACKING segments, classify each 2048-group's mode so we can
    // attribute the (expected) zero-filled DELTA_FOR / CONSTANT_DELTA / unknown
    // groups to `deferred_rows` rather than counting them as decode failures.
    // A `deferred` row is one whose group mode is NOT yet implemented (the
    // kernel deterministically zero-fills it). FOR/CONSTANT groups must match
    // bit-exact -- a mismatch there is a real bug.
    auto group_deferred = [&](idx_t row_in_seg) -> bool {
      if (codec != 1) return false;  // UNCOMPRESSED is always implemented
      idx_t g = row_in_seg / 2048;
      string m; int64_t f, w, doff;
      ParseBitpackingGroup(ps.base, ps.seg_bytes, ref.physical_type_size, g, m, f, w, doff);
      return !(m == "FOR" || m == "CONSTANT");
    };

    int32_t rc;
    if (type_code == 0) {
      dec32.assign(count, 0);
      rc = mojo_gpu_decode_segment(ps.base, NumericCast<int64_t>(ps.seg_bytes),
                                   NumericCast<int64_t>(count), codec, 0, dec32.data());
      if (rc != 0) { bd->skipped_segments++; return; }
      for (idx_t i = 0; i < count; i++) {
        int64_t want = reference[start + i];
        bool match = (static_cast<int64_t>(dec32[i]) == want);
        if (!match) {
          if (group_deferred(i)) { bd->deferred_rows++; }
          else {
            if (bd->first_mismatch_row < 0) {
              bd->first_mismatch_row = NumericCast<int64_t>(start + i);
              bd->first_got = dec32[i]; bd->first_want = want;
            }
            bd->mismatches++;
          }
        }
        bd->checked_rows++;
      }
    } else {
      dec64.assign(count, 0);
      rc = mojo_gpu_decode_segment(ps.base, NumericCast<int64_t>(ps.seg_bytes),
                                   NumericCast<int64_t>(count), codec, 1, dec64.data());
      if (rc != 0) { bd->skipped_segments++; return; }
      for (idx_t i = 0; i < count; i++) {
        int64_t want = reference[start + i];
        bool match = (dec64[i] == want);
        if (!match) {
          if (group_deferred(i)) { bd->deferred_rows++; }
          else {
            if (bd->first_mismatch_row < 0) {
              bd->first_mismatch_row = NumericCast<int64_t>(start + i);
              bd->first_got = dec64[i]; bd->first_want = want;
            }
            bd->mismatches++;
          }
        }
        bd->checked_rows++;
      }
    }
  });

  int64_t implemented = bd->checked_rows - bd->deferred_rows;
  if (bd->checked_rows == 0) {
    bd->status = "NO_SEGMENTS_DECODED";
  } else if (bd->mismatches != 0) {
    bd->status = "FAIL: " + std::to_string(bd->mismatches) + " mismatches in implemented (FOR/CONSTANT) groups";
  } else if (bd->deferred_rows == 0) {
    bd->status = "PASS: " + std::to_string(bd->checked_rows) + " values bit-identical";
  } else {
    bd->status = "PASS (partial): " + std::to_string(implemented) +
                 " FOR/CONSTANT values bit-identical; " + std::to_string(bd->deferred_rows) +
                 " rows in deferred DELTA_FOR/CONSTANT_DELTA groups (zero-filled, not yet implemented)";
  }

  return_types = {LogicalType::VARCHAR, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT};
  names = {"status", "checked_rows", "mismatches", "first_mismatch_row", "skipped_segments",
           "first_got", "first_want", "deferred_rows"};
  return std::move(bd);
}

void GpuNativeDecodeCheckFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuNativeDecodeCheckBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  if (gs.offset > 0) { output.SetCardinality(0); return; }
  output.data[0].SetValue(0, Value(bd.status));
  FlatVector::GetData<int64_t>(output.data[1])[0] = bd.checked_rows;
  FlatVector::GetData<int64_t>(output.data[2])[0] = bd.mismatches;
  FlatVector::GetData<int64_t>(output.data[3])[0] = bd.first_mismatch_row;
  FlatVector::GetData<int64_t>(output.data[4])[0] = bd.skipped_segments;
  FlatVector::GetData<int64_t>(output.data[5])[0] = bd.first_got;
  FlatVector::GetData<int64_t>(output.data[6])[0] = bd.first_want;
  FlatVector::GetData<int64_t>(output.data[7])[0] = bd.deferred_rows;
  output.SetCardinality(1);
  gs.offset = 1;
}

void RegisterGpuNativeDecodeCheckTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_native_decode_check", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                   GpuNativeDecodeCheckFunc, GpuNativeDecodeCheckBind, GpuNativeTFInit);
  loader.RegisterFunction(tf);
}

// ===-------------------------------------------------------------------===//
// Explicit pin control + observability table functions (feature #4).
//
//   gpu_pin_status()                      -> resident pins, for observability
//   gpu_unpin(key)                        -> free one resident pin by key
//   gpu_unpin_all()                       -> free all (evictable) resident pins
//   gpu_pin_table(table, column [, prec])  -> pre-pin a kNN embedding column warm
//
// Names mirror the existing gpu_cosine* / gpu_native_* table functions. unpin is
// always correctness-safe (a later query simply rebuilds COLD); in-use pins
// (refcount > 0, a query in flight) are skipped and reported, never force-freed.
// ===-------------------------------------------------------------------===//
struct GpuPinStatusBindData : public TableFunctionData {
  struct Row { string key; string kind; int64_t bytes; int64_t n_rows; int64_t K;
               int64_t last_use_age; int64_t in_use; };
  vector<Row> rows;
  int64_t budget_mb = 0;
  int64_t resident_mb = 0;
};

unique_ptr<FunctionData> GpuPinStatusBind(ClientContext &, TableFunctionBindInput &,
                                          vector<LogicalType> &return_types, vector<string> &names) {
  auto bd = make_uniq<GpuPinStatusBindData>();
  {
    std::lock_guard<std::mutex> g(g_pin_mu);
    for (auto &kv : g_pins) {
      GpuPinStatusBindData::Row r;
      r.key = kv.first;
      r.kind = (kv.second.kind == PinKind::FP16) ? "fp16" : "fp32";
      r.bytes = NumericCast<int64_t>(kv.second.bytes);
      r.n_rows = NumericCast<int64_t>(kv.second.n_rows);
      r.K = NumericCast<int64_t>(kv.second.K);
      // last_use_age: ticks since this entry was last touched (0 = most recent).
      r.last_use_age = NumericCast<int64_t>(g_pin_tick - kv.second.last_use);
      r.in_use = kv.second.refcount;
      bd->rows.push_back(std::move(r));
    }
    bd->resident_mb = NumericCast<int64_t>(g_pin_bytes_resident / (1024ull * 1024ull));
  }
  bd->budget_mb = NumericCast<int64_t>(PinBudgetBytes() / (1024ull * 1024ull));
  return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT};
  names = {"key", "kind", "bytes", "rows", "K", "last_use_age", "in_use",
           "resident_mb", "budget_mb"};
  return std::move(bd);
}

void GpuPinStatusFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuPinStatusBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  idx_t n = MinValue<idx_t>(bd.rows.size() - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    output.data[0].SetValue(i, Value(r.key));
    output.data[1].SetValue(i, Value(r.kind));
    FlatVector::GetData<int64_t>(output.data[2])[i] = r.bytes;
    FlatVector::GetData<int64_t>(output.data[3])[i] = r.n_rows;
    FlatVector::GetData<int64_t>(output.data[4])[i] = r.K;
    FlatVector::GetData<int64_t>(output.data[5])[i] = r.last_use_age;
    FlatVector::GetData<int64_t>(output.data[6])[i] = r.in_use;
    FlatVector::GetData<int64_t>(output.data[7])[i] = bd.resident_mb;
    FlatVector::GetData<int64_t>(output.data[8])[i] = bd.budget_mb;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuPinStatusTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_pin_status", {}, GpuPinStatusFunc, GpuPinStatusBind, GpuNativeTFInit);
  loader.RegisterFunction(tf);
}

// gpu_unpin(key) / gpu_unpin_all(): free resident pins. Returns one row
// (freed BIGINT, skipped_in_use BIGINT). Skips in-use entries (a query holds a
// lease) — they are reported in skipped_in_use, never force-freed.
struct GpuUnpinBindData : public TableFunctionData {
  int64_t freed = 0;
  int64_t skipped_in_use = 0;
};

unique_ptr<FunctionData> GpuUnpinBindImpl(ClientContext &, TableFunctionBindInput &input,
                                          vector<LogicalType> &return_types, vector<string> &names,
                                          bool all) {
  auto bd = make_uniq<GpuUnpinBindData>();
  std::lock_guard<std::mutex> g(g_pin_mu);
  if (all) {
    for (auto it = g_pins.begin(); it != g_pins.end();) {
      if (it->second.refcount > 0) { bd->skipped_in_use++; ++it; continue; }
      ResidentPin e = it->second;
      g_pin_bytes_resident -= e.bytes;
      it = g_pins.erase(it);
      FreeResidentPin(e);
      bd->freed++;
    }
  } else {
    auto key = input.inputs[0].GetValue<string>();
    auto it = g_pins.find(key);
    if (it != g_pins.end()) {
      if (it->second.refcount > 0) {
        bd->skipped_in_use++;
      } else {
        ResidentPin e = it->second;
        g_pin_bytes_resident -= e.bytes;
        g_pins.erase(it);
        FreeResidentPin(e);
        bd->freed++;
      }
    }
  }
  return_types = {LogicalType::BIGINT, LogicalType::BIGINT};
  names = {"freed", "skipped_in_use"};
  return std::move(bd);
}

unique_ptr<FunctionData> GpuUnpinBind(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
  return GpuUnpinBindImpl(context, input, return_types, names, /*all=*/false);
}
unique_ptr<FunctionData> GpuUnpinAllBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
  return GpuUnpinBindImpl(context, input, return_types, names, /*all=*/true);
}

void GpuUnpinFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuUnpinBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  if (gs.offset > 0) { output.SetCardinality(0); return; }
  FlatVector::GetData<int64_t>(output.data[0])[0] = bd.freed;
  FlatVector::GetData<int64_t>(output.data[1])[0] = bd.skipped_in_use;
  output.SetCardinality(1);
  gs.offset = 1;
}

void RegisterGpuUnpinTableFunctions(ExtensionLoader &loader) {
  TableFunction tf("gpu_unpin", {LogicalType::VARCHAR}, GpuUnpinFunc, GpuUnpinBind, GpuNativeTFInit);
  loader.RegisterFunction(tf);
  TableFunction tfa("gpu_unpin_all", {}, GpuUnpinFunc, GpuUnpinAllBind, GpuNativeTFInit);
  loader.RegisterFunction(tfa);
}

// gpu_pin_table(table, column [, precision]): pre-pin a kNN embedding column so
// the first gpu_cosine* query against it is warm. Materializes + uploads under
// the same budget/LRU path; the lease is dropped immediately after binding (no
// query runs here) so the entry stays resident and evictable. Returns one row
// (key VARCHAR, kind VARCHAR, rows BIGINT, K BIGINT, bytes BIGINT).
struct GpuPinTableBindData : public TableFunctionData {
  string key, kind;
  int64_t n_rows = 0, K = 0, bytes = 0;
};

unique_ptr<FunctionData> GpuPinTableBind(ClientContext &context, TableFunctionBindInput &input,
                                         vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  auto column = input.inputs[1].GetValue<string>();
  bool use_fp16 = true;  // default fp16, matching gpu_cosine_topk*
  if (input.inputs.size() >= 3 && !input.inputs[2].IsNull()) {
    std::string prec = StringUtil::Lower(input.inputs[2].GetValue<string>());
    if (prec == "fp16" || prec == "half") {
      use_fp16 = true;
    } else if (prec == "fp32" || prec == "exact" || prec == "f32") {
      use_fp16 = false;
    } else {
      throw InvalidInputException("gpu_pin_table: unknown precision '" + prec +
                                  "' (expected 'fp16' or 'fp32')");
    }
  }
  PinKind kind = use_fp16 ? PinKind::FP16 : PinKind::FP32;
  // EnsurePinnedLeased materializes + pins (or reuses); the lease drops at the
  // end of this scope, leaving the entry resident + evictable.
  {
    PinLease lease = EnsurePinnedLeased(context, table, column, kind, "gpu_pin_table");
    if (!lease.entry.handle) { throw InvalidInputException("gpu_pin_table: GPU pin failed"); }
  }
  auto bd = make_uniq<GpuPinTableBindData>();
  bd->key = use_fp16 ? (table + "." + column + "#f16") : (table + "." + column);
  {
    std::lock_guard<std::mutex> g(g_pin_mu);
    auto it = g_pins.find(bd->key);
    if (it != g_pins.end()) {
      bd->kind = (it->second.kind == PinKind::FP16) ? "fp16" : "fp32";
      bd->n_rows = NumericCast<int64_t>(it->second.n_rows);
      bd->K = NumericCast<int64_t>(it->second.K);
      bd->bytes = NumericCast<int64_t>(it->second.bytes);
    }
  }
  return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT};
  names = {"key", "kind", "rows", "K", "bytes"};
  return std::move(bd);
}

void GpuPinTableFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuPinTableBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  if (gs.offset > 0) { output.SetCardinality(0); return; }
  output.data[0].SetValue(0, Value(bd.key));
  output.data[1].SetValue(0, Value(bd.kind));
  FlatVector::GetData<int64_t>(output.data[2])[0] = bd.n_rows;
  FlatVector::GetData<int64_t>(output.data[3])[0] = bd.K;
  FlatVector::GetData<int64_t>(output.data[4])[0] = bd.bytes;
  output.SetCardinality(1);
  gs.offset = 1;
}

void RegisterGpuPinTableTableFunction(ExtensionLoader &loader) {
  // 2-arg (default fp16) and 3-arg (explicit precision) overloads.
  TableFunction tf2("gpu_pin_table", {LogicalType::VARCHAR, LogicalType::VARCHAR},
                    GpuPinTableFunc, GpuPinTableBind, GpuNativeTFInit);
  loader.RegisterFunction(tf2);
  TableFunction tf3("gpu_pin_table",
                    {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR},
                    GpuPinTableFunc, GpuPinTableBind, GpuNativeTFInit);
  loader.RegisterFunction(tf3);
}

void LoadInternal(ExtensionLoader &loader) {
  mojo_gpu_ctx_init();                                 // pay the ~32 ms DeviceContext init once, at LOAD
  RegisterGpuOperator(loader.GetDatabaseInstance());  // transparent cosine operator
  RegisterGpuCosineTableFunction(loader);             // pin-resident cosine TF
  RegisterGpuCosineTopkTableFunction(loader);         // pin-resident cosine top-k TF
  RegisterGpuCosineTopkBatchTableFunction(loader);    // batched (table-of-queries) cosine top-k TF
  RegisterGpuNativeSegInfoTableFunction(loader);      // Phase A: native-storage segment reachability
  RegisterGpuNativeGroupDumpTableFunction(loader);    // debug: per-group mode/width dump
  RegisterGpuNativeDecodeCheckTableFunction(loader);  // Phase C: GPU-direct decode bit-exact check
  RegisterGpuPinStatusTableFunction(loader);          // pin cache observability (resident pins)
  RegisterGpuUnpinTableFunctions(loader);             // explicit unpin (key / all)
  RegisterGpuPinTableTableFunction(loader);           // pre-pin a kNN embedding column warm
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
