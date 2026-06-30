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
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_columnref_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/table_filter.hpp"
#include "duckdb/parser/constraints/not_null_constraint.hpp"
#include "duckdb/parser/constraints/unique_constraint.hpp"
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
#include <cctype>
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
// GPU_OP_TRANSCENDENTAL: 1 if the descriptor has a transcendental aggregate
// (sqrt/exp/ln/log10/sin/cos in any metric program). Enables routing for an
// otherwise KIND_UNKNOWN transcendental (e.g. a grouped sum/avg of f(col)).
int64_t mojo_gpu_desc_is_transcendental(void *handle);
// GPU_OP_STATS: 1 if the descriptor has a statistical aggregate (stddev/var/
// covar/corr/regr_*). Enables routing for an otherwise KIND_UNKNOWN stat plan.
int64_t mojo_gpu_desc_is_stats(void *handle);
// A1 (GPU_OP_NULLABLE): 1 iff UNGROUPED int-path multi/aggregate (all SUM/AVG/count(*),
// not f64). Enables routing for an int multi-aggregate plan whose KIND is UNKNOWN.
int64_t mojo_gpu_desc_a1_ungrouped_ok(void *handle);
// GPU_OP_NULLABLE_GROUPED: 1 iff DENSE_GROUP int-path all-{SUM,AVG,count(*)} aggregate.
int64_t mojo_gpu_desc_a1_grouped_ok(void *handle);
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
                             int64_t n_rows, int64_t type_tag,
                             int64_t dec_scale);   // 0 ok (dec_scale: GPU_OP_STATS)
// GPU_OP_NULLABLE: feed a per-row validity byte array (1=valid, 0=NULL) for one
// column, same (req_i, col_j) addressing as feed_column. Called AFTER feed_column
// only for columns that held a NULL; absent => column is all-valid. 0 ok.
int64_t mojo_gpu_feed_validity(void *handle, int64_t req_i, int64_t col_j,
                               void *ptr, int64_t n_rows);
// SKIP-MATERIALIZE (GPU_OP_COLPOOL=2): set st.n_rows for the FACT request
// unconditionally (called for request 0 before the feed loop with res->RowCount()).
// When the narrowed SELECT omits ALL fact columns this is the only n_rows source.
int64_t mojo_gpu_feed_rowcount(void *handle, int64_t n_rows);     // 0 ok
// SKIP-MATERIALIZE active for THIS query (1) or not (0): pool on + sub-flag on +
// in-scope ungrouped predicate-independent class (Q6/Q14). When 1 the C++ side
// uses the narrowed SQL feed (NOT the GPU-direct fact feed) and calls
// feed_rowcount before the feed loop. 0 => Phase 1 / verbatim behavior.
int64_t mojo_gpu_skipmat_active(void *handle);
int64_t mojo_gpu_pin_finalize(void *handle);          // 0 ok
int64_t mojo_gpu_result_rows(void *handle);
int64_t mojo_gpu_result_i128(void *handle, int64_t row, int64_t col, int64_t *lo, int64_t *hi);
int64_t mojo_gpu_result_i64(void *handle, int64_t row, int64_t col);
double  mojo_gpu_result_f64(void *handle, int64_t row, int64_t col);
int64_t mojo_gpu_result_str(void *handle, int64_t row, int64_t col, uint8_t *out, int64_t cap);
// GPU_OP_STATS: 1 if the (row,col) cell is a real value, 0 if it is a SQL NULL.
// Defaults to 1 (valid) for every non-stat path (no validity mask emitted), so the
// int128 / transcendental result readback is byte-identical.
int64_t mojo_gpu_result_valid(void *handle, int64_t row, int64_t col);
// Phase 1 column pool (GPU_OP_COLPOOL): monotonic count of bytes pushed H2D on
// pool misses. The dedup proof reads the DELTA across queries (a shared column
// uploaded once => later queries add nothing). Surfaced to SQL by the
// gpu_colpool_status() table function for the measurement.
int64_t mojo_gpu_colpool_uploaded_bytes();
// Current RESIDENT bytes held by the pool (sum of pooled-column footprints) and
// by the tracked aggregate residency (_pin2). For the VRAM-bound assertion.
int64_t mojo_gpu_colpool_pool_bytes();
int64_t mojo_gpu_colpool_pin2_bytes();
// Phase 2 cost-aware placement observability (all diagnostic-only).
int64_t mojo_gpu_colpool_hits();
int64_t mojo_gpu_colpool_misses();
int64_t mojo_gpu_colpool_evictions();
int64_t mojo_gpu_colpool_resident_cols();
int64_t mojo_gpu_colpool_promoted_cols();
int64_t mojo_gpu_colpool_costaware();
}

namespace duckdb {

// NR1 (decline -> SIMD overrides): defined in
// packages/mojo-kernel-overrides/src/mojo_overrides.cpp, compiled + linked into
// this .so by build.sh. It mutates the built-in catalog so the Mojo SIMD kernels
// (sqrt/sin/cos/ln/exp/log10 + sum/avg/min/max, with stock fallback) replace the
// built-ins in place. Because that is catalog-level and orthogonal to our
// optimizer pass, any query this operator DECLINES then runs through the override
// kernels before stock -- turning "decline = parity" into "decline = still beat
// DuckDB" on the shapes the SIMD kernels win (min/max, transcendental, INT128 sum).
void RegisterMojoOverrides(DatabaseInstance &db);

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
static bool BelowGpuCrossover(const LogicalOperator &node);  // item 5 crossover

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
    // item 5: small-N cosine loses to the CPU array_cosine_distance override -> decline.
    if (MatchCosineProjection(proj) && !BelowGpuCrossover(proj)) {
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

// The ClientContext currently being optimized, published by GpuCosineOptimize for
// the duration of OptimizeNode so the join-uniqueness check (Part 2 of audit Group
// E) can issue a bounded nested probe (count(*) == count(DISTINCT key)) on a dim
// table whose join key is NOT covered by a declared PRIMARY KEY / UNIQUE
// constraint. nullptr outside an optimize pass -> the check then fail-closes on any
// non-constraint dim key (declines the offload). Set/cleared on the same thread the
// optimizer runs on; the nested probe runs on a FRESH Connection (its own
// ClientContext), so it does not collide with this pointer or g_saved_disabled.
thread_local ClientContext *g_gpu_op_context = nullptr;

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
    g_gpu_op_context = &input.context;
    OptimizeNode(plan);
    g_gpu_op_context = nullptr;
  } catch (...) {
    // Never let a rewrite failure break the query; fall back to stock DuckDB.
    g_gpu_op_context = nullptr;
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

// ===========================================================================
// JOIN-UNIQUENESS GATE (audit Group E, case 1: SILENT WRONG RESULT).
//
// The engine lowers every FK->dim INNER join as a dense per-key gather
// dims[off + key] -- ONE dim row per fact row. That is only correct when the
// DIM side of the join key is UNIQUE; against a non-unique build side a true
// many-to-many join collapses to 1x (the gather picks one dim row) and the
// aggregate comes out too low. So we accept a fact->dim edge ONLY when the
// dim-side join column is PROVABLY unique. Fail-closed: if we can't prove it,
// DECLINE the whole offload (-> stock DuckDB CPU, which is correct).
//
// Two signals, cheapest first:
//   (1) a declared PRIMARY KEY / UNIQUE single-column constraint on the dim
//       column (covers user tables that declare keys -- no probe needed); then
//   (2) an EXACT distinct-count probe: count(*) == count(DISTINCT <col>) over
//       the dim table, run on a fresh nested Connection. This is needed because
//       the tpch extension's dbgen creates the TPC-H tables with ONLY NOT NULL
//       constraints (no PRIMARY KEY), and DuckDB's HLL approx-distinct stat is
//       far too inaccurate at scale to use as a uniqueness oracle (e.g. sf1
//       o_orderkey: 1.5M distinct reported as ~1.49M, p_partkey 200k as ~156k).
//       The dim tables are small relative to the fact, so the probe is bounded.
// ===========================================================================

// (1) True if `col_name` is covered by a single-column PRIMARY KEY or UNIQUE
// constraint on the GET's catalog table.
bool ColHasUniqueConstraint(LogicalGet *g, const std::string &col_name) {
  if (!g) { return false; }
  auto te = g->GetTable();
  if (!te) { return false; }
  for (auto &cons : te->GetConstraints()) {
    if (cons->type != ConstraintType::UNIQUE) { continue; }
    auto &uc = cons->Cast<UniqueConstraint>();
    const auto &names = uc.GetColumnNames();
    if (names.size() == 1 && names[0] == col_name) { return true; }
  }
  return false;
}

// Cheap CURRENT exact row count for the dim table behind `g`, WITHOUT a scan:
// read it straight off the storage row-group collection (DataTable::GetTotalRows,
// which is just row_groups->GetTotalRows() -- a counter, no I/O). Used as the
// cache-validity stamp for the uniqueness probe below. Returns -1 if a cheap
// exact count is unavailable (non-duck/virtual table, no storage); callers then
// fall back to keying the cache on estimated_cardinality (weaker for DML-safety).
int64_t DimTableExactRowCount(LogicalGet *g) {
  if (!g) { return -1; }
  auto te = g->GetTable();
  if (!te) { return -1; }
  if (!te->IsDuckTable()) { return -1; }  // base GetStorage() throws otherwise
  try {
    return (int64_t)te->GetStorage().GetTotalRows();
  } catch (...) {
    return -1;
  }
}

// Session cache for the EXACT-distinct uniqueness probe. The optimizer re-runs on
// every query compilation, so without this the count(DISTINCT <col>) probe over a
// 1.5M-row dim (orders) re-executes on EVERY warm Q5 iteration (~5-6ms each),
// eroding the warm win the operator exists for. We cache the verdict ALONGSIDE the
// dim table's row count AT PROBE TIME, keyed per (database, table, column): a
// subsequent probe is a cache hit only if the row count still matches, so any DML
// that changes the row count (insert/delete) invalidates the entry and forces a
// re-probe.
//
// thread_local: the optimizer pass (and the nested probe Connection) run on the
// query's thread; this matches the existing g_gpu_op_context thread_local model
// and avoids cross-thread locking. The cache is keyed per DatabaseInstance pointer
// to avoid cross-db collisions when one process attaches several databases.
//
// RESIDUAL CAVEAT (documented, accepted): a delete+insert (or update) that keeps
// the row count IDENTICAL while changing the column's uniqueness would reuse a
// stale verdict. This gate is a heuristic safety net (it only ever DECLINES an
// offload to fall back to correct stock DuckDB), and analytic dim tables are
// effectively static, so this edge is acceptable.
struct UniqProbeEntry {
  bool result;        // probed verdict: column is exactly distinct over the table
  int64_t rowcount;   // dim-table row count when the probe ran (cache stamp)
};
thread_local std::unordered_map<std::string, UniqProbeEntry> g_uniq_probe_cache;

// US (\x1f) is not a legal SQL identifier char (plain_ident rejects it), so it is
// a collision-free separator between the db-pointer / table / column key parts.
std::string UniqProbeCacheKey(const ClientContext *ctx, const std::string &table,
                              const std::string &col) {
  char dbbuf[32];
  std::snprintf(dbbuf, sizeof(dbbuf), "%p",
                (void *)(ctx ? ctx->db.get() : nullptr));
  return std::string(dbbuf) + "\x1f" + table + "\x1f" + col;
}

// (2) Exact distinct-count probe: count(*) == count(DISTINCT <col>) on `table`.
// Quotes identifiers (defensive); returns false on any error / unexpected shape
// (fail-closed). Runs on a fresh Connection so it does not perturb the optimizer
// state of the query being compiled.
//
// `current_rowcount` is the dim table's current exact row count (from
// DimTableExactRowCount, fetched where the LogicalGet is in scope), or a weaker
// estimated_cardinality fallback if an exact count is unavailable. It is used both
// as the cache-validity stamp and to short-circuit on a cache hit (no query).
bool ColIsExactUniqueProbe(ClientContext *ctx, const std::string &table,
                           const std::string &col, int64_t current_rowcount,
                           bool dbg) {
  if (!ctx) { return false; }
  // Cache lookup: reuse the cached verdict iff the row count is unchanged.
  std::string key = UniqProbeCacheKey(ctx, table, col);
  auto it = g_uniq_probe_cache.find(key);
  if (it != g_uniq_probe_cache.end() && it->second.rowcount == current_rowcount) {
    if (dbg) { fprintf(stderr, "[gpu-uniq] %s.%s cache-hit unique=%d rows=%lld\n",
                       table.c_str(), col.c_str(), (int)it->second.result,
                       (long long)current_rowcount); }
    return it->second.result;
  }
  // Reject anything that isn't a plain identifier so the probe SQL can't be
  // anything but a simple aggregate over one table (no injection, no surprises).
  auto plain_ident = [](const std::string &s) {
    if (s.empty()) { return false; }
    for (char c : s) {
      if (!(std::isalnum((unsigned char)c) || c == '_')) { return false; }
    }
    return true;
  };
  if (!plain_ident(table) || !plain_ident(col)) { return false; }
  // The nested query re-enters our optimize hook, which sets g_gpu_op_context to
  // its own (fresh) context and clears it to nullptr on exit -- save/restore the
  // outer pass's pointer so sibling aggregate nodes in the SAME outer plan still
  // see a valid context after this probe returns.
  ClientContext *saved_ctx = g_gpu_op_context;
  try {
    Connection con(*ctx->db);
    auto res = con.Query("SELECT count(*) = count(DISTINCT " + col +
                         ") FROM " + table);
    g_gpu_op_context = saved_ctx;
    if (!res || res->HasError()) { return false; }
    auto chunk = res->Fetch();
    if (!chunk || chunk->size() == 0) { return false; }
    auto v = chunk->GetValue(0, 0);
    if (v.IsNull()) { return false; }            // empty table -> can't prove
    bool result = v.GetValue<bool>();
    // Cache the real probe verdict against the row count we probed at. (Error /
    // empty / non-ident paths above intentionally do NOT cache: they are transient
    // fail-closed returns, re-checked next time.)
    g_uniq_probe_cache[key] = UniqProbeEntry{result, current_rowcount};
    return result;
  } catch (...) {
    g_gpu_op_context = saved_ctx;
    return false;
  }
}

// Require that every DIRECT fact->dim join edge gathers from a UNIQUE build key.
// The engine lowers a fact->dim INNER join as a dense per-key gather
// dims[off + fact_fk] (one dim row per FACT row); against a non-unique dim build
// side a true many-to-many join silently collapses to 1x and the aggregate comes
// out too low (audit Group E, case 1). So we DECLINE the offload unless the
// dim-side key of every fact->dim edge is provably unique.
//
// We gate ONLY edges with the fact table on one side (the dim is the OTHER side's
// table/column). dim<->dim conditions are deliberately NOT gated here:
//   * a same-row correlated equality (Q5 c_nationkey=s_nationkey) is applied as a
//     row filter (OP_EQ), introduces no fanout; and
//   * a transitive dim<->dim edge (e.g. nation attaching via customer.c_nationkey)
//     can name a non-key column on a dim that is NOT how that dim is actually
//     gathered -- gating it would over-decline Q5 (which executes on the bespoke
//     KIND_Q5 path, not the generic dense gather). Q5's two FACT edges
//     (l_orderkey->o_orderkey, l_suppkey->s_suppkey) ARE gated and unique.
//
// fact = max estimated_cardinality GET (== descriptor.mojo). Fail-closed on any
// unresolved fact-edge dim table. Q5/Q14/Q3 keep routing: their fact->dim keys
// (o_orderkey, s_suppkey, p_partkey) pass the exact-distinct probe (dbgen tables
// carry NO PK constraint, so the constraint fast-path alone would not suffice).
bool JoinDimKeysProvablyUnique(const JoinTree &jt,
                               const std::vector<JoinEq> &eqs) {
  if (eqs.empty()) { return true; }  // single GET -> no join -> nothing to gate
  bool dbg = (std::getenv("GPU_OP_UNIQ_DEBUG") != nullptr);
  // Snapshot the optimize-pass context once: the exact-distinct probe runs a
  // nested query whose own optimize hook resets g_gpu_op_context to nullptr, so
  // reading the thread_local again on a later edge would see null.
  ClientContext *ctx = g_gpu_op_context;

  // fact = max estimated_cardinality among the collected GETs (== descriptor.mojo).
  std::string fact_table;
  int64_t fact_card = -1;
  for (auto *g : jt.gets) {
    auto te = g->GetTable();
    if (!te) { return false; }
    if ((int64_t)g->estimated_cardinality > fact_card) {
      fact_card = (int64_t)g->estimated_cardinality;
      fact_table = te->name;
    }
  }
  if (fact_table.empty()) { return false; }
  if (dbg) {
    fprintf(stderr, "[gpu-uniq] fact=%s gets:", fact_table.c_str());
    for (auto *g : jt.gets) { auto te = g->GetTable(); fprintf(stderr, " %s(%lld)",
        te ? te->name.c_str() : "?", (long long)g->estimated_cardinality); }
    fprintf(stderr, "\n[gpu-uniq] eqs:");
    for (auto &e : eqs) { fprintf(stderr, " %s.%s=%s.%s", e.lt.c_str(), e.lc.c_str(),
        e.rt.c_str(), e.rc.c_str()); }
    fprintf(stderr, "\n");
  }

  // Gate each DIRECT fact->dim edge (fact on exactly one side).
  for (auto &e : eqs) {
    std::string dim_table, dim_col;
    if (e.lt == fact_table && e.rt != fact_table) {
      dim_table = e.rt; dim_col = e.rc;
    } else if (e.rt == fact_table && e.lt != fact_table) {
      dim_table = e.lt; dim_col = e.lc;
    } else {
      continue;  // dim<->dim (or fact-self) edge: not a direct fact gather here
    }
    LogicalGet *dim_g = FindGet(jt, dim_table.c_str());
    if (!dim_g) { return false; }  // unresolved fact-edge dim -> fail-closed
    if (ColHasUniqueConstraint(dim_g, dim_col)) {
      if (dbg) { fprintf(stderr, "[gpu-uniq] %s.%s constraint-unique\n",
                         dim_table.c_str(), dim_col.c_str()); }
      continue;
    }
    // Row count stamp for the probe cache: cheap exact storage count where
    // available, else the GET's estimated_cardinality (weaker DML-safety, noted
    // on the cache). dim_g (the dim LogicalGet) is in scope here.
    int64_t dim_rows = DimTableExactRowCount(dim_g);
    if (dim_rows < 0) { dim_rows = (int64_t)dim_g->estimated_cardinality; }
    bool probed = ColIsExactUniqueProbe(ctx, dim_table, dim_col, dim_rows, dbg);
    if (dbg) { fprintf(stderr, "[gpu-uniq] %s.%s ctx=%p rows=%lld probe-unique=%d\n",
                       dim_table.c_str(), dim_col.c_str(), (void *)ctx,
                       (long long)dim_rows, (int)probed); }
    if (!probed) { return false; }  // fact-edge dim key not unique -> decline
  }
  return true;
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

  // PASS_PROGRAMS (NR3, GPU_OP_FILTER_OR). A trailing additive section: for a
  // single-table residual OR-of-equalities LogicalFilter, the postfix program
  // (reusing the aggregate `Op` type) the on-GPU expr-VM AND-composes into the
  // GET's pass program. `get_ordinal` indexes GETS in emit order. Empty (n_pass=0)
  // unless the flag is on AND the residual filter serialized -> default byte-identical.
  struct PassProg { int64_t get_ordinal; std::vector<Op> ops; };
  std::vector<PassProg> pass_programs;

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
    // PASS_PROGRAMS (NR3, trailing additive). n_pass==0 by default -> the reader
    // sees the same trailing token a flag-off build emits (no layout change).
    tape.push_back((int64_t)pass_programs.size());
    for (auto &pp : pass_programs) {
      tape.push_back(pp.get_ordinal);
      tape.push_back((int64_t)pp.ops.size());
      for (auto &op : pp.ops) {
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
  // Unsigned integer result types (GPU_OP_STATS only: DuckDB's regr_count returns
  // UINTEGER / UBIGINT). PRESERVE the exact type -- LogicalGpuAgg replaces the whole
  // aggregate node, so its declared output schema must match what the parent plan
  // expects (mapping these to BIGINT would mismatch a UINT32 parent vector).
  case LogicalTypeId::UTINYINT:  tag = rp::TYPE_UTINYINT; break;
  case LogicalTypeId::USMALLINT: tag = rp::TYPE_USMALLINT; break;
  case LogicalTypeId::UINTEGER:  tag = rp::TYPE_UINTEGER; break;
  case LogicalTypeId::UBIGINT:   tag = rp::TYPE_UBIGINT; break;
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

// GPU_OP_TRANSCENDENTAL / GPU_OP_STATS are DEFAULT-ON (opt-out), mirroring
// GPU_OP_GENERIC: unset => enabled; "off"/"0"/"none" => disabled. These f64 win
// classes are NVIDIA-only (the Mojo descriptor scope guard declines on Metal/AMD and
// on unsupported shapes) and fail-closed across the correctness envelope hardened in
// this series (NULL columns, fractional DOUBLE consts, DECIMAL scale!=2, INT128-backed
// decimals, agg-const residency collisions), so enabling them by default only adds the
// win where it is safe and declines to stock otherwise.
static bool GpuOpFlagOn(const char *name) {
  const char *v = std::getenv(name);
  if (v == nullptr) { return true; }
  return !(std::strcmp(v, "off") == 0 || std::strcmp(v, "0") == 0 ||
           std::strcmp(v, "none") == 0);
}

// NR3 (GPU_OP_FILTER_OR): route a single-table residual LogicalFilter that is an
// OR-of-equalities (and small sparse IN, which DuckDB lowers to OR-of-equalities)
// over INTEGER/DATE columns, serialized into an expression-VM PASS-PROGRAM. This
// flag is DEFAULT-OFF (presence-only, like GPU_OP_NATIVE_DECODE): unset => OFF,
// so the descent capture below is skipped and behavior is BYTE-IDENTICAL to today.
static bool GpuOpFilterOrOn() { return std::getenv("GPU_OP_FILTER_OR") != nullptr; }

// GPU_OP_NULLABLE (default-ON; opt out with off/0/none, like GPU_OP_TRANSCENDENTAL/
// GPU_OP_STATS): the A1 unified pass model routes the provably-safe nullable slice --
// UNGROUPED single-table count(*) / SUM / AVG / multi-aggregate over nullable agg/filter
// columns (int128 SUM/AVG/count(*) via per-metric validity multiply + per-agg
// validity-count NULL-on-empty; f64 transcendental/stats single-agg via validity-in-pass).
// Filter-col validity folds into the host pass column; count(*) is unmultiplied. Every
// other nullable shape fail-closes at the gate -> stock. Validated bit-exact vs stock on
// Apple + RTX 4090 across an adversarial sweep; flipped default-on after that hardening.
static bool GpuOpNullableOn() { return GpuOpFlagOn("GPU_OP_NULLABLE"); }

// GPU_OP_NULLABLE_GROUPED (default-ON; opt out with off/0/none): extend the nullable
// slice to a DENSE GROUP BY with NOT-NULL group key(s) over nullable agg/filter columns
// (int path: SUM/AVG/count(*)). The A1 per-metric validity multiply + per-group
// validity-count NULL marking + the dense filter-count existence gate (all per-group)
// handle it with no new kernel. A nullable GROUP KEY stays declined (it would form its
// own SQL NULL group). Same default-on rationale + validation as GPU_OP_NULLABLE.
static bool GpuOpNullableGroupedOn() {
  return GpuOpFlagOn("GPU_OP_NULLABLE_GROUPED");
}

// NR1 (GPU_OP_OVERRIDES): co-install the Mojo SIMD catalog overrides at LOAD so
// queries this operator declines use the Mojo kernels before stock. DEFAULT-OFF
// (presence-only, like GPU_OP_FILTER_OR): unset => the catalog is untouched and
// behavior is BYTE-IDENTICAL to today (GPU-accept paths are unaffected either way
// -- they're rewritten to GPU before the catalog scalar/agg functions run). Set
// GPU_OP_OVERRIDES=1 to enable. Flip default-on only after the both-platform
// composition gate (accepted=GPU, declined=override-kernel, all == stock).
static bool GpuOpOverridesOn() { return std::getenv("GPU_OP_OVERRIDES") != nullptr; }

// item 5 (GPU<->CPU crossover): below GPU_OP_MIN_ROWS input rows, the GPU
// pin/transfer overhead loses to the co-installed CPU SIMD kernels
// (GPU_OP_OVERRIDES) -- so DECLINE the offload and let the CPU tier (or stock)
// run it. Default 50000 input rows; GPU_OP_MIN_ROWS=0 disables the crossover
// (GPU takes every matched shape, the prior behavior).
static int64_t GpuOpMinRows() {
  const char *s = std::getenv("GPU_OP_MIN_ROWS");
  if (!s || !*s) { return 50000; }
  char *end = nullptr;
  long long v = std::strtoll(s, &end, 10);
  if (end == s || v < 0) { return 50000; }
  return (int64_t)v;
}
// Estimated rows feeding `node` (max child cardinality == fact-scan estimate for
// an aggregate/projection over a scan). The node's own estimated_cardinality is
// the OUTPUT (e.g. 1 for an ungrouped aggregate), so we look at the input.
static bool BelowGpuCrossover(const LogicalOperator &node) {
  int64_t min_rows = GpuOpMinRows();
  if (min_rows <= 0) { return false; }
  int64_t rows = 0;
  for (auto &c : node.children) { rows = MaxValue<int64_t>(rows, (int64_t)c->estimated_cardinality); }
  return rows < min_rows;
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
    // The const tape stores a scaled int64 + a divisor; a DOUBLE/FLOAT carries no
    // decimal scale, so only INTEGER-VALUED doubles round-trip exactly. Refuse a
    // fractional double (return -1 -> caller fails closed) rather than silently
    // llround-ing it: e.g. sum(sqrt(x)*1.5) would otherwise emit *2, exp(x*0.3)
    // would emit *0. (Fractional DECIMAL literals are unaffected — they take the
    // scale-aware DECIMAL case above and round-trip exactly via col/const_div.)
    double d = v.GetValue<double>();
    double rr = (double)llround(d);
    if (rr != d) { return -1; }
    int64_t raw = (int64_t)rr;
    lo = raw; hi = raw < 0 ? -1 : 0; break;
  }
  default: break;
  }
  return b.add_const(tag, scale, width, lo, hi, str_id);
}

// Emit a postfix (RPN) program for an aggregate-argument expression into `prog`.
// Reuses the IsRevenueExpr / IsPromoPredicate grammar. Returns false on any
// unfamiliar sub-expression (e.g. a transcendental function the GPU expr-VM has
// no opcode for) so the caller declines the whole offload instead of silently
// emitting a wrong (zero) program. (Previously: emitted PUSH_CONST(0), which made
// sum(sqrt(col)) compute sum(0)=0 — a silent correctness bug.)
bool EmitProgram(const Expression &e, const JoinTree &jt, RawPlanBuilder &b,
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
        return EmitProgram(*proj->expressions[idx], jt, b, prog, proj);
      }
    }
    // GPU_OP_TRANSCENDENTAL / GPU_OP_STATS: the float64 expr-VM reconstructs each
    // column's TRUE double from its SCALED-INT64 storage via col_div (= 10^scale),
    // which is exactly right for DECIMAL/INTEGER/…/HUGEINT columns. A NATIVE
    // FLOAT/DOUBLE column is stored as raw IEEE bits (not a scaled int), so
    // `Float64(raw_int64_bits) / col_div` would read garbage. The VM has no
    // bit-reinterpret path, so fail-closed on native-float source columns ->
    // the whole plan declines to stock CPU (correct, just not accelerated).
    if (GpuOpFlagOn("GPU_OP_TRANSCENDENTAL") ||
        GpuOpFlagOn("GPU_OP_STATS")) {
      auto pt = ref.return_type.InternalType();
      if (pt == PhysicalType::FLOAT || pt == PhysicalType::DOUBLE) {
        return false;
      }
    }
    // WIDTH GATE (FIX 1, Group A): the aggregate-INPUT materialize/feed path reads
    // each fed column at a fixed byte stride keyed off its physical type, and the
    // plain int aggregate kernels only consume INT32 (4B) or INT64 (8B) columns.
    // A column whose internal type is NOT one of those is read at the wrong stride
    // -> garbage, OR throws "unsupported materialized column physical type" in the
    // feed switch (routed-then-error). Fail-closed here so the whole offload
    // declines to correct stock CPU. (FLOAT/DOUBLE are already declined just above
    // under the f64 flags; the f64 CAST path independently declines INT128 below.)
    //   * DECIMAL: require INT64 backing (precision 10..18). This declines
    //     DECIMAL(1..9) [INT16/INT32-backed] AND DECIMAL(19..38) [INT128-backed].
    //     TPC-H DECIMAL(15,2) is INT64-backed -> STILL routes.
    //   * else (integer/other numeric): require INT32 or INT64. This declines
    //     TINYINT (INT8), SMALLINT (INT16), HUGEINT (INT128). INTEGER (INT32) and
    //     BIGINT (INT64) -> STILL route.
    {
      auto wpt = ref.return_type.InternalType();
      if (ref.return_type.id() == LogicalTypeId::DECIMAL) {
        if (wpt != PhysicalType::INT64) { return false; }
      } else {
        if (wpt != PhysicalType::INT32 && wpt != PhysicalType::INT64) {
          return false;
        }
      }
    }
    auto col = ResolveJoinColref(ref, jt.gets);
    int64_t t = b.intern(col.table_name);
    int64_t c = b.intern(col.col_name);
    prog.push_back({rp::OP_LOAD_COL, t, c});
    return true;
  }
  if (cls == ExpressionClass::BOUND_CONSTANT) {
    int64_t cid = AddValueConst(b, e.Cast<BoundConstantExpression>().value);
    if (cid < 0) { return false; }  // inexact (fractional DOUBLE/FLOAT) -> fail-closed
    prog.push_back({rp::OP_PUSH_CONST, cid, 0});
    return true;
  }
  if (cls == ExpressionClass::BOUND_FUNCTION) {
    auto &fn = e.Cast<BoundFunctionExpression>();
    const std::string &nm = fn.function.name;
    int64_t binop = 0;
    if (nm == "*" || nm == "multiply") { binop = rp::OP_MUL; }
    else if (nm == "-" || nm == "subtract") { binop = rp::OP_SUB; }
    else if (nm == "+" || nm == "add") { binop = rp::OP_ADD; }
    if (binop && fn.children.size() == 2) {
      if (!EmitProgram(*fn.children[0], jt, b, prog, proj)) { return false; }
      if (!EmitProgram(*fn.children[1], jt, b, prog, proj)) { return false; }
      prog.push_back({binop, 0, 0});
      return true;
    }
    // Power with an INTEGER-VALUED CONSTANT exponent (flag GPU_OP_TRANSCENDENTAL).
    // power(x, K) -> <base program>, PUSH_CONST(K), OP_POW; the float64 VM computes
    // it by exact binary exponentiation (pure multiplies — bit-faithful for any base
    // sign). ONLY integer-valued constant exponents are emitted: the f64 const tape
    // rounds doubles to int (AddValueConst/llround), so a fractional exponent would be
    // silently wrong -> fail-closed (decline) on fractional or non-constant (column)
    // exponents. Returns DOUBLE -> routed to the f64 accumulator like the unary ops.
    if (GpuOpFlagOn("GPU_OP_TRANSCENDENTAL") &&
        (nm == "pow" || nm == "power") && fn.children.size() == 2) {
      const Expression *ec = fn.children[1].get();
      while (ec->GetExpressionClass() == ExpressionClass::BOUND_CAST) {
        ec = ec->Cast<BoundCastExpression>().child.get();
      }
      if (ec->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
        const Value &ev = ec->Cast<BoundConstantExpression>().value;
        if (!ev.IsNull() && ev.type().IsNumeric()) {
          double d = ev.GetValue<double>();
          double r = (double)std::llround(d);
          if (r == d && std::fabs(r) <= 64.0) {
            if (!EmitProgram(*fn.children[0], jt, b, prog, proj)) { return false; }
            int64_t cid = AddValueConst(b, ev);
            if (cid < 0) { return false; }  // (integer-valued -> never; defensive)
            prog.push_back({rp::OP_PUSH_CONST, cid, 0});
            prog.push_back({rp::OP_POW, 0, 0});
            return true;
          }
        }
      }
      // fractional / non-constant exponent -> fall through -> fail-closed (decline)
    }
    // Transcendental unary functions (flag GPU_OP_TRANSCENDENTAL only). Maps a
    // 1-child sqrt/exp/ln(or log)/log10/sin/cos to the matching OP_* opcode, which
    // ONLY the float64 expr-VM handles (sum/avg of f(col) -> DOUBLE). When the flag
    // is off these stay UNRECOGNIZED -> fail-closed (decline, prior behavior).
    // The scope guard (UNGROUPED-only + DOUBLE accumulator) is enforced on the Mojo
    // side (build_descriptor_impl + finalize); anything else declines -> CPU.
    if (GpuOpFlagOn("GPU_OP_TRANSCENDENTAL") && fn.children.size() == 1) {
      int64_t uop = 0;
      // DuckDB semantics: ln(x)=natural log; log(x)==log10(x)=base-10; log10(x)
      // base-10. (Verified: SELECT ln(10),log(10),log10(10) -> 2.302,1.0,1.0.)
      if (nm == "sqrt") { uop = rp::OP_SQRT; }
      else if (nm == "exp") { uop = rp::OP_EXP; }
      else if (nm == "ln") { uop = rp::OP_LN; }
      else if (nm == "log10" || nm == "log") { uop = rp::OP_LOG10; }
      else if (nm == "log2") { uop = rp::OP_LOG2; }
      // FIX 4 (Group F): sin/cos are computed in FLOAT32 on NVIDIA (the f64 expr-VM
      // casts to f32 -- NVIDIA has no precise f64 sin/cos PTX), giving rel-err
      // ~5e-7, well outside the aggregate's exactness tolerance -> SILENT WRONG
      // RESULTS for sum(sin(x))/sum(cos(x)). The other transcendentals (sqrt/exp/
      // ln/log10/log2/pow) ARE f64-precise and keep routing. Do NOT map sin/cos to
      // an opcode -> they stay UNRECOGNIZED -> EmitProgram fails closed -> the whole
      // plan declines to stock CPU. (On Apple sin/cos already decline via the
      // dispatcher, so this is a no-op there; the win we keep is NVIDIA-only.)
      // else if (nm == "sin") { uop = rp::OP_SIN; }  // declined: f32-only, imprecise
      // else if (nm == "cos") { uop = rp::OP_COS; }  // declined: f32-only, imprecise
      if (uop) {
        if (!EmitProgram(*fn.children[0], jt, b, prog, proj)) { return false; }
        prog.push_back({uop, 0, 0});
        return true;
      }
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
        if (!EmitProgram(*chk.when_expr, jt, b, prog, proj)) { return false; }
      }
      if (!EmitProgram(*chk.then_expr, jt, b, prog, proj)) { return false; }
      int64_t zero = b.add_const(rp::TYPE_BIGINT, 0, 0, 0, 0, -1);
      prog.push_back({rp::OP_PUSH_CONST, zero, 0});
      prog.push_back({rp::OP_SELECT, 0, 0});
      return true;
    }
  }
  // Transparent numeric CAST pass-through (flag GPU_OP_TRANSCENDENTAL only).
  // DuckDB wraps the transcendental's argument in a CAST (e.g. sqrt(CAST(
  // l_extendedprice AS DOUBLE))). The float64 expr-VM reconstructs the TRUE double
  // of every column from its scaled-int64 storage via col_div (= 10^scale), which
  // is EXACTLY a DECIMAL/INTEGER -> DOUBLE/FLOAT cast. So emitting the child's
  // program unchanged is value-correct; the cast becomes a no-op at the VM level.
  // Restricted to numeric source+target (the only thing the VM models) and to the
  // flag, so the int128 path is untouched. Anything else still fails closed.
  // GPU_OP_STATS uses the SAME f64 expr-VM (col_div reconstructs the true double
  // from scaled-int64 storage), so the DECIMAL->DOUBLE cast DuckDB wraps a stat
  // argument in is likewise a value-level no-op -> pass the child through.
  if ((GpuOpFlagOn("GPU_OP_TRANSCENDENTAL") ||
       GpuOpFlagOn("GPU_OP_STATS")) &&
      cls == ExpressionClass::BOUND_CAST) {
    auto &ce = e.Cast<BoundCastExpression>();
    auto tid = e.return_type.id();
    auto sid = ce.child->return_type.id();
    auto is_num = [](LogicalTypeId id) {
      return id == LogicalTypeId::DOUBLE || id == LogicalTypeId::FLOAT ||
             id == LogicalTypeId::DECIMAL || id == LogicalTypeId::BIGINT ||
             id == LogicalTypeId::INTEGER || id == LogicalTypeId::SMALLINT ||
             id == LogicalTypeId::TINYINT || id == LogicalTypeId::HUGEINT;
    };
    if (is_num(tid) && is_num(sid)) {
      // The float64 VM reconstructs each operand from a SCALED INT64; an INT128-backed
      // source (DECIMAL precision>18, or HUGEINT) does not fit int64 -> reading it as
      // int64 truncates/misreads it (e.g. sum(sqrt(DECIMAL(38,4))) came out ~2x wrong).
      // Fail-closed -> decline to stock. (The int128 aggregate paths read hugeint
      // correctly; only this f64 expr-VM is int64-limited. A 16-byte-aware f64 feed
      // that keeps the win on wide decimals is a follow-up.)
      if (ce.child->return_type.InternalType() == PhysicalType::INT128) {
        return false;
      }
      return EmitProgram(*ce.child, jt, b, prog, proj);
    }
  }

  // Unfamiliar (e.g. sqrt/exp/ln — no GPU expr-VM opcode): fail-closed so the
  // caller declines the offload to stock/overrides rather than emitting sum(0).
  return false;
}

// Map a DuckDB aggregate function name to an AggKind tag (0 if unsupported).
int64_t MapAggKind(const std::string &name) {
  if (name == "sum" || name == "sum_no_overflow") { return rp::AGG_SUM; }
  if (name == "avg") { return rp::AGG_AVG; }
  if (name == "count_star") { return rp::AGG_COUNT_STAR; }
  if (name == "min") { return rp::AGG_MIN; }
  if (name == "max") { return rp::AGG_MAX; }
  // Statistical aggregates (flag GPU_OP_STATS only). All DOUBLE-result, derived
  // closed-form on the host from the shared sums the f64 seg kernels accumulate.
  // When the flag is off these return 0 -> the agg-emit loop fails closed (the
  // whole plan declines to stock CPU), so the default build is byte-identical.
  if (GpuOpFlagOn("GPU_OP_STATS")) {
    if (name == "stddev_samp" || name == "stddev") { return rp::AGG_STDDEV_SAMP; }
    if (name == "stddev_pop") { return rp::AGG_STDDEV_POP; }
    if (name == "var_samp" || name == "variance") { return rp::AGG_VAR_SAMP; }
    if (name == "var_pop") { return rp::AGG_VAR_POP; }
    if (name == "covar_samp") { return rp::AGG_COVAR_SAMP; }
    if (name == "covar_pop") { return rp::AGG_COVAR_POP; }
    if (name == "corr") { return rp::AGG_CORR; }
    if (name == "regr_slope") { return rp::AGG_REGR_SLOPE; }
    if (name == "regr_intercept") { return rp::AGG_REGR_INTERCEPT; }
    if (name == "regr_r2") { return rp::AGG_REGR_R2; }
    if (name == "regr_avgx") { return rp::AGG_REGR_AVGX; }
    if (name == "regr_avgy") { return rp::AGG_REGR_AVGY; }
    if (name == "regr_sxx") { return rp::AGG_REGR_SXX; }
    if (name == "regr_syy") { return rp::AGG_REGR_SYY; }
    if (name == "regr_sxy") { return rp::AGG_REGR_SXY; }
    if (name == "regr_count") { return rp::AGG_REGR_COUNT; }
  }
  return 0;
}

// True if an AggKind tag is a statistical aggregate (GPU_OP_STATS).
bool IsStatAggKind(int64_t k) {
  return k >= rp::AGG_STDDEV_SAMP && k <= rp::AGG_REGR_COUNT;
}

// True if a stat aggregate is 2-arg (covar/corr/regr_*); false for the 1-arg
// stddev/var family. Used to decide whether to emit the OP_ARGSEP + second-arg
// program in the agg-emit loop.
bool IsStat2Arg(int64_t k) {
  switch (k) {
  case rp::AGG_STDDEV_SAMP:
  case rp::AGG_STDDEV_POP:
  case rp::AGG_VAR_SAMP:
  case rp::AGG_VAR_POP:
    return false;
  default:
    return true; // covar/corr/regr_*
  }
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

// NR3 (GPU_OP_FILTER_OR): serialize a residual single-table filter expression
// into a postfix PASS-PROGRAM (the same `Op` tape the aggregates use). FAIL-CLOSED
// (return false -> caller declines the whole offload) on ANYTHING outside the safe
// envelope; never drop a predicate. Handles ONLY:
//   - CONJUNCTION_OR  -> children chained with OP_ADD (!=0 iff any branch passes;
//                        every leaf is 0/1 so the sum is >=0 and !=0 iff some leaf is 1).
//   - CONJUNCTION_AND -> children chained with OP_MUL (!=0 iff all branches pass).
//   - COMPARE_{EQUAL,NOTEQUAL,LESSTHAN,LESSTHANOREQUALTO,GREATERTHAN,
//     GREATERTHANOREQUALTO} -> col<op>const over THIS GET on an INTEGER/DATE column:
//                        LOAD_COL slot; PUSH_CONST const_id; OP_{EQ,NE,LT,LE,GT,GE}
//                        (each pushes 0/1). This lets an OR-of-RANGE / inequality
//                        residual filter (a<10 OR a>95, BETWEEN as AND-of-ranges,
//                        a!=5, ...) lower exactly like OR-of-equalities (GPU_OP_FILTER_OR).
// `gets` is the single-element {this GET}; resolution reuses ResolveJoinColref +
// AddValueConst exactly as the aggregate EmitProgram does. `op_count` is bumped per
// emitted op and the whole thing declines if it exceeds 64 (stack stays <=2..3 by
// construction; the cap bounds the program size). VARCHAR/DECIMAL/BIGINT/BOOLEAN
// colrefs and any non-{EQ,NE,LT,LE,GT,GE} comparison / non-constant side fail-closed
// (so a VARCHAR const never reaches AddValueConst -- which would silently intern a
// str_id and compare vs 0 down the int eval path). When the COLUMN is on the RIGHT
// of an inequality (`5 > a`), the comparator is INVERTED (LT<->GT, LE<->GE; EQ/NE
// symmetric) so the emitted LOAD_COL;PUSH_CONST;<op> keeps lhs=column, rhs=const.
bool EmitOrEq(const Expression &e, const std::vector<LogicalGet *> &gets,
              RawPlanBuilder &b, std::vector<RawPlanBuilder::Op> &prog,
              int64_t &op_count) {
  if (op_count > 64) { return false; }
  auto cls = e.GetExpressionClass();
  if (cls == ExpressionClass::BOUND_CONJUNCTION) {
    auto &conj = e.Cast<BoundConjunctionExpression>();
    if (conj.children.size() < 2) { return false; }
    int64_t combine;
    if (e.type == ExpressionType::CONJUNCTION_OR) { combine = rp::OP_ADD; }
    else if (e.type == ExpressionType::CONJUNCTION_AND) { combine = rp::OP_MUL; }
    else { return false; }
    if (!EmitOrEq(*conj.children[0], gets, b, prog, op_count)) { return false; }
    for (idx_t i = 1; i < conj.children.size(); i++) {
      if (!EmitOrEq(*conj.children[i], gets, b, prog, op_count)) { return false; }
      prog.push_back({combine, 0, 0});
      op_count++;
      if (op_count > 64) { return false; }
    }
    return true;
  }
  if (cls == ExpressionClass::BOUND_COMPARISON) {
    // Map the DuckDB comparison type -> the emittable int expr-VM opcode. Only the
    // six 0/1 range/equality comparisons are supported; anything else (DISTINCT_FROM,
    // NOT_DISTINCT_FROM, IN-as-comparison, ...) fails-closed below. The mapping is
    // written for column-on-LEFT (lhs=col, rhs=const); when the column is on the
    // RIGHT we INVERT it (5 > a == a < 5), so LT<->GT and LE<->GE swap, EQ/NE stay.
    auto &cmp = e.Cast<BoundComparisonExpression>();
    // One side a colref on THIS get, the other a bound constant (either order).
    const Expression *col_side = nullptr;
    const Expression *const_side = nullptr;
    bool col_on_right = false;
    if (cmp.left->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
        cmp.right->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
      col_side = cmp.left.get(); const_side = cmp.right.get();
    } else if (cmp.right->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF &&
               cmp.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
      col_side = cmp.right.get(); const_side = cmp.left.get();
      col_on_right = true;
    } else {
      return false;  // colref-vs-colref / function side / etc. -> decline
    }
    // Resolve the opcode for the canonical (col <op> const) orientation, inverting
    // the comparator when the column was on the RIGHT so lhs=column, rhs=const holds.
    int64_t cmp_op = 0;
    switch (e.type) {
    case ExpressionType::COMPARE_EQUAL:    cmp_op = rp::OP_EQ; break;
    case ExpressionType::COMPARE_NOTEQUAL: cmp_op = rp::OP_NE; break;
    case ExpressionType::COMPARE_LESSTHAN:
      cmp_op = col_on_right ? rp::OP_GT : rp::OP_LT; break;
    case ExpressionType::COMPARE_LESSTHANOREQUALTO:
      cmp_op = col_on_right ? rp::OP_GE : rp::OP_LE; break;
    case ExpressionType::COMPARE_GREATERTHAN:
      cmp_op = col_on_right ? rp::OP_LT : rp::OP_GT; break;
    case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
      cmp_op = col_on_right ? rp::OP_LE : rp::OP_GE; break;
    default:
      return false;  // unsupported comparison type -> decline
    }
    auto &ref = col_side->Cast<BoundColumnRefExpression>();
    // TYPE GATE: only INTEGER / DATE go down the int eval path. EXCLUDE everything
    // else (VARCHAR/DECIMAL/BIGINT/BOOLEAN/...) -> fail-closed BEFORE AddValueConst.
    auto tid = ref.return_type.id();
    if (tid != LogicalTypeId::INTEGER && tid != LogicalTypeId::DATE) { return false; }
    auto col = ResolveJoinColref(ref, gets);
    if (col.col_name.empty()) { return false; }  // not on this GET
    int64_t t = b.intern(col.table_name);
    int64_t c = b.intern(col.col_name);
    // The const must match the column's INTEGER/DATE family (same id) so the int
    // limb is exact; AddValueConst returns -1 only for fractional doubles (n/a here).
    auto &cval = const_side->Cast<BoundConstantExpression>().value;
    if (cval.type().id() != tid) { return false; }
    int64_t cid = AddValueConst(b, cval);
    if (cid < 0) { return false; }
    prog.push_back({rp::OP_LOAD_COL, t, c});
    prog.push_back({rp::OP_PUSH_CONST, cid, 0});
    prog.push_back({cmp_op, 0, 0});
    op_count += 3;
    return true;
  }
  return false;  // any other expression class -> decline
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

  // NR3 (GPU_OP_FILTER_OR): capture a single-table residual LogicalFilter whose
  // single child is a LOGICAL_GET and whose .expressions are non-empty (an OR-of-
  // equalities / sparse-IN that DuckDB leaves above the scan). When captured, we
  // descend past it (`below = filter.children[0]`) so the dispatch below proceeds
  // as a single GET, and serialize the filter's predicate into a PASS_PROGRAM after
  // the GETS section. When the flag is OFF (or the shape differs) we do NOTHING ->
  // `below` stays the LOGICAL_FILTER -> it falls into the `else { return false; }`
  // dispatch below -> BYTE-IDENTICAL decline. (A join-tree residual filter stays
  // declined by CollectJoinTree's non-empty-expressions guard; this path is
  // single-GET only.)
  const LogicalFilter *residual_filter = nullptr;
  if (GpuOpFilterOrOn() && below->type == LogicalOperatorType::LOGICAL_FILTER) {
    auto &flt = below->Cast<LogicalFilter>();
    if (flt.children.size() == 1 &&
        flt.children[0]->type == LogicalOperatorType::LOGICAL_GET &&
        !flt.expressions.empty()) {
      residual_filter = &flt;
      below = flt.children[0].get();
    }
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

  // NULL-SAFETY GATE. The materialize/feed path copies only the column DATA array
  // (FedColumn::fill = a raw memcpy) and -- unless GPU_OP_NULLABLE captures it --
  // does NOT carry DuckDB's validity mask, so a NULL row is read as raw garbage
  // int64 -> SILENT WRONG RESULTS (sum off, avg=0, filtered-sum=garbage). Returns
  // true iff ANY projected column of ANY GET is nullable (lacks a NOT NULL
  // constraint). Provably complete: GetColumnIds() is a SUPERSET of the columns the
  // GPU reads (aggregate args, group keys, filter columns, join keys, gathered dim
  // columns). TPC-H tables are created all-NOT-NULL (dbgen) so the accepted classes
  // never trip this.
  auto any_nullable_projected = [&]() -> bool {
    for (auto *g : jt.gets) {
      auto te = g->GetTable();
      if (!te) { return true; }  // non-table GET: can't verify -> treat as nullable
      for (auto &ci : g->GetColumnIds()) {
        if (ci.IsRowIdColumn() || ci.IsVirtualColumn()) { continue; }
        idx_t cidx = ci.GetPrimaryIndex();
        bool is_not_null = false;
        for (auto &cons : te->GetConstraints()) {
          if (cons->type == ConstraintType::NOT_NULL &&
              cons->Cast<NotNullConstraint>().index.index == cidx) {
            is_not_null = true;
            break;
          }
        }
        if (!is_not_null) { return true; }
      }
    }
    return false;
  };
  // Default (GPU_OP_NULLABLE off): decline ANY nullable column here, byte-identical
  // to before. When ON: defer the decision to the role-aware SAFE-SLICE gate at the
  // END of this function (after aggregates/filters/groups are serialized, so every
  // column ROLE is known). A nullable query that is NOT the safe slice is declined
  // there exactly as today; the safe slice routes with validity-folding (see
  // _pin_finalize_generic). Cache the flag for the late gate.
  const bool nullable_on = GpuOpNullableOn();
  if (!nullable_on && any_nullable_projected()) { return false; }

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
      // TYPE GATE (FIX 2): a pushed-down zone-map filter column is MATERIALIZED and
      // fed like any other column, so it must be one of the physical types the feed
      // switch handles (INT32/INT64/INT128/DOUBLE, or logical VARCHAR). Without this
      // gate, `WHERE bool_col = true` routes then THROWS "unsupported materialized
      // column physical type for BOOLEAN" in the feed loop (routed-then-error). The
      // feed switch's `default:` covers BOOLEAN (PhysicalType::BOOL), TINYINT
      // (INT8) and SMALLINT (INT16) -- decline them here. DATE is PhysicalType::
      // INT32 and integer/date range filters (TPC-H) still route.
      if (col_idx < g->returned_types.size()) {
        const LogicalType &ft = g->returned_types[col_idx];
        bool feedable = (ft.id() == LogicalTypeId::VARCHAR);
        if (!feedable) {
          switch (ft.InternalType()) {
          case PhysicalType::INT32:
          case PhysicalType::INT64:
          case PhysicalType::INT128:
          case PhysicalType::DOUBLE:
            feedable = true;
            break;
          default:
            feedable = false;
          }
        }
        if (!feedable) { return false; }  // e.g. BOOLEAN/TINYINT/SMALLINT -> decline
      } else {
        return false;  // can't verify the filter column type -> fail-closed
      }
      int64_t cmp = MapCmp(cf.comparison_type);
      if (cmp == 0) { return false; }
      int64_t cid = AddValueConst(out, cf.constant);
      if (cid < 0) { return false; }  // inexact (fractional DOUBLE/FLOAT) filter const -> decline
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
      } else if (tf.filter_type == TableFilterType::OPTIONAL_FILTER && residual_filter) {
        // NR3 (GPU_OP_FILTER_OR): an OptionalFilter is a zone-map pruning hint only,
        // never authoritative for correctness. DuckDB pushes one as the partial copy
        // of the residual IN/OR predicate that also remains as the LogicalFilter we
        // captured above this GET. Since we serialize that residual into the
        // PASS_PROGRAM below, the optional copy is subsumed -> safe to skip. Gated on
        // residual_filter so the flag-OFF path keeps declining on OPTIONAL_FILTER
        // exactly as before (byte-identical default behavior).
        continue;
      } else {
        return false;  // unmodeled filter shape
      }
    }
    out.gets.push_back(std::move(ge));
  }

  // 3b. PASS_PROGRAMS (NR3, GPU_OP_FILTER_OR). If a single-table residual filter
  // was captured, serialize EACH of its (implicitly ANDed) expressions into ONE
  // postfix program, chaining the per-expression results with OP_MUL (AND). It
  // attaches to GET ordinal 0 (the single GET this path supports). If ANY
  // expression fails to serialize -> DECLINE the whole offload (never drop a
  // predicate -> never a wrong answer). residual_filter is non-null only when the
  // flag is on, so this whole block is dead by default.
  if (residual_filter) {
    if (jt.gets.size() != 1 || !single_get) { return false; }  // single-GET only
    std::vector<RawPlanBuilder::Op> ops;
    int64_t op_count = 0;
    for (idx_t i = 0; i < residual_filter->expressions.size(); i++) {
      if (!EmitOrEq(*residual_filter->expressions[i], jt.gets, out, ops,
                    op_count)) {
        return false;
      }
      if (i > 0) {
        ops.push_back({rp::OP_MUL, 0, 0});  // AND across the filter's expressions
        op_count++;
        if (op_count > 64) { return false; }
      }
    }
    if (ops.empty()) { return false; }  // captured non-empty filter must emit ops
    out.pass_programs.push_back({(int64_t)0, std::move(ops)});
  }

  // 4. JOINS (one INNER entry with the resolved conds; none if single GET).
  if (!single_get) {
    // JOIN-UNIQUENESS GATE (audit Group E, case 1). The FK->dim gather lowering
    // is only correct when each edge's DIM-side join key is unique; against a
    // non-unique build side a many-to-many join silently collapses to 1x. Decline
    // the whole offload unless every dim key is provably unique. This runs BEFORE
    // serializing the join (so a non-unique join never even forms a descriptor
    // that could misclassify as Q3/Q5/Q14). Q5/Q14/Q3's FK->PK keys are recognized
    // by the exact distinct probe (their dbgen tables carry no PK constraint).
    if (!JoinDimKeysProvablyUnique(jt, eqs)) { return false; }
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
  // GPU_OP_NULLABLE_GROUPED: capture the resolved group-key (table,col) so the nullable
  // gate can require every group key to be NOT NULL (a NULL group key would form its
  // own SQL NULL group, which the dense gid build does not model).
  std::vector<std::pair<std::string, std::string>> group_key_cols;
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
    group_key_cols.push_back({table, col});
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
    // Program: empty for COUNT_STAR; a statistical aggregate emits its argument
    // program(s) into Agg.program -- 1-arg (stddev/var) is just the x program;
    // 2-arg (covar/corr/regr_*) emits the dependent-y program, OP_ARGSEP, then the
    // independent-x program. (DuckDB's regr_*(y, x) puts dependent first.) The Mojo
    // metric lowering splits on OP_ARGSEP and builds the shared-sum metrics.
    if (IsStatAggKind(kind)) {
      if (IsStat2Arg(kind)) {
        if (ag.children.size() != 2) { return false; }
        if (!EmitProgram(*ag.children[0], jt, out, ae.program, proj)) { return false; }
        ae.program.push_back({rp::OP_ARGSEP, 0, 0});
        if (!EmitProgram(*ag.children[1], jt, out, ae.program, proj)) { return false; }
      } else {
        if (ag.children.size() != 1) { return false; }
        if (!EmitProgram(*ag.children[0], jt, out, ae.program, proj)) { return false; }
      }
    } else if (kind != rp::AGG_COUNT_STAR && ag.children.size() == 1) {
      if (!EmitProgram(*ag.children[0], jt, out, ae.program, proj)) { return false; }
    }
    out.aggregates.push_back(std::move(ae));
  }

  // GPU_OP_NULLABLE SAFE-SLICE GATE (role-aware; runs only when the flag is on).
  // The early NULL-safety gate was deferred to here so every column ROLE is known.
  // ACCEPT a nullable query ONLY for the provably-safe slice where folding each
  // row's validity into the single shared host pass column is BIT-IDENTICAL to SQL
  // aggregate NULL semantics (a NULL agg-input / filter row is excluded exactly like
  // a filtered-out row; ungrouped_count_m is summed over the SAME pass-gated rows so
  // sum/avg stay correct). Conditions (each derived from the adversarial NULL-
  // semantics analysis -- see PERF_BACKLOG):
  //   * single_get (no join: no nullable FK / multi-table fan-out)
  //   * UNGROUPED (agg.groups.empty(): no nullable GROUP KEY, which would form its
  //     own NULL group rather than be excluded)
  //   * GPU_OP_NATIVE_DECODE unset: GUARANTEES the host-pass-bake path. All three
  //     in-kernel-predicate paths (q6_pred / gen_pred / f64_pred) that SKIP the host
  //     bake require native-decode; without it the host bake (where validity is
  //     folded) always runs.
  //   * exactly ONE aggregate, kind SUM with an INT128 result (ret_is_int128):
  //     - SUM excludes count(*) (counts ALL rows incl. NULL-input -> a shared-pass
  //       fold would undercount), MIN/MAX (declined anyway), and stat kinds.
  //     - the INT128-result requirement is load-bearing in TWO ways: (a) it keeps
  //       this to the int64/int128 assembly path that writes res_lo/res_hi (the f64
  //       transcendental SUM -- sum(sqrt(x)) etc. -- returns DOUBLE, ret_is_int128=0,
  //       so it declines: that path is NVIDIA-only and unvalidated for nullable);
  //       (b) it EXCLUDES AVG: DuckDB rewrites a nullable avg(x) into a `sum`
  //       aggregate (kind_tag == AGG_SUM!) with a DOUBLE output column + a division
  //       projection above -- the int128 assembly would write res_lo while the
  //       DOUBLE extraction reads res_f64 (=> 0.0, a wrong result). Requiring an
  //       INT128 result fences that out (avg's output is DOUBLE) -> avg declines to
  //       stock. (A single nullable-input SUM is also, by construction, a single
  //       distinct agg-input column, so shape-5's per-aggregate-exclusion hazard
  //       cannot arise.)
  //   * OR the f64 path: a single transcendental SUM (sum(sqrt(x))..) or a
  //     statistical aggregate (stddev/var/covar/corr/regr_*). These are NVIDIA-only
  //     (the Mojo builder's f64 scope guard declines non-NVIDIA -> stock, so Apple is
  //     unaffected), is_float64 -> the host-bake pass column gates the f64 metrics
  //     IDENTICALLY to the int path, so the same validity fold excludes NULL rows. A
  //     multi-arg stat (corr(x,y)) is NULL-excluded when EITHER arg is NULL -- exactly
  //     AND-ing both columns' validity into the one pass bit. AVG stays excluded (same
  //     avg-as-sum-double hazard); a transcendental SUM has a transcendental opcode in
  //     its program, which is how we tell it from a plain (rewritten-avg) SUM.
  // IS NULL / IS NOT NULL filters (which would INVERT the fold) already decline in
  // the filter walk above (unmodeled filter shape -> return false), so no extra check
  // is needed. Any query failing the slice re-applies the original blanket decline.
  if (nullable_on) {
    // A1 unified pass model: accept N aggregates (not just one). The host pass column
    // folds in only FILTER-column validity; each metric multiplies in its own
    // agg-input-column validity, and count(*) is unmultiplied -- so count(*) (counts
    // ALL filter-passing rows) and multiple aggregates over DIFFERENT nullable columns
    // (each excludes only its own NULLs) are correct. Per-aggregate NULL-on-empty uses
    // each aggregate's validity-product count. Accept iff single-table, UNGROUPED, no
    // native-decode, and EVERY aggregate is an int128 SUM / AVG / count(*) / stat /
    // transcendental SUM (the kinds the Mojo finalize lowers under A1).
    bool base = single_get && agg.groups.empty() &&
                std::getenv("GPU_OP_NATIVE_DECODE") == nullptr &&
                !out.aggregates.empty();
    bool all_ok = base;
    bool has_f64 = false;  // any stat / transcendental-SUM aggregate
    if (base) {
      for (auto &a : out.aggregates) {
        bool ok = false;
        if (a.kind_tag == rp::AGG_COUNT_STAR) {
          ok = true;  // counts all filter-passing rows; never validity-multiplied
        } else if (a.kind_tag == rp::AGG_SUM && a.ret_is_int128 == 1) {
          ok = true;  // int128 SUM (avg-as-sum-double is DOUBLE -> ret_is_int128==0)
        } else if (a.kind_tag == rp::AGG_AVG) {
          ok = true;  // numerator * validity, denominator = validity-product count
        } else if (IsStatAggKind(a.kind_tag)) {
          ok = true;  // stddev/var/covar/corr/regr_* (NVIDIA-only; Apple declines f64)
          has_f64 = true;
        } else {
          for (auto &op : a.program) {  // transcendental SUM: sum(sqrt|exp|ln|..(x))
            if (a.kind_tag == rp::AGG_SUM && op.op_tag >= rp::OP_SQRT &&
                op.op_tag <= rp::OP_LOG2) {
              ok = true;
              has_f64 = true;
              break;
            }
          }
        }
        if (!ok) { all_ok = false; break; }
      }
    }
    // The A1 per-metric validity multiply is INT-only. An f64 (transcendental/stats)
    // aggregate uses the validity-IN-PASS fold instead -- a NULL row is excluded from
    // the kernel entirely, because a metric tape would otherwise EVALUATE the
    // transcendental on a NULL row's garbage (e.g. sqrt(negative) -> domain error)
    // before a multiply could zero it. Pass-folding excludes whole rows, so it cannot
    // give per-column NULL exclusion across MULTIPLE f64 aggregates -> restrict f64
    // nullable to a SINGLE aggregate (the shipped ce3e7db scope). Int multi-aggregate
    // (count(*) + sum/avg over different nullable columns) is unaffected.
    if (has_f64 && out.aggregates.size() != 1) { all_ok = false; }

    // GPU_OP_NULLABLE_GROUPED: extend to a DENSE GROUP BY with NOT-NULL group key(s)
    // over nullable agg/filter columns (int path). The A1 per-metric validity multiply
    // + per-group validity-count NULL marking + the dense filter-count existence gate
    // are all per-group (g*M-indexed) -> no new kernel. Require: single-table, grouped,
    // no native-decode, every aggregate int {SUM int128, AVG, count(*)} (the routing
    // accessor a1_grouped_ok additionally requires DENSE), and EVERY group key NOT NULL
    // (a nullable group key would form its own SQL NULL group, unmodeled by the dense
    // gid build -> stays declined).
    if (!all_ok && GpuOpNullableGroupedOn() && single_get &&
        !agg.groups.empty() &&
        std::getenv("GPU_OP_NATIVE_DECODE") == nullptr &&
        !out.aggregates.empty()) {
      bool g_ok = true;
      for (auto &a : out.aggregates) {
        if (!(a.kind_tag == rp::AGG_COUNT_STAR ||
              (a.kind_tag == rp::AGG_SUM && a.ret_is_int128 == 1) ||
              a.kind_tag == rp::AGG_AVG)) {
          g_ok = false;
          break;
        }
      }
      if (g_ok) {
        // Collect nullable column NAMES (same storage-index + NOT_NULL-constraint basis
        // as any_nullable_projected; name via the catalog) and decline if any group key
        // is among them.
        std::set<std::string> nullable_names;
        for (auto *g : jt.gets) {
          auto te = g->GetTable();
          if (!te) { g_ok = false; break; }
          for (auto &ci : g->GetColumnIds()) {
            if (ci.IsRowIdColumn() || ci.IsVirtualColumn()) { continue; }
            idx_t cidx = ci.GetPrimaryIndex();
            bool nn = false;
            for (auto &cons : te->GetConstraints()) {
              if (cons->type == ConstraintType::NOT_NULL &&
                  cons->Cast<NotNullConstraint>().index.index == cidx) {
                nn = true;
                break;
              }
            }
            if (!nn) { nullable_names.insert(te->GetColumn(LogicalIndex(cidx)).Name()); }
          }
        }
        if (g_ok) {
          for (auto &gk : group_key_cols) {
            if (nullable_names.count(gk.second)) { g_ok = false; break; }
          }
        }
      }
      if (g_ok) { all_ok = true; }
    }

    if (!all_ok && any_nullable_projected()) { return false; }
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
  case rp::TYPE_UTINYINT:  return LogicalType::UTINYINT;
  case rp::TYPE_USMALLINT: return LogicalType::USMALLINT;
  case rp::TYPE_UINTEGER:  return LogicalType::UINTEGER;
  case rp::TYPE_UBIGINT:   return LogicalType::UBIGINT;
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

// Phase G Stage 1 (flag-gated, additive): for the COLD fact-materialize request
// of a fully-fixed-width-decodable aggregate, feed the fact columns from the
// GPU-direct DuckDB-native segment decoder instead of running the nested
// Connection::Query + chunk-gather. Defined after the native-storage helpers
// (ResolveNativeColumn / ForEachColumnSegment / CodecToMojo / TypeCodeForSize),
// so it is forward-declared here for use in GetGlobalSourceState.
//
// `fact_cols` are the projected fact column names parsed from the request-0
// materialize SQL (deterministic `SELECT c0, c1, ... FROM <fact_table>` form).
// Returns true iff every fact column was decoded on-GPU and fed (the request is
// fully satisfied); false means NOTHING was fed and the caller MUST run the
// normal Connection::Query feed for the whole request (never partial). When it
// returns true, `*out_bytes_moved` carries the host->device bytes uploaded (sum
// of decoded segment byte sizes) for optional instrumentation.
bool TryGpuDirectFactFeed(ClientContext &context, void *h,
                          const std::string &fact_table,
                          const std::vector<std::string> &fact_cols,
                          int64_t kind, int64_t *out_bytes_moved);

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

        // SKIP-MATERIALIZE (GPU_OP_COLPOOL=2): when active for this query, the
        // narrowed SELECT (emitted by materialize_sql) reads only the NON-resident
        // fact columns and the finalize sources the resident ones from the pool.
        // BYPASS the GPU-direct fact feed entirely (it would re-decode the full
        // column set) and use the narrowed SQL feed below. (The decode-skip is
        // deferred.) Recompute per request; only the fact request (i==0) matters.
        bool skipmat = (mojo_gpu_skipmat_active(h) == 1);

        // Phase G Stage 1 (flag-gated): for the FACT request (i==0) ONLY, when
        // GPU_OP_NATIVE_DECODE is set and every projected fact column resolves to
        // a fully-decodable native column, feed the fact columns from the
        // GPU-direct segment decoder instead of this nested SQL query + gather.
        // Eligibility / decode is all-or-nothing per request; on any unsupported
        // column/segment TryGpuDirectFactFeed feeds NOTHING and returns false, so
        // we fall through to the unchanged Connection::Query feed below.
        if (i == 0 && !skipmat && std::getenv("GPU_OP_NATIVE_DECODE")) {
          int64_t kind = mojo_gpu_desc_kind(h);
          // Allow Q6 (ungrouped, all-fixed-width fact request). Other kinds stay
          // on the SQL feed even with the flag on (tightly scoped first cut).
          if (kind == rp::KIND_Q6) {
            char fbuf[256] = {0};
            int64_t flen2 = mojo_gpu_desc_fact_table(
                h, reinterpret_cast<uint8_t *>(fbuf), (int64_t)sizeof(fbuf) - 1);
            if (flen2 < 0) { flen2 = 0; }
            if (flen2 > (int64_t)sizeof(fbuf) - 1) { flen2 = (int64_t)sizeof(fbuf) - 1; }
            fbuf[flen2] = '\0';
            std::string fact_table(fbuf);
            // Parse the projected fact column names out of the deterministic
            // request-0 SQL: "SELECT c0, c1, ... FROM <fact_table>" (no ORDER BY
            // for Q6 / UNGROUPED). This is the exact column order feed_column
            // expects (matches _materialize_columns).
            std::vector<std::string> fact_cols;
            {
              const std::string kSel = "SELECT ";
              const std::string kFrom = " FROM ";
              auto p0 = sql.find(kSel);
              auto p1 = sql.find(kFrom);
              if (p0 == 0 && p1 != std::string::npos && p1 > kSel.size()) {
                std::string mid = sql.substr(kSel.size(), p1 - kSel.size());
                size_t start = 0;
                while (start <= mid.size()) {
                  size_t comma = mid.find(',', start);
                  std::string tok = mid.substr(
                      start, comma == std::string::npos ? std::string::npos : comma - start);
                  // trim surrounding whitespace
                  size_t a = tok.find_first_not_of(" \t");
                  size_t b = tok.find_last_not_of(" \t");
                  if (a != std::string::npos) { fact_cols.push_back(tok.substr(a, b - a + 1)); }
                  if (comma == std::string::npos) { break; }
                  start = comma + 1;
                }
              }
            }
            int64_t bytes_moved = 0;
            if (!fact_cols.empty() &&
                TryGpuDirectFactFeed(context, h, fact_table, fact_cols, kind, &bytes_moved)) {
              continue;  // request fully fed via GPU-direct decode; skip SQL feed
            }
          }
        }

        // SKIP-MATERIALIZE landmine #1 (all-resident edge): when the narrowed
        // request-0 SELECT has ZERO projected columns (every fact column is
        // pool-resident), the SQL is "SELECT  FROM <table>" -- ILLEGAL to run.
        // Detect the empty projection list and SKIP the fact Connection::Query
        // entirely. materialize_sql already seeded st.n_rows from the resident row
        // count for this case, so the finalize sources every column from the pool
        // and knows the row count. (A genuine query whose projection is empty is
        // impossible here -- materialize_sql only emits an empty list when it
        // omitted every column after a guaranteed pool HIT.)
        if (i == 0 && skipmat) {
          const std::string kSel = "SELECT ";
          const std::string kFrom = " FROM ";
          auto p0 = sql.find(kSel);
          auto p1 = sql.find(kFrom);
          bool empty_projection = false;
          if (p0 == 0 && p1 != std::string::npos && p1 >= kSel.size()) {
            std::string mid = sql.substr(kSel.size(), p1 - kSel.size());
            empty_projection = (mid.find_first_not_of(" \t") == std::string::npos);
          }
          if (empty_projection) {
            continue;  // all fact columns resident -> nothing to scan/feed
          }
        }

        Connection con(*context.db);
        auto res = con.Query(sql);
        if (res->HasError()) {
          throw InvalidInputException("GPU_AGG: materialize query failed: " + res->GetError());
        }
        idx_t total_rows = res->RowCount();
        idx_t n_cols = res->types.size();

        // SKIP-MATERIALIZE landmine #1: set st.n_rows for the FACT request from the
        // observed row count BEFORE the feed loop. Required so the finalize knows
        // n_rows even for an OMITTED column (whose feed_column never runs); for fed
        // columns feed_column sets the same value, so this is idempotent. Gated by
        // `skipmat` so the non-skip-materialize paths (flag off / =1 / Q1/Q3/Q5)
        // are byte-IDENTICAL to before -- they never call this extra C-ABI entry.
        if (i == 0 && skipmat) {
          mojo_gpu_feed_rowcount(h, (int64_t)total_rows);
        }

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
        // GPU_OP_NULLABLE: per-column per-row validity staging (1=valid, 0=NULL).
        // Only allocated/walked when the flag is ON (default-off => zero overhead,
        // byte-identical to today). buf_valid[c] stays row-aligned with the data
        // (1's appended for AllValid chunks); col_has_null[c] records whether the
        // column ever held a NULL, so only those columns are fed validity below.
        const bool nullable_on = GpuOpNullableOn();
        std::vector<std::vector<uint8_t>> buf_valid(nullable_on ? n_cols : 0);
        std::vector<char> col_has_null(n_cols, 0);
        if (nullable_on) {
          for (idx_t c = 0; c < n_cols; c++) { buf_valid[c].reserve(total_rows); }
        }
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
            // GPU_OP_NULLABLE: capture validity for EVERY column (before the VARCHAR
            // branch's `continue`) so buf_valid[c] stays row-aligned. A NULL row is
            // read as raw garbage data, so the finalize must know to exclude it.
            if (nullable_on) {
              auto &vmask = FlatVector::Validity(chunk->data[c]);
              if (vmask.AllValid()) {
                buf_valid[c].insert(buf_valid[c].end(), n, (uint8_t)1);
              } else {
                col_has_null[c] = 1;
                for (idx_t r = 0; r < n; r++) {
                  buf_valid[c].push_back(vmask.RowIsValid(r) ? (uint8_t)1 : (uint8_t)0);
                }
              }
            }
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
          // GPU_OP_STATS: pass the source decimal scale (0 for non-DECIMAL) so the
          // finalize builds col_div from the column's ACTUAL scale, not a hardcode.
          int64_t dscale =
              (ct.id() == LogicalTypeId::DECIMAL) ? (int64_t)DecimalType::GetScale(ct) : 0;
          int64_t rc = mojo_gpu_feed_column(h, i, (int64_t)c, ptr,
                                            (int64_t)total_rows, tag, dscale);
          if (rc != 0) {
            throw InvalidInputException("GPU_AGG: feed_column failed (rc " +
                                        std::to_string(rc) + ")");
          }
        }
        // GPU_OP_NULLABLE: feed validity for any column that held a NULL (others
        // stay all-valid Mojo-side). Same (req_i=i, col_j=c) addressing; the Mojo
        // side translates col_j through fed_pos_to_matcol exactly as feed_column.
        if (nullable_on) {
          for (idx_t c = 0; c < n_cols; c++) {
            if (!col_has_null[c]) { continue; }
            int64_t vrc = mojo_gpu_feed_validity(h, i, (int64_t)c,
                                                 buf_valid[c].data(),
                                                 (int64_t)total_rows);
            if (vrc != 0) {
              throw InvalidInputException("GPU_AGG: feed_validity failed (rc " +
                                          std::to_string(vrc) + ")");
            }
          }
        }
      }
    }

    int64_t fin_rc = mojo_gpu_pin_finalize(h);
    // Audit Group G: domain-error rc codes from the transcendental f64 finalize.
    // A single out-of-domain row (sqrt of a negative / log of a non-positive)
    // poisons the f64 aggregate to NaN; stock DuckDB RAISES rather than returning
    // nan, so surface the matching OutOfRangeException here (rc 10=sqrt, 11=log,
    // 12=unspecified domain). This is NOT a fallback-to-CPU path -- like stock, the
    // query errors.
    if (fin_rc == 10) {
      throw OutOfRangeException("cannot take square root of a negative number");
    }
    if (fin_rc == 11) {
      throw OutOfRangeException("cannot take logarithm of a negative number");
    }
    if (fin_rc == 12) {
      throw OutOfRangeException(
          "transcendental aggregate argument out of domain");
    }
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
        // GPU_OP_STATS: a degenerate stat cell (e.g. var_samp of <2 rows,
        // regr_intercept with zero independent variance) is a real SQL NULL.
        // Non-stat paths always report valid=1, so this is a no-op there.
        if (mojo_gpu_result_valid(h, row, (int64_t)c) == 0) {
          FlatVector::SetNull(chunk.data[c], r, true);
          continue;
        }
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
        // Unsigned integer results (GPU_OP_STATS: regr_count). The assembler stores
        // the non-negative count as a signed int64 (res_lo); narrow to the column's
        // unsigned physical width here so the output vector matches the parent plan.
        case LogicalTypeId::UTINYINT: {
          FlatVector::GetData<uint8_t>(chunk.data[c])[r] =
              (uint8_t)mojo_gpu_result_i64(h, row, (int64_t)c);
          break;
        }
        case LogicalTypeId::USMALLINT: {
          FlatVector::GetData<uint16_t>(chunk.data[c])[r] =
              (uint16_t)mojo_gpu_result_i64(h, row, (int64_t)c);
          break;
        }
        case LogicalTypeId::UINTEGER: {
          FlatVector::GetData<uint32_t>(chunk.data[c])[r] =
              (uint32_t)mojo_gpu_result_i64(h, row, (int64_t)c);
          break;
        }
        case LogicalTypeId::UBIGINT: {
          FlatVector::GetData<uint64_t>(chunk.data[c])[r] =
              (uint64_t)mojo_gpu_result_i64(h, row, (int64_t)c);
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

  // GPU_OP_TRANSCENDENTAL: a transcendental sum/avg of f(col) has no TPC-H kind
  // when grouped (KIND_UNKNOWN for a GROUP BY shape), and an UNGROUPED one borrows
  // KIND_Q6 (1 agg, no dims). Enable routing whenever the descriptor carries a
  // transcendental op (the Mojo scope guard already validated the shape +
  // NVIDIA-only + flag-gated the op emission). Honors GPU_OP_GENERIC restriction
  // only via off/none above; a named restriction (e.g. "q3") does not list a
  // transcendental, so route it unless GPU_OP_GENERIC explicitly excludes all.
  if (GpuOpFlagOn("GPU_OP_TRANSCENDENTAL") &&
      mojo_gpu_desc_is_transcendental(h)) {
    enabled = true;
  }

  // GPU_OP_STATS: a statistical aggregate plan (stddev/var/covar/corr/regr_*) has
  // no TPC-H kind (KIND_UNKNOWN). Enable routing whenever the descriptor carries a
  // stat aggregate -- the Mojo scope guard already validated the shape (UNGROUPED/
  // DENSE, no FK dims, NVIDIA-only) and flag-gated the agg-kind emission.
  if (GpuOpFlagOn("GPU_OP_STATS") && mojo_gpu_desc_is_stats(h)) {
    enabled = true;
  }

  // A1 (GPU_OP_NULLABLE): an UNGROUPED int multi-aggregate (count(*) + SUM/AVG over
  // possibly-NULLABLE columns) classifies KIND_UNKNOWN -- no single TPC-H kind matches
  // 2+ aggregates -- but the generic ungrouped int128 kernel executes it, and the A1
  // metric lowering gives count(*) its own (unmultiplied) metric + each SUM/AVG its own
  // validity multiply. Enable it when GPU_OP_NULLABLE is on; the accessor restricts to
  // UNGROUPED all-int {SUM,AVG,count(*)} so an f64 stat/transcendental is never mixed
  // onto the int path (and single-agg int already routes via KIND_Q6, so this only adds
  // the multi-aggregate case). The SerializeMatchedPlan nullable gate already
  // fail-closed any nullable shape outside the accepted set before we got here.
  if (GpuOpNullableOn() && mojo_gpu_desc_a1_ungrouped_ok(h)) {
    enabled = true;
  }

  // GPU_OP_NULLABLE_GROUPED: a DENSE int GROUP BY (count(*) + SUM/AVG over nullable
  // columns, NOT-NULL group key) classifies KIND_UNKNOWN -- the generic dense int128
  // kernel + the A1 per-group machinery execute it. The SerializeMatchedPlan grouped
  // gate above already required NOT-NULL group keys + accepted agg kinds.
  if (GpuOpNullableGroupedOn() && mojo_gpu_desc_a1_grouped_ok(h)) {
    enabled = true;
  }

  if (!enabled) { mojo_gpu_desc_free(h); return false; }

  // item 5: below the crossover, the co-installed CPU SIMD kernels (or stock) win
  // -- decline so the descriptor isn't routed to the GPU.
  if (BelowGpuCrossover(*node)) { mojo_gpu_desc_free(h); return false; }

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

// === Phase G Stage 1: GPU-direct fact-column feed (flag-gated, additive) ======
// Decode every fact column's native storage segments on the GPU (the exact path
// gpu_native_decode_check proves bit-exact) and feed them via mojo_gpu_feed_column
// in the same column order the SQL feed would. All-or-nothing per request.
//
// One column's segments, decoded into a single contiguous host buffer in scan
// order, with the metadata feed_column needs.
struct DecodedFactColumn {
  int type_code = -1;          // 0 = int32 (4B), 1 = int64 (8B)
  int64_t tag = rp::TYPE_INVALID;
  int64_t dec_scale = 0;       // DECIMAL scale (0 for non-DECIMAL); GPU_OP_STATS
  idx_t n_rows = 0;
  int64_t bytes_moved = 0;     // sum of decoded segment byte sizes (H->D)
  std::vector<int32_t> v32;
  std::vector<int64_t> v64;
};

// Decode all DATA segments of one native column. Returns false (leaving `out`
// untouched/partial) the moment any segment is unsupported -- the caller treats
// that as ineligible for the whole request. Mirrors gpu_native_decode_check:
//   * only the top-level data column path ("[N]", no comma) is decoded;
//   * codec must be UNCOMPRESSED or BITPACKING (CodecToMojo >= 0);
//   * the segment must be pinnable (base != null);
//   * for BITPACKING, every 2048-row group's mode must be one of the four
//     implemented fixed-width modes (FOR/CONSTANT/CONSTANT_DELTA/DELTA_FOR);
//   * has_updates segments are rejected (the persistent block bytes would be
//     stale -- extra-conservative beyond the proven check).
bool DecodeNativeColumnForFeed(ClientContext &context, NativeColumnRef &ref,
                               DecodedFactColumn &out) {
  out.type_code = TypeCodeForSize(ref.physical_type_size);
  if (out.type_code < 0) { return false; }
  out.tag = LogicalTypeToTag(ref.type);
  // DECIMAL scale (0 for non-DECIMAL). The GPU_OP_STATS f64 path needs the real
  // scale to build col_div (10^scale); the transcendental path defaults to the
  // TPC-H scale-2 fallback, but threading the true scale here is always correct.
  out.dec_scale = (ref.type.id() == LogicalTypeId::DECIMAL)
                      ? (int64_t)DecimalType::GetScale(ref.type)
                      : 0;

  bool ok = true;
  idx_t global_row = 0;
  ForEachColumnSegment(context, ref, [&](idx_t /*idx*/, PinnedSegment &ps) {
    if (!ok) { return; }
    // Only the top-level data column ("[N]"); skip validity / nested children.
    if (ps.info.column_path.find(',') != std::string::npos) { return; }
    if (ps.info.has_updates) { ok = false; return; }
    int codec = CodecToMojo(EnumUtil::FromString<CompressionType>(ps.info.compression_type.c_str()));
    if (!ps.base || codec < 0) { ok = false; return; }
    idx_t count = ps.info.segment_count;
    idx_t start = global_row;
    global_row += count;
    if (codec == 1) {
      idx_t n_groups = (count + 2048 - 1) / 2048;
      for (idx_t g = 0; g < n_groups; g++) {
        std::string m; int64_t f, w, doff;
        ParseBitpackingGroup(ps.base, ps.seg_bytes, ref.physical_type_size, g, m, f, w, doff);
        if (!(m == "FOR" || m == "CONSTANT" || m == "CONSTANT_DELTA" || m == "DELTA_FOR")) {
          ok = false; return;
        }
      }
    }
    // Decode into the contiguous output at the running global offset.
    int32_t rc;
    if (out.type_code == 0) {
      out.v32.resize(start + count);
      rc = mojo_gpu_decode_segment(ps.base, NumericCast<int64_t>(ps.seg_bytes),
                                   NumericCast<int64_t>(count), codec, 0, out.v32.data() + start);
    } else {
      out.v64.resize(start + count);
      rc = mojo_gpu_decode_segment(ps.base, NumericCast<int64_t>(ps.seg_bytes),
                                   NumericCast<int64_t>(count), codec, 1, out.v64.data() + start);
    }
    if (rc != 0) { ok = false; return; }
    out.bytes_moved += NumericCast<int64_t>(ps.seg_bytes);
  });
  if (!ok) { return false; }
  out.n_rows = global_row;
  return true;
}

bool TryGpuDirectFactFeed(ClientContext &context, void *h,
                          const std::string &fact_table,
                          const std::vector<std::string> &fact_cols,
                          int64_t /*kind*/, int64_t *out_bytes_moved) {
  // Pass 1: resolve + decode every fact column. Any failure => feed nothing.
  std::vector<DecodedFactColumn> decoded(fact_cols.size());
  idx_t expect_rows = 0;
  int64_t total_bytes = 0;
  for (size_t c = 0; c < fact_cols.size(); c++) {
    NativeColumnRef ref;
    try {
      ref = ResolveNativeColumn(context, fact_table, fact_cols[c]);
    } catch (...) {
      return false;  // e.g. parquet-backed / no native segments / bad name
    }
    if (!DecodeNativeColumnForFeed(context, ref, decoded[c])) { return false; }
    total_bytes += decoded[c].bytes_moved;
    if (c == 0) { expect_rows = decoded[c].n_rows; }
    else if (decoded[c].n_rows != expect_rows) { return false; }  // ragged => bail
  }
  if (decoded.empty() || expect_rows == 0) { return false; }

  // Pass 2: feed each decoded column once, in the SQL-feed column order.
  for (size_t c = 0; c < fact_cols.size(); c++) {
    void *ptr = (decoded[c].type_code == 0)
                    ? static_cast<void *>(decoded[c].v32.data())
                    : static_cast<void *>(decoded[c].v64.data());
    // Thread the real DECIMAL scale so the GPU_OP_STATS f64 path builds col_div
    // (10^scale) correctly. With predicate-independent residency the stats/
    // transcendental f64 path NOW feeds through native-decode (it is the
    // GPU_OP_NATIVE_DECODE-gated residency prerequisite), so a hardcoded 0 here
    // gave dimensioned stats (stddev/var/covar) a 100x scale error. 0 for non-
    // DECIMAL columns (dates/ints), matching the SQL-feed path's GetScale.
    int64_t rc = mojo_gpu_feed_column(h, 0, (int64_t)c, ptr,
                                      NumericCast<int64_t>(decoded[c].n_rows),
                                      decoded[c].tag, decoded[c].dec_scale);
    if (rc != 0) {
      // A feed failure mid-request would leave the request half-fed; the feed
      // path overwrites per (req,col) and finalize would read stale slots. This
      // is not expected (the columns decoded cleanly), so surface it loudly.
      throw InvalidInputException("GPU_AGG: native-decode feed_column failed (rc " +
                                  std::to_string(rc) + ") for fact col " + fact_cols[c]);
    }
    fprintf(stderr, "[native-decode] fed fact col %s (rows=%lld) via GPU-direct decode\n",
            fact_cols[c].c_str(), (long long)decoded[c].n_rows);
  }
  if (out_bytes_moved) { *out_bytes_moved = total_bytes; }
  fprintf(stderr,
          "[native-decode] fact request fed %zu cols, %lld rows, %lld host->device bytes\n",
          fact_cols.size(), (long long)expect_rows, (long long)total_bytes);
  return true;
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
    // attribute any (still-unimplemented) zero-filled groups to `deferred_rows`
    // rather than counting them as decode failures. A `deferred` row is one
    // whose group mode is NOT implemented in the kernel (deterministic
    // zero-fill). The four fixed-width modes -- FOR, CONSTANT, CONSTANT_DELTA,
    // and DELTA_FOR -- are all implemented now, so a mismatch in any of them is
    // a real bug. Only INVALID / AUTO / unknown modes remain deferred.
    auto group_deferred = [&](idx_t row_in_seg) -> bool {
      if (codec != 1) return false;  // UNCOMPRESSED is always implemented
      idx_t g = row_in_seg / 2048;
      string m; int64_t f, w, doff;
      ParseBitpackingGroup(ps.base, ps.seg_bytes, ref.physical_type_size, g, m, f, w, doff);
      return !(m == "FOR" || m == "CONSTANT" || m == "CONSTANT_DELTA" || m == "DELTA_FOR");
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
    bd->status = "FAIL: " + std::to_string(bd->mismatches) +
                 " mismatches in implemented (CONSTANT/CONSTANT_DELTA/FOR/DELTA_FOR) groups";
  } else if (bd->deferred_rows == 0) {
    bd->status = "PASS: " + std::to_string(bd->checked_rows) + " values bit-identical";
  } else {
    bd->status = "PASS (partial): " + std::to_string(implemented) +
                 " values bit-identical; " + std::to_string(bd->deferred_rows) +
                 " rows in deferred (INVALID/AUTO/unknown) groups (zero-filled, not yet implemented)";
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

// gpu_colpool_status() -> one row (uploaded_bytes BIGINT): the monotonic count
// of bytes the column pool pushed H2D on misses (GPU_OP_COLPOOL). The dedup
// proof selects this between queries; the DELTA shrinks once shared columns are
// resident. Observability only -- does not touch the pool / affect results.
struct GpuColPoolStatusBindData : public TableFunctionData {
  int64_t uploaded_bytes = 0;
  int64_t pool_bytes = 0;
  int64_t pin2_bytes = 0;
  int64_t budget_mb = 0;
  // Phase 2 cost-aware placement observability (appended after the Phase 1
  // columns so existing positional selects keep working).
  int64_t hits = 0;
  int64_t misses = 0;
  int64_t evictions = 0;
  int64_t resident_cols = 0;
  int64_t promoted_cols = 0;
  int64_t costaware = 0;
};

unique_ptr<FunctionData> GpuColPoolStatusBind(ClientContext &, TableFunctionBindInput &,
                                              vector<LogicalType> &return_types, vector<string> &names) {
  auto bd = make_uniq<GpuColPoolStatusBindData>();
  bd->uploaded_bytes = mojo_gpu_colpool_uploaded_bytes();
  bd->pool_bytes = mojo_gpu_colpool_pool_bytes();
  bd->pin2_bytes = mojo_gpu_colpool_pin2_bytes();
  bd->budget_mb = NumericCast<int64_t>(PinBudgetBytes() / (1024ull * 1024ull));
  bd->hits = mojo_gpu_colpool_hits();
  bd->misses = mojo_gpu_colpool_misses();
  bd->evictions = mojo_gpu_colpool_evictions();
  bd->resident_cols = mojo_gpu_colpool_resident_cols();
  bd->promoted_cols = mojo_gpu_colpool_promoted_cols();
  bd->costaware = mojo_gpu_colpool_costaware();
  return_types = {LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT, LogicalType::BIGINT, LogicalType::BIGINT,
                  LogicalType::BIGINT};
  names = {"uploaded_bytes", "pool_bytes", "pin2_bytes", "budget_mb",
           "hits", "misses", "evictions", "resident_cols", "promoted_cols",
           "costaware"};
  return std::move(bd);
}

void GpuColPoolStatusFunc(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<GpuColPoolStatusBindData>();
  auto &gs = data.global_state->Cast<GpuNativeTFState>();
  if (gs.offset > 0) { output.SetCardinality(0); return; }
  FlatVector::GetData<int64_t>(output.data[0])[0] = bd.uploaded_bytes;
  FlatVector::GetData<int64_t>(output.data[1])[0] = bd.pool_bytes;
  FlatVector::GetData<int64_t>(output.data[2])[0] = bd.pin2_bytes;
  FlatVector::GetData<int64_t>(output.data[3])[0] = bd.budget_mb;
  FlatVector::GetData<int64_t>(output.data[4])[0] = bd.hits;
  FlatVector::GetData<int64_t>(output.data[5])[0] = bd.misses;
  FlatVector::GetData<int64_t>(output.data[6])[0] = bd.evictions;
  FlatVector::GetData<int64_t>(output.data[7])[0] = bd.resident_cols;
  FlatVector::GetData<int64_t>(output.data[8])[0] = bd.promoted_cols;
  FlatVector::GetData<int64_t>(output.data[9])[0] = bd.costaware;
  output.SetCardinality(1);
  gs.offset += 1;
}

void RegisterGpuColPoolStatusTableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_colpool_status", {}, GpuColPoolStatusFunc, GpuColPoolStatusBind, GpuNativeTFInit);
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
  RegisterGpuColPoolStatusTableFunction(loader);      // column-pool uploaded-bytes (dedup proof)
  RegisterGpuUnpinTableFunctions(loader);             // explicit unpin (key / all)
  RegisterGpuPinTableTableFunction(loader);           // pre-pin a kNN embedding column warm
  if (GpuOpOverridesOn()) {                            // NR1: declined queries -> Mojo SIMD kernels before stock
    RegisterMojoOverrides(loader.GetDatabaseInstance());
  }
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
  if (duckdb::GpuOpOverridesOn()) {  // NR1: parity with the LOAD path
    duckdb::RegisterMojoOverrides(*con->context->db);
  }
}
}
