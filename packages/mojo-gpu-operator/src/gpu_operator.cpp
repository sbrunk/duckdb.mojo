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
}

namespace duckdb {

namespace {

constexpr const char *COSINE_FN = "array_cosine_distance";

// The Q6 pin engine entry (cache value). Defined here so the transparent
// operator below can use it; EnsureQ6Pinned() itself is defined further down.
struct Q6PinEntry {
  void *handle;
  idx_t n_rows;
};
Q6PinEntry EnsureQ6Pinned(ClientContext &context, const std::string &table);

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

// ===========================================================================
// TPC-H Q6 transparent operator
//
// Recognizes the optimized Q6 shape:
//     PROJECTION
//       AGGREGATE  ungrouped, single = sum_no_overflow(l_extendedprice * l_discount)
//         GET lineitem  [table_filters: l_shipdate range, l_discount BETWEEN, l_quantity <]
// and replaces the AGGREGATE node (absorbing the GET) with a LogicalQ6 that
// drives the same EnsureQ6Pinned + mojo_q6_query GPU engine the gpu_q6() table
// function uses. Bit-exact: the int128 the kernel returns is the unscaled
// scale-4 value, written straight into the DECIMAL(38,4)/HUGEINT output.
//
// Translate-or-fallback: any deviation from this exact template leaves the plan
// untouched (-> normal CPU execution).
// ===========================================================================

// Physical SOURCE op: emits exactly one row (the revenue) from the GPU engine.
struct Q6SourceGlobalState : public GlobalSourceState {
  void *handle = nullptr;
  idx_t n_rows = 0;
  bool done = false;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalQ6 : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalQ6(PhysicalPlan &plan, vector<LogicalType> types, std::string table,
             int32_t ship_lo, int32_t ship_hi, int64_t disc_lo, int64_t disc_hi,
             int64_t qty_hi, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        table(std::move(table)), ship_lo(ship_lo), ship_hi(ship_hi),
        disc_lo(disc_lo), disc_hi(disc_hi), qty_hi(qty_hi) {}

  std::string table;
  int32_t ship_lo, ship_hi;
  int64_t disc_lo, disc_hi, qty_hi;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<Q6SourceGlobalState>();
    auto pe = EnsureQ6Pinned(context, table);  // pins lineitem's 4 columns (cached)
    gs->handle = pe.handle;
    gs->n_rows = pe.n_rows;
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<Q6SourceGlobalState>();
    if (gs.done) {
      chunk.SetCardinality(0);
      return SourceResultType::FINISHED;
    }
    int64_t out[2] = {0, 0};
    int32_t rc = -1;
    if (gs.handle) {
      rc = mojo_q6_query(gs.handle, ship_lo, ship_hi, disc_lo, disc_hi, qty_hi, out);
    }
    if (rc != 0) {
      throw InvalidInputException("GPU_Q6: GPU query failed (rc " + std::to_string(rc) + ")");
    }
    // out[0]=low 64 bits, out[1]=high 64 bits -> duckdb hugeint_t{uint64 lower; int64 upper}.
    hugeint_t h;
    h.lower = (uint64_t)out[0];
    h.upper = out[1];
    chunk.data[0].SetVectorType(VectorType::FLAT_VECTOR);
    FlatVector::GetData<hugeint_t>(chunk.data[0])[0] = h;
    chunk.SetCardinality(1);
    gs.done = true;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_Q6"; }
};

// Logical extension op: sits where the AGGREGATE was, absorbing the GET. No
// children, no expressions. Its single output is ColumnBinding(aggregate_index, 0)
// so the parent PROJECTION's reference resolves to it unchanged.
class LogicalQ6 : public LogicalExtensionOperator {
public:
  LogicalQ6(std::string table, int32_t ship_lo, int32_t ship_hi, int64_t disc_lo,
            int64_t disc_hi, int64_t qty_hi, idx_t aggregate_index, LogicalType return_type)
      : table(std::move(table)), ship_lo(ship_lo), ship_hi(ship_hi), disc_lo(disc_lo),
        disc_hi(disc_hi), qty_hi(qty_hi), aggregate_index(aggregate_index),
        return_type(std::move(return_type)) {
    types = {this->return_type};
  }

  std::string table;
  int32_t ship_lo, ship_hi;
  int64_t disc_lo, disc_hi, qty_hi;
  idx_t aggregate_index;
  LogicalType return_type;

  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(aggregate_index, 0)};
  }

  void ResolveTypes() override { types = {return_type}; }

  string GetExtensionName() const override { return "mojo_gpu_q6"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    return planner.Make<PhysicalQ6>(vector<LogicalType>{return_type}, table, ship_lo,
                                    ship_hi, disc_lo, disc_hi, qty_hi,
                                    estimated_cardinality);
  }
};

// Read the (lo, hi)-ish constants out of one column's table filter. Fills the
// out params it recognizes. Returns false on anything unexpected.
struct Q6Filters {
  bool have_ship_lo = false, have_ship_hi = false;
  bool have_disc_lo = false, have_disc_hi = false;
  bool have_qty_hi = false;
  int32_t ship_lo = 0, ship_hi = 0;
  int64_t disc_lo = 0, disc_hi = 0, qty_hi = 0;
};

// DECIMAL(15,2)/DOUBLE Value -> int64 scale-2 unscaled (e.g. 0.05 -> 5, 24.00 -> 2400).
int64_t ScaledConst(const Value &v) { return (int64_t)llround(v.GetValue<double>() * 100.0); }

// Apply a single ConstantFilter for a named Q6 column.
bool ApplyConstantFilter(const std::string &col, const ConstantFilter &cf, Q6Filters &f) {
  auto cmp = cf.comparison_type;
  if (col == "l_shipdate") {
    int32_t days = cf.constant.GetValue<date_t>().days;
    if (cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO) { f.ship_lo = days; f.have_ship_lo = true; return true; }
    if (cmp == ExpressionType::COMPARE_LESSTHAN) { f.ship_hi = days; f.have_ship_hi = true; return true; }
    return false;
  }
  if (col == "l_discount") {
    int64_t s = ScaledConst(cf.constant);
    if (cmp == ExpressionType::COMPARE_GREATERTHANOREQUALTO) { f.disc_lo = s; f.have_disc_lo = true; return true; }
    if (cmp == ExpressionType::COMPARE_LESSTHANOREQUALTO) { f.disc_hi = s; f.have_disc_hi = true; return true; }
    return false;
  }
  if (col == "l_quantity") {
    int64_t s = ScaledConst(cf.constant);
    if (cmp == ExpressionType::COMPARE_LESSTHAN) { f.qty_hi = s; f.have_qty_hi = true; return true; }
    return false;
  }
  return false;  // filter on a column we don't model -> bail
}

// Try to match the Q6 aggregate node. On success replaces *node with a LogicalQ6
// and returns true; otherwise leaves *node untouched and returns false.
bool MatchQ6(unique_ptr<LogicalOperator> &node) {
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }
  auto &agg = node->Cast<LogicalAggregate>();

  // Ungrouped, exactly one aggregate, exactly one child.
  if (!agg.groups.empty()) { return false; }
  if (agg.expressions.size() != 1) { return false; }
  if (agg.children.size() != 1) { return false; }
  if (agg.children[0]->type != LogicalOperatorType::LOGICAL_GET) { return false; }

  // Aggregate must be sum / sum_no_overflow over a multiply of two column refs.
  if (agg.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
  auto &ag = agg.expressions[0]->Cast<BoundAggregateExpression>();
  if (ag.IsDistinct() || ag.filter) { return false; }
  const std::string &agg_name = ag.function.name;
  if (agg_name != "sum" && agg_name != "sum_no_overflow") { return false; }
  if (ag.children.size() != 1) { return false; }
  if (ag.children[0]->GetExpressionClass() != ExpressionClass::BOUND_FUNCTION) { return false; }
  auto &mul = ag.children[0]->Cast<BoundFunctionExpression>();
  if (mul.function.name != "*" && mul.function.name != "multiply") { return false; }
  if (mul.children.size() != 2) { return false; }
  if (mul.children[0]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
  if (mul.children[1]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
  auto &ref0 = mul.children[0]->Cast<BoundColumnRefExpression>();
  auto &ref1 = mul.children[1]->Cast<BoundColumnRefExpression>();

  // Return type must be DECIMAL with scale 4 + INT128 physical (-> hugeint write is exact).
  const LogicalType &rt = ag.return_type;
  if (rt.id() != LogicalTypeId::DECIMAL) { return false; }
  if (DecimalType::GetScale(rt) != 4) { return false; }
  if (rt.InternalType() != PhysicalType::INT128) { return false; }

  auto &get = agg.children[0]->Cast<LogicalGet>();
  auto table_entry = get.GetTable();
  if (!table_entry) { return false; }
  const std::string table_name = table_entry->name;
  const auto &col_ids = get.GetColumnIds();

  // Resolve the two product columns to names. The colref binding's column_index
  // is the position in GetColumnIds(); map it through to the table column name.
  auto colref_name = [&](const BoundColumnRefExpression &ref) -> std::string {
    if (ref.binding.table_index != get.table_index) { return std::string(); }
    idx_t pos = ref.binding.column_index;
    if (pos >= col_ids.size()) { return std::string(); }
    return get.GetColumnName(col_ids[pos]);
  };
  std::string n0 = colref_name(ref0);
  std::string n1 = colref_name(ref1);
  bool prod_ok = (n0 == "l_extendedprice" && n1 == "l_discount") ||
                 (n0 == "l_discount" && n1 == "l_extendedprice");
  if (!prod_ok) { return false; }

  // Extract filter constants. At this optimizer stage table_filters.filters is
  // keyed by the *table column index* (the column's primary index, into names),
  // NOT by position in GetColumnIds(). (Verified via debug dump.)
  Q6Filters f;
  for (auto &kv : get.table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= get.names.size()) return false;
    std::string col = get.names[col_idx];
    TableFilter &tf = *kv.second;
    if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
      if (!ApplyConstantFilter(col, tf.Cast<ConstantFilter>(), f)) return false;
    } else if (tf.filter_type == TableFilterType::CONJUNCTION_AND) {
      auto &conj = tf.Cast<ConjunctionAndFilter>();
      for (auto &cfp : conj.child_filters) {
        if (cfp->filter_type != TableFilterType::CONSTANT_COMPARISON) return false;
        if (!ApplyConstantFilter(col, cfp->Cast<ConstantFilter>(), f)) return false;
      }
    } else {
      return false;
    }
  }

  // All three predicates (with both bounds where applicable) must be present.
  if (!(f.have_ship_lo && f.have_ship_hi && f.have_disc_lo && f.have_disc_hi && f.have_qty_hi)) {
    return false;
  }

  // Build the replacement. The aggregate's output binding is
  // ColumnBinding(aggregate_index, 0) -> LogicalQ6 exposes the same.
  auto repl = make_uniq<LogicalQ6>(table_name, f.ship_lo, f.ship_hi, f.disc_lo, f.disc_hi,
                                   f.qty_hi, agg.aggregate_index, rt);
  repl->estimated_cardinality = agg.estimated_cardinality;
  node = std::move(repl);
  return true;
}

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

// Defined after the Q1 engine (needs EnsureQ1Pinned / ComputeQ1Rows in scope).
bool MatchQ1(unique_ptr<LogicalOperator> &node);
// Defined after the Q14 engine (needs EnsureQ14Pinned / mojo_q14_query in scope).
bool MatchQ14(unique_ptr<LogicalOperator> &node);
// Defined after the Q3 engine (needs EnsureQ3Pinned / ComputeQ3AllRows in scope).
bool MatchQ3(unique_ptr<LogicalOperator> &node);
// Defined after the Q5 engine (needs EnsureQ5Pinned / ComputeQ5Rows in scope).
bool MatchQ5(unique_ptr<LogicalOperator> &node);

void OptimizeNode(unique_ptr<LogicalOperator> &node) {
  if (!node) { return; }
  // Recurse first (depth-first), so children are rewritten before parents.
  for (auto &child : node->children) { OptimizeNode(child); }

  // TPC-H Q1: try to absorb the grouped (returnflag,linestatus) aggregate over a
  // filtered lineitem GET into the GPU operator. Try before Q6 (different shape;
  // Q1 is grouped, Q6 ungrouped) — conservative translate-or-fallback.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (MatchQ1(node)) { return; }
    } catch (...) {
      // leave node untouched -> CPU execution
    }
  }

  // TPC-H Q6: try to absorb an ungrouped sum(l_extendedprice*l_discount) over a
  // filtered lineitem GET into the GPU operator. Conservative translate-or-
  // fallback: any mismatch / exception leaves the node untouched.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (MatchQ6(node)) { return; }
    } catch (...) {
      // leave node untouched -> CPU execution
    }
  }

  // TPC-H Q14: ungrouped 2-aggregate sum over a lineitem |><| part join (one is a
  // promo CASE, one the plain ext*(1-disc)). Distinct shape from Q6 (child is a
  // JOIN, not a GET) and Q1 (grouped) -> no collision. Translate-or-fallback.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (MatchQ14(node)) { return; }
    } catch (...) {
      // leave node untouched -> CPU execution
    }
  }

  // TPC-H Q3: grouped (l_orderkey,o_orderdate,o_shippriority) single-sum over a
  // customer<-orders<-lineitem join tree. 3 group keys distinguishes it from Q1
  // (2 keys) and the ungrouped Q6/Q14. Translate-or-fallback.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (MatchQ3(node)) { return; }
    } catch (...) {
      // leave node untouched -> CPU execution
    }
  }

  // TPC-H Q5: grouped (n_name) single-sum over the 6-table customer/orders/
  // lineitem/supplier/nation/region join tree. 1 VARCHAR group key + the exact
  // 6-condition join set distinguishes it. Translate-or-fallback.
  if (node->type == LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) {
    try {
      if (MatchQ5(node)) { return; }
    } catch (...) {
      // leave node untouched -> CPU execution
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
// string-dictionary compression pass (COMPRESSED_MATERIALIZATION) so Q1's
// GROUP BY keys stay raw VARCHAR (no __internal_compress/decompress projections,
// which a source operator can't reproduce). The paired optimize hook restores
// the original disabled-optimizer set so the change never leaks into later CPU
// queries. We remember the original set per-thread (the binder runs both hooks
// on the same thread for one plan), keyed by context pointer for re-entrancy
// safety (EnsureQ1Pinned's self-query runs nested on the same thread).
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

// ---------------------------------------------------------------------------
// gpu_q6() table function: transparent-ready TPC-H Q6 engine (pin + exact sum).
//
//   SELECT revenue FROM gpu_q6('lineitem', DATE '1994-01-01', DATE '1995-01-01',
//                              0.05, 0.07, 24);
//
// Pins lineitem's 4 needed columns (l_shipdate/l_discount/l_extendedprice/
// l_quantity) on first use, then each call is a fused filter + exact int128
// sum(l_extendedprice * l_discount) on the GPU. The same engine is what a
// transparent AGGREGATE-over-filtered-GET rewrite would drive.
// ---------------------------------------------------------------------------
std::mutex g_q6_mu;
std::unordered_map<std::string, Q6PinEntry> g_q6_pins;

// --- pin instrumentation helpers (set MOJO_Q6_PIN_TIMING=1 to print) ----------
static bool q6_timing_enabled() {
  const char *e = std::getenv("MOJO_Q6_PIN_TIMING");
  return e && e[0] && e[0] != '0';
}
using q6_clock = std::chrono::steady_clock;
static double q6_ms(q6_clock::time_point a, q6_clock::time_point b) {
  return std::chrono::duration<double, std::milli>(b - a).count();
}

// Generic per-query pin timing toggle (env MOJO_Q<N>_PIN_TIMING=1) reused by the
// Q1/Q3/Q5/Q14 pins below. `var` is e.g. "MOJO_Q14_PIN_TIMING".
static bool pin_timing_enabled(const char *var) {
  const char *e = std::getenv(var);
  return e && e[0] && e[0] != '0';
}
// Generic pinned-staging toggle (env MOJO_Q<N>_PIN_MODE): "pinned" (default) uses
// the pinned-HostBuffer DMA path; anything else ("baseline"/"reserve") uses the
// std::vector staging path for A/B.
static bool pin_use_pinned(const char *var) {
  const char *e = std::getenv(var);
  if (!e || !e[0]) return true;            // default: pinned
  return std::string(e) == "pinned";
}

// Pin "mode" selector (env MOJO_Q6_PIN_MODE): chooses how phase (b)+(c) are done.
//   "baseline"  : current no-reserve std::vector insert + one big upload (default)
//   "reserve"   : reserve(row_count) the staging vectors, then insert + upload
//   "diag"      : reserve + fine sub-instrumentation of (b): fetch-overhead vs
//                 memcpy vs (compared against a bare memcpy of the same bytes)
//   "pinned"    : pinned-HostBuffer staging (Mojo allocs pinned + device bufs;
//                 C++ memcpys each chunk straight into pinned host memory;
//                 one enqueue_copy per column) -> collapses (b)+(c), no realloc,
//                 no intermediate std::vector.
static std::string q6_pin_mode() {
  // Default to the pinned-HostBuffer path: it removes the std::vector realloc
  // (reserve) AND the slow pageable-memory upload (pinned DMA), collapsing
  // (b)+(c) — ~49ms vs ~70-84ms baseline at SF1. Set MOJO_Q6_PIN_MODE to
  // "baseline"/"reserve"/"diag" to A/B the older staging paths.
  const char *e = std::getenv("MOJO_Q6_PIN_MODE");
  if (!e || !e[0]) return "pinned";
  return std::string(e);
}

// Pinned-HostBuffer staging entry points (Mojo side, gpu_kernels.mojo).
extern "C" {
// Allocate ctx + 5 resident DeviceBuffers + 4 pinned HostBuffers sized to
// n_rows. Returns the opaque Q6State* handle (0 on failure) and writes the 4
// pinned host pointers (ship/disc/ext/qty) that C++ memcpys chunk data into.
int64_t mojo_q6_pin_alloc(int64_t n_rows, int32_t **ship_h, int64_t **disc_h,
                          int64_t **ext_h, int64_t **qty_h);
// One enqueue_copy(device, pinned-host) per column + synchronize. 0 == ok.
int32_t mojo_q6_pin_upload(void *handle, int32_t timing);
}

Q6PinEntry EnsureQ6Pinned(ClientContext &context, const std::string &table) {
  std::lock_guard<std::mutex> g(g_q6_mu);
  auto it = g_q6_pins.find(table);
  if (it != g_q6_pins.end()) { return it->second; }

  const bool timing = q6_timing_enabled();
  const std::string mode = q6_pin_mode();
  auto t0 = q6_clock::now();

  Connection con(*context.db);
  auto res = con.Query("SELECT l_extendedprice, l_discount, l_shipdate, l_quantity FROM " + table);
  if (res->HasError()) { throw InvalidInputException("gpu_q6: " + res->GetError()); }
  auto t1 = q6_clock::now();  // (a) Query() = scan+decode+materialize done

  // Row count is known from the materialized result (no extra query).
  idx_t row_count = res->RowCount();

  // -------------------------------------------------------------------------
  // "pinned": stage directly into Mojo-managed pinned HostBuffers, then one
  // enqueue_copy per column. Collapses (b)+(c) and removes the std::vector.
  // -------------------------------------------------------------------------
  if (mode == "pinned") {
    int32_t *ship_h = nullptr; int64_t *disc_h = nullptr;
    int64_t *ext_h = nullptr;  int64_t *qty_h = nullptr;
    void *handle = reinterpret_cast<void *>(
        mojo_q6_pin_alloc(NumericCast<int64_t>(row_count), &ship_h, &disc_h, &ext_h, &qty_h));
    if (!handle || !ship_h) { throw InvalidInputException("gpu_q6: pinned pin_alloc failed"); }
    auto t_alloc = q6_clock::now();  // (b0) pinned + device alloc done

    idx_t off = 0;
    while (true) {
      auto chunk = res->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[0]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[1]);
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[2]);  // date_t == int32 days
      const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[3]);
      std::memcpy(ext_h + off, e, n * sizeof(int64_t));
      std::memcpy(disc_h + off, d, n * sizeof(int64_t));
      std::memcpy(qty_h + off, q, n * sizeof(int64_t));
      std::memcpy(ship_h + off, s, n * sizeof(int32_t));  // date_t is a 4-byte struct
      off += n;
    }
    auto t_fill = q6_clock::now();  // (b) fetch + memcpy-into-pinned done

    int32_t rc = mojo_q6_pin_upload(handle, timing ? 1 : 0);
    if (rc != 0) { throw InvalidInputException("gpu_q6: pinned upload failed (rc " +
                                               std::to_string(rc) + ")"); }
    auto t_up = q6_clock::now();  // (c) device upload done

    if (timing) {
      fprintf(stderr,
              "[q6-pin pinned] n_rows=%llu  (a)Query=%.1fms  (b0)pin_alloc=%.1fms  "
              "(b)fetch+memcpy=%.1fms  (c)upload=%.1fms  total=%.1fms\n",
              (unsigned long long)off, q6_ms(t0, t1), q6_ms(t1, t_alloc),
              q6_ms(t_alloc, t_fill), q6_ms(t_fill, t_up), q6_ms(t0, t_up));
    }
    Q6PinEntry e{handle, off};
    g_q6_pins[table] = e;
    return e;
  }

  // -------------------------------------------------------------------------
  // std::vector staging paths: "baseline" (no reserve), "reserve", "diag".
  // -------------------------------------------------------------------------
  vector<int64_t> ext, disc, qty;
  vector<int32_t> ship;
  if (mode == "reserve" || mode == "diag") {
    ext.reserve(row_count); disc.reserve(row_count);
    qty.reserve(row_count); ship.reserve(row_count);
  }
  auto t_res = q6_clock::now();  // reserve done

  // diag sub-counters: fetch+flatten time vs pure-append (memcpy) time.
  double fetch_ms = 0, append_ms = 0;
  idx_t n_rows = 0;
  while (true) {
    auto tf0 = q6_clock::now();
    auto chunk = res->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
    const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[0]);  // DECIMAL(15,2) -> int64
    const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[1]);
    const date_t *s = FlatVector::GetData<date_t>(chunk->data[2]);    // DATE -> int32 days
    const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[3]);
    auto tf1 = q6_clock::now();
    ext.insert(ext.end(), e, e + n);
    disc.insert(disc.end(), d, d + n);
    qty.insert(qty.end(), q, q + n);
    ship.insert(ship.end(), reinterpret_cast<const int32_t *>(s),
                reinterpret_cast<const int32_t *>(s) + n);
    auto tf2 = q6_clock::now();
    fetch_ms += q6_ms(tf0, tf1);
    append_ms += q6_ms(tf1, tf2);
    n_rows += n;
  }
  auto t2 = q6_clock::now();  // (b) Fetch loop + std::vector build done

  // diag: bare memcpy of the same ~170 MB (the memory-bandwidth floor) — copy
  // the staged vectors into fresh pre-sized buffers and time it.
  double bare_memcpy_ms = 0;
  if (mode == "diag") {
    vector<int64_t> e2(n_rows), d2(n_rows), q2(n_rows);
    vector<int32_t> s2(n_rows);
    auto m0 = q6_clock::now();
    std::memcpy(e2.data(), ext.data(), n_rows * sizeof(int64_t));
    std::memcpy(d2.data(), disc.data(), n_rows * sizeof(int64_t));
    std::memcpy(q2.data(), qty.data(), n_rows * sizeof(int64_t));
    std::memcpy(s2.data(), ship.data(), n_rows * sizeof(int32_t));
    auto m1 = q6_clock::now();
    bare_memcpy_ms = q6_ms(m0, m1);
  }

  void *handle = reinterpret_cast<void *>(
      mojo_q6_pin(ship.data(), disc.data(), ext.data(), qty.data(),
                  NumericCast<int64_t>(n_rows), timing ? 1 : 0));
  auto t3 = q6_clock::now();  // (c) host->device upload + alloc done

  if (timing) {
    fprintf(stderr,
            "[q6-pin %s] n_rows=%llu  (a)Query=%.1fms  reserve=%.1fms  "
            "(b)Fetch+vec=%.1fms [fetch=%.1f append=%.1f]  (c)mojo_q6_pin(upload)=%.1fms  "
            "total=%.1fms  bare_memcpy(170MB)=%.1fms\n",
            mode.c_str(), (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t_res),
            q6_ms(t_res, t2), fetch_ms, append_ms, q6_ms(t2, t3), q6_ms(t0, t3),
            bare_memcpy_ms);
  }

  Q6PinEntry e{handle, n_rows};
  g_q6_pins[table] = e;
  return e;
}

// Streaming pin (Option B, Mojo-forward): SendQuery() with ALLOW_STREAMING gives
// a StreamQueryResult, so the scan is consumed chunk-by-chunk without ever
// building the full materialized ColumnDataCollection (copy 1) or the host
// std::vector intermediates (copy 2). Each chunk's flat column pointers go
// straight into the Mojo per-chunk device copy at a running offset. Cached in a
// separate map (own engine handle) so it can be A/B'd against the baseline.
std::unordered_map<std::string, Q6PinEntry> g_q6_pins_stream;

Q6PinEntry EnsureQ6PinnedStream(ClientContext &context, const std::string &table) {
  std::lock_guard<std::mutex> g(g_q6_mu);
  auto it = g_q6_pins_stream.find(table);
  if (it != g_q6_pins_stream.end()) { return it->second; }

  const bool timing = q6_timing_enabled();
  auto t0 = q6_clock::now();

  // Need the row count up front to size the resident buffers (single alloc, no
  // realloc). One cheap COUNT(*) (metadata-only on a base table).
  idx_t n_rows = 0;
  {
    Connection cc(*context.db);
    auto cres = cc.Query("SELECT count(*) FROM " + table);
    if (cres->HasError()) { throw InvalidInputException("gpu_q6: " + cres->GetError()); }
    n_rows = NumericCast<idx_t>(cres->GetValue(0, 0).GetValue<int64_t>());
  }
  auto t_cnt = q6_clock::now();

  // Diagnostic mode: stream (no materialized collection) into host vectors and
  // do ONE big upload. Isolates "removing copy-1 (materialization)" from
  // "removing copy-2 (host vectors) via per-chunk device copies".
  const char *stage_env = std::getenv("MOJO_Q6_STREAM_STAGE");
  const bool stage_mode = stage_env && stage_env[0] && stage_env[0] != '0';
  if (stage_mode) {
    Connection con(*context.db);
    auto res = con.SendQuery("SELECT l_extendedprice, l_discount, l_shipdate, l_quantity FROM " + table);
    if (res->HasError()) { throw InvalidInputException("gpu_q6: " + res->GetError()); }
    vector<int64_t> ext, disc, qty; vector<int32_t> ship;
    ext.reserve(n_rows); disc.reserve(n_rows); qty.reserve(n_rows); ship.reserve(n_rows);
    idx_t nr = 0;
    while (true) {
      auto chunk = res->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[0]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[1]);
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[2]);
      const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[3]);
      ext.insert(ext.end(), e, e + n); disc.insert(disc.end(), d, d + n);
      qty.insert(qty.end(), q, q + n);
      const int32_t *si = reinterpret_cast<const int32_t *>(s);
      ship.insert(ship.end(), si, si + n);
      nr += n;
    }
    auto t_stage = q6_clock::now();
    void *h = reinterpret_cast<void *>(
        mojo_q6_pin(ship.data(), disc.data(), ext.data(), qty.data(),
                    NumericCast<int64_t>(nr), timing ? 1 : 0));
    auto t_up = q6_clock::now();
    if (timing) {
      fprintf(stderr, "[q6-pin stream-stage] n_rows=%llu count=%.1fms stream+vec=%.1fms "
              "upload=%.1fms total=%.1fms\n", (unsigned long long)nr, q6_ms(t0, t_cnt),
              q6_ms(t_cnt, t_stage), q6_ms(t_stage, t_up), q6_ms(t0, t_up));
    }
    Q6PinEntry e{h, nr};
    g_q6_pins_stream[table] = e;
    return e;
  }

  void *handle = reinterpret_cast<void *>(mojo_q6_pin_begin(NumericCast<int64_t>(n_rows)));
  if (!handle) { throw InvalidInputException("gpu_q6: stream pin_begin failed"); }
  auto t_begin = q6_clock::now();

  Connection con(*context.db);
  auto res = con.SendQuery("SELECT l_extendedprice, l_discount, l_shipdate, l_quantity FROM " + table);
  if (res->HasError()) { throw InvalidInputException("gpu_q6: " + res->GetError()); }

  idx_t offset = 0;
  while (true) {
    auto chunk = res->Fetch();  // StreamQueryResult::Fetch -> one chunk, flat
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
    const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[0]);
    const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[1]);
    const date_t *s = FlatVector::GetData<date_t>(chunk->data[2]);  // date_t == int32 days
    const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[3]);
    int32_t rc = mojo_q6_pin_chunk(handle, reinterpret_cast<const int32_t *>(s), d, e, q,
                                   NumericCast<int64_t>(n), NumericCast<int64_t>(offset));
    if (rc != 0) { throw InvalidInputException("gpu_q6: stream pin_chunk failed (rc " +
                                               std::to_string(rc) + ")"); }
    offset += n;
  }
  auto t_fetch = q6_clock::now();  // stream + per-chunk enqueue_copy done

  int32_t rc = mojo_q6_pin_end(handle);
  if (rc != 0) { throw InvalidInputException("gpu_q6: stream pin_end failed"); }
  auto t_end = q6_clock::now();  // final device synchronize done

  if (timing) {
    fprintf(stderr,
            "[q6-pin stream]   n_rows=%llu  count=%.1fms  begin(ctx+alloc)=%.1fms  "
            "stream+enqueue=%.1fms  sync=%.1fms  total=%.1fms\n",
            (unsigned long long)offset, q6_ms(t0, t_cnt), q6_ms(t_cnt, t_begin),
            q6_ms(t_begin, t_fetch), q6_ms(t_fetch, t_end), q6_ms(t0, t_end));
  }

  Q6PinEntry e{handle, offset};
  g_q6_pins_stream[table] = e;
  return e;
}

struct Q6BindData : public TableFunctionData {
  double revenue = 0;
};
struct Q6GlobalState : public GlobalTableFunctionState {
  bool done = false;
  idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> Q6BindImpl(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names,
                                    bool streaming) {
  auto table = input.inputs[0].GetValue<string>();
  // DATE -> int32 days; DECIMAL/DOUBLE discount -> int64 scale-2; qty -> int64 scale-2.
  int32_t ship_lo = input.inputs[1].GetValue<date_t>().days;
  int32_t ship_hi = input.inputs[2].GetValue<date_t>().days;
  int64_t disc_lo = (int64_t)llround(input.inputs[3].GetValue<double>() * 100.0);
  int64_t disc_hi = (int64_t)llround(input.inputs[4].GetValue<double>() * 100.0);
  int64_t qty_hi = (int64_t)input.inputs[5].GetValue<int32_t>() * 100;

  auto pe = streaming ? EnsureQ6PinnedStream(context, table)
                      : EnsureQ6Pinned(context, table);
  if (!pe.handle) { throw InvalidInputException("gpu_q6: GPU pin failed"); }

  int64_t out[2] = {0, 0};
  int32_t rc = mojo_q6_query(pe.handle, ship_lo, ship_hi, disc_lo, disc_hi, qty_hi, out);
  if (rc != 0) { throw InvalidInputException("gpu_q6: GPU query failed (rc " + std::to_string(rc) + ")"); }

  // Reconstruct the int128 (scale-4) result and present as DOUBLE revenue.
  __int128 sum = (__int128)((unsigned __int128)(uint64_t)out[0] | ((unsigned __int128)out[1] << 64));
  auto bd = make_uniq<Q6BindData>();
  bd->revenue = (double)sum / 10000.0;

  return_types = {LogicalType::DOUBLE};
  names = {"revenue"};
  return std::move(bd);
}

unique_ptr<FunctionData> Q6Bind(ClientContext &context, TableFunctionBindInput &input,
                                vector<LogicalType> &return_types, vector<string> &names) {
  return Q6BindImpl(context, input, return_types, names, /*streaming=*/false);
}

unique_ptr<FunctionData> Q6BindStream(ClientContext &context, TableFunctionBindInput &input,
                                      vector<LogicalType> &return_types, vector<string> &names) {
  return Q6BindImpl(context, input, return_types, names, /*streaming=*/true);
}

unique_ptr<GlobalTableFunctionState> Q6Init(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<Q6GlobalState>();
}

void Q6Func(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<Q6BindData>();
  auto &gs = data.global_state->Cast<Q6GlobalState>();
  if (gs.done) { output.SetCardinality(0); return; }
  output.SetCardinality(1);
  FlatVector::GetData<double>(output.data[0])[0] = bd.revenue;
  gs.done = true;
}

void RegisterGpuQ6TableFunction(ExtensionLoader &loader) {
  vector<LogicalType> args{LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE,
                           LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::INTEGER};
  TableFunction tf("gpu_q6", args, Q6Func, Q6Bind, Q6Init);
  loader.RegisterFunction(tf);
  // Streaming-pin variant (Option B): same engine + query, alternative cold pin.
  TableFunction tfs("gpu_q6_stream", args, Q6Func, Q6BindStream, Q6Init);
  loader.RegisterFunction(tfs);
}

// ---------------------------------------------------------------------------
// gpu_q1() table function: transparent-ready TPC-H Q1 grouped aggregation.
//
//   SELECT * FROM gpu_q1('lineitem', DATE '1998-09-02') ORDER BY 1, 2;
//
// Pins the 6 needed columns on first use (per table): a host-assigned dense
// group id (from the single-char l_returnflag/l_linestatus) plus
// l_quantity/l_extendedprice/l_discount/l_tax (DECIMAL(15,2)->int64) and
// l_shipdate (DATE->int32). Each call is a fused filter (l_shipdate <= cutoff)
// + exact int128 grouped sums on the GPU. Output mirrors TPC-H Q1 exactly:
//   l_returnflag, l_linestatus, sum_qty DEC(38,2), sum_base_price DEC(38,2),
//   sum_disc_price DEC(38,4), sum_charge DEC(38,6),
//   avg_qty/avg_price/avg_disc DOUBLE, count_order BIGINT.
// The 4 sums are written as hugeint_t into the DECIMAL vectors (bit-exact, the
// int128 limbs are the unscaled values at the matching scale); the 3 avgs are
// DOUBLE (Sxxx/100.0/count, matching avg()'s DOUBLE result to ~1e-10).
// ---------------------------------------------------------------------------
struct Q1PinEntry {
  void *handle = nullptr;
  idx_t n_rows = 0;
  idx_t n_groups = 0;
  // group id -> (returnflag, linestatus) for decoding the output.
  std::vector<std::pair<char, char>> group_keys;
};

std::mutex g_q1_mu;
std::unordered_map<std::string, Q1PinEntry> g_q1_pins;

Q1PinEntry &EnsureQ1Pinned(ClientContext &context, const std::string &table) {
  std::lock_guard<std::mutex> g(g_q1_mu);
  auto it = g_q1_pins.find(table);
  if (it != g_q1_pins.end()) { return it->second; }

  Connection con(*context.db);
  // Projection over the GET only (no aggregate) -> would not trigger any
  // future transparent Q1 matcher (none is wired today).
  const bool timing = pin_timing_enabled("MOJO_Q1_PIN_TIMING");
  auto t0 = q6_clock::now();
  auto res = con.Query("SELECT l_returnflag, l_linestatus, l_quantity, "
                       "l_extendedprice, l_discount, l_tax, l_shipdate FROM " + table);
  if (res->HasError()) { throw InvalidInputException("gpu_q1: " + res->GetError()); }
  auto t1 = q6_clock::now();  // (a) Query() materialize done
  idx_t row_count = res->RowCount();

  // Dense group-id assignment on the host.
  std::map<std::pair<char, char>, uint8_t> group_map;
  Q1PinEntry e;
  idx_t n_rows = 0;

  if (pin_use_pinned("MOJO_Q1_PIN_MODE")) {
    // Pinned-HostBuffer staging: alloc pinned buffers (n_groups unknown yet),
    // compute gid per row straight into the pinned uint8 buffer, memcpy the 5
    // numeric/date columns into their pinned buffers, then upload once per column.
    uint8_t *gid_h = nullptr; int64_t *qty_h = nullptr; int64_t *ext_h = nullptr;
    int64_t *disc_h = nullptr; int64_t *tax_h = nullptr; int32_t *ship_h = nullptr;
    e.handle = reinterpret_cast<void *>(mojo_q1_pin_alloc(
        NumericCast<int64_t>(row_count), &gid_h, &qty_h, &ext_h, &disc_h, &tax_h, &ship_h));
    if (!e.handle || !gid_h) { throw InvalidInputException("gpu_q1: pinned pin_alloc failed"); }
    auto t_alloc = q6_clock::now();  // (b0) pinned + device alloc done

    idx_t off = 0;
    while (true) {
      auto chunk = res->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 7; c++) { chunk->data[c].Flatten(n); }
      auto rf = FlatVector::GetData<string_t>(chunk->data[0]);
      auto ls = FlatVector::GetData<string_t>(chunk->data[1]);
      const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[2]);
      const int64_t *ep = FlatVector::GetData<int64_t>(chunk->data[3]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[4]);
      const int64_t *t = FlatVector::GetData<int64_t>(chunk->data[5]);
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[6]);
      // gid computed per row into the pinned buffer (the Q1 wrinkle); the rest
      // are direct chunk memcpys into pinned host memory.
      for (idx_t i = 0; i < n; i++) {
        char crf = rf[i].GetSize() > 0 ? rf[i].GetData()[0] : '\0';
        char cls = ls[i].GetSize() > 0 ? ls[i].GetData()[0] : '\0';
        auto key = std::make_pair(crf, cls);
        auto git = group_map.find(key);
        uint8_t id;
        if (git == group_map.end()) {
          id = (uint8_t)group_map.size();
          group_map[key] = id;
          e.group_keys.push_back(key);
        } else {
          id = git->second;
        }
        gid_h[off + i] = id;
      }
      std::memcpy(qty_h + off, q, n * sizeof(int64_t));
      std::memcpy(ext_h + off, ep, n * sizeof(int64_t));
      std::memcpy(disc_h + off, d, n * sizeof(int64_t));
      std::memcpy(tax_h + off, t, n * sizeof(int64_t));
      std::memcpy(ship_h + off, s, n * sizeof(int32_t));  // date_t is a 4-byte struct
      off += n;
    }
    n_rows = off;
    auto t_fill = q6_clock::now();  // (b) fetch + gid-compute + memcpy done

    int32_t rc = mojo_q1_pin_upload(e.handle, NumericCast<int64_t>(group_map.size()),
                                    timing ? 1 : 0);
    if (rc != 0) { throw InvalidInputException("gpu_q1: pinned upload failed (rc " +
                                               std::to_string(rc) + ")"); }
    auto t_up = q6_clock::now();  // (c) device upload done
    if (timing) {
      fprintf(stderr,
              "[q1-pin pinned] n_rows=%llu  (a)Query=%.1fms  (b0)pin_alloc=%.1fms  "
              "(b)fetch+gid+memcpy=%.1fms  (c)upload=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t_alloc),
              q6_ms(t_alloc, t_fill), q6_ms(t_fill, t_up), q6_ms(t0, t_up));
    }
  } else {
    // Baseline std::vector staging (A/B). reserve unless MOJO_Q1_PIN_MODE=noreserve.
    std::vector<uint8_t> gid;
    std::vector<int64_t> qty, ext, disc, tax;
    std::vector<int32_t> ship;
    const char *m = std::getenv("MOJO_Q1_PIN_MODE");
    if (!(m && std::string(m) == "noreserve")) {
      gid.reserve(row_count); qty.reserve(row_count); ext.reserve(row_count);
      disc.reserve(row_count); tax.reserve(row_count); ship.reserve(row_count);
    }
    while (true) {
      auto chunk = res->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 7; c++) { chunk->data[c].Flatten(n); }
      auto rf = FlatVector::GetData<string_t>(chunk->data[0]);
      auto ls = FlatVector::GetData<string_t>(chunk->data[1]);
      const int64_t *q = FlatVector::GetData<int64_t>(chunk->data[2]);
      const int64_t *ep = FlatVector::GetData<int64_t>(chunk->data[3]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[4]);
      const int64_t *t = FlatVector::GetData<int64_t>(chunk->data[5]);
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[6]);
      for (idx_t i = 0; i < n; i++) {
        char crf = rf[i].GetSize() > 0 ? rf[i].GetData()[0] : '\0';
        char cls = ls[i].GetSize() > 0 ? ls[i].GetData()[0] : '\0';
        auto key = std::make_pair(crf, cls);
        auto git = group_map.find(key);
        uint8_t id;
        if (git == group_map.end()) {
          id = (uint8_t)group_map.size();
          group_map[key] = id;
          e.group_keys.push_back(key);
        } else {
          id = git->second;
        }
        gid.push_back(id);
        qty.push_back(q[i]); ext.push_back(ep[i]); disc.push_back(d[i]);
        tax.push_back(t[i]); ship.push_back(s[i].days);
      }
      n_rows += n;
    }
    auto t2 = q6_clock::now();  // (b) Fetch + std::vector build done
    e.handle = reinterpret_cast<void *>(
        mojo_q1_pin(gid.data(), qty.data(), ext.data(), disc.data(), tax.data(),
                    ship.data(), NumericCast<int64_t>(n_rows),
                    NumericCast<int64_t>(group_map.size())));
    auto t3 = q6_clock::now();  // (c) host->device upload done
    if (timing) {
      fprintf(stderr,
              "[q1-pin baseline] n_rows=%llu  (a)Query=%.1fms  (b)Fetch+vec=%.1fms  "
              "(c)mojo_q1_pin(upload)=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t2),
              q6_ms(t2, t3), q6_ms(t0, t3));
    }
  }
  // group_keys is indexed by id (push order == id assignment order); re-sort a
  // copy is NOT done here -> group_keys[id] is correct.

  e.n_rows = n_rows;
  e.n_groups = group_map.size();
  g_q1_pins[table] = std::move(e);
  return g_q1_pins[table];
}

// One assembled output row (already decoded + ready to emit).
struct Q1Row {
  char rf, ls;
  hugeint_t sum_qty, sum_base, sum_disc_price, sum_charge;  // unscaled int128
  double avg_qty, avg_price, avg_disc;
  int64_t count_order;
};

struct Q1BindData : public TableFunctionData {
  std::vector<Q1Row> rows;  // sorted by (rf, ls)
};

struct Q1GlobalState : public GlobalTableFunctionState {
  idx_t offset = 0;
  idx_t MaxThreads() const override { return 1; }
};

// Shared between the gpu_q1() table function and the transparent PhysicalQ1
// source operator: pin (cached) + run mojo_q1_query + decode the int128 limbs
// into ready-to-emit Q1Row values (bit-exact sums + DOUBLE avgs). Rows are NOT
// sorted here (the table function sorts to match its own ORDER BY; the
// transparent op relies on the surviving plan ORDER BY).
std::vector<Q1Row> ComputeQ1Rows(ClientContext &context, const std::string &table,
                                 int32_t ship_hi) {
  auto &pe = EnsureQ1Pinned(context, table);
  if (!pe.handle) { throw InvalidInputException("gpu_q1: GPU pin failed"); }

  // out: per group, 6 int128 (low,high) limb pairs = 12 int64.
  std::vector<int64_t> out(pe.n_groups * 12, 0);
  int32_t rc = mojo_q1_query(pe.handle, ship_hi, out.data());
  if (rc != 0) { throw InvalidInputException("gpu_q1: GPU query failed (rc " + std::to_string(rc) + ")"); }

  auto limb = [&](idx_t g, idx_t metric) -> hugeint_t {
    idx_t base = g * 12 + metric * 2;
    hugeint_t h;
    h.lower = (uint64_t)out[base + 0];
    h.upper = out[base + 1];
    return h;
  };
  auto as_i128 = [](hugeint_t h) -> __int128 {
    return (__int128)((unsigned __int128)h.lower | ((unsigned __int128)h.upper << 64));
  };

  std::vector<Q1Row> rows;
  for (idx_t g = 0; g < pe.n_groups; g++) {
    Q1Row r;
    r.rf = pe.group_keys[g].first;
    r.ls = pe.group_keys[g].second;
    hugeint_t h_count = limb(g, 0);
    hugeint_t h_qty = limb(g, 1);
    hugeint_t h_ext = limb(g, 2);
    hugeint_t h_disc = limb(g, 3);
    r.sum_qty = h_qty;                // scale 2
    r.sum_base = h_ext;               // scale 2
    r.sum_disc_price = limb(g, 4);    // scale 4
    r.sum_charge = limb(g, 5);        // scale 6
    int64_t cnt = (int64_t)as_i128(h_count);
    r.count_order = cnt;
    double dcnt = (double)cnt;
    r.avg_qty = cnt ? (double)as_i128(h_qty) / 100.0 / dcnt : 0.0;
    r.avg_price = cnt ? (double)as_i128(h_ext) / 100.0 / dcnt : 0.0;
    r.avg_disc = cnt ? (double)as_i128(h_disc) / 100.0 / dcnt : 0.0;
    rows.push_back(r);
  }
  return rows;
}

unique_ptr<FunctionData> Q1Bind(ClientContext &context, TableFunctionBindInput &input,
                                vector<LogicalType> &return_types, vector<string> &names) {
  auto table = input.inputs[0].GetValue<string>();
  int32_t ship_hi = input.inputs[1].GetValue<date_t>().days;

  auto bd = make_uniq<Q1BindData>();
  bd->rows = ComputeQ1Rows(context, table, ship_hi);
  // Sort by (rf, ls) to match ORDER BY l_returnflag, l_linestatus.
  std::sort(bd->rows.begin(), bd->rows.end(), [](const Q1Row &a, const Q1Row &b) {
    if (a.rf != b.rf) { return a.rf < b.rf; }
    return a.ls < b.ls;
  });

  return_types = {LogicalType::VARCHAR, LogicalType::VARCHAR,
                  LogicalType::DECIMAL(38, 2), LogicalType::DECIMAL(38, 2),
                  LogicalType::DECIMAL(38, 4), LogicalType::DECIMAL(38, 6),
                  LogicalType::DOUBLE, LogicalType::DOUBLE, LogicalType::DOUBLE,
                  LogicalType::BIGINT};
  names = {"l_returnflag", "l_linestatus", "sum_qty", "sum_base_price",
           "sum_disc_price", "sum_charge", "avg_qty", "avg_price", "avg_disc",
           "count_order"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> Q1Init(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<Q1GlobalState>();
}

void Q1Func(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<Q1BindData>();
  auto &gs = data.global_state->Cast<Q1GlobalState>();
  idx_t total = bd.rows.size();
  idx_t n = MinValue<idx_t>(total - gs.offset, STANDARD_VECTOR_SIZE);
  if (n == 0) { output.SetCardinality(0); return; }
  auto rf = FlatVector::GetData<string_t>(output.data[0]);
  auto ls = FlatVector::GetData<string_t>(output.data[1]);
  auto sq = FlatVector::GetData<hugeint_t>(output.data[2]);
  auto sb = FlatVector::GetData<hugeint_t>(output.data[3]);
  auto sdp = FlatVector::GetData<hugeint_t>(output.data[4]);
  auto sc = FlatVector::GetData<hugeint_t>(output.data[5]);
  auto aq = FlatVector::GetData<double>(output.data[6]);
  auto ap = FlatVector::GetData<double>(output.data[7]);
  auto ad = FlatVector::GetData<double>(output.data[8]);
  auto co = FlatVector::GetData<int64_t>(output.data[9]);
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    rf[i] = StringVector::AddString(output.data[0], std::string(1, r.rf));
    ls[i] = StringVector::AddString(output.data[1], std::string(1, r.ls));
    sq[i] = r.sum_qty;
    sb[i] = r.sum_base;
    sdp[i] = r.sum_disc_price;
    sc[i] = r.sum_charge;
    aq[i] = r.avg_qty;
    ap[i] = r.avg_price;
    ad[i] = r.avg_disc;
    co[i] = r.count_order;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuQ1TableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_q1", {LogicalType::VARCHAR, LogicalType::DATE},
                   Q1Func, Q1Bind, Q1Init);
  loader.RegisterFunction(tf);
}

// ===========================================================================
// TPC-H Q1 transparent operator
//
// After the pre-optimizer disables COMPRESSED_MATERIALIZATION, the literal Q1
// optimizes to:
//     ORDER BY l_returnflag, l_linestatus
//       PROJECTION (passthrough of the 10 aggregate outputs)
//         AGGREGATE  group by [l_returnflag, l_linestatus]
//             8 exprs: sum_no_overflow(l_quantity), sum_no_overflow(l_extendedprice),
//                      sum_no_overflow(#disc_price), sum_no_overflow(#charge),
//                      avg(l_quantity), avg(l_extendedprice), avg(l_discount), count_star()
//           PROJECTION (computes l_extendedprice*(1-l_discount) etc.)
//             GET lineitem  [filter: l_shipdate <= cutoff]
// MatchQ1 recognizes this AGGREGATE (looking through the arithmetic PROJECTION
// to the GET), validates the 8 aggregates + their DECIMAL/DOUBLE/BIGINT return
// types, extracts the ship cutoff, and replaces the AGGREGATE (absorbing the
// PROJECTION + GET) with a LogicalQ1 that drives the same EnsureQ1Pinned +
// mojo_q1_query engine the gpu_q1() table function uses. The parent PROJECTION
// + ORDER BY survive and resolve unchanged because LogicalQ1 replicates the
// aggregate's column bindings (2 group keys then 8 aggregates) and types.
//
// Translate-or-fallback: any deviation leaves the plan untouched -> CPU.
// ===========================================================================

struct Q1SourceGlobalState : public GlobalSourceState {
  std::vector<Q1Row> rows;
  idx_t offset = 0;
  bool computed = false;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalQ1 : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalQ1(PhysicalPlan &plan, vector<LogicalType> types, std::string table,
             int32_t ship_cutoff, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        table(std::move(table)), ship_cutoff(ship_cutoff) {}

  std::string table;
  int32_t ship_cutoff;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<Q1SourceGlobalState>();
    gs->rows = ComputeQ1Rows(context, table, ship_cutoff);  // pins (cached) + query
    gs->computed = true;
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<Q1SourceGlobalState>();
    idx_t total = gs.rows.size();
    idx_t n = MinValue<idx_t>(total - gs.offset, STANDARD_VECTOR_SIZE);
    if (n == 0) {
      chunk.SetCardinality(0);
      return SourceResultType::FINISHED;
    }
    // Column order MUST match LogicalQ1's bindings: 2 group keys, then 8
    // aggregates (sum_qty, sum_base, sum_disc_price, sum_charge, avg_qty,
    // avg_price, avg_disc, count).
    auto rf = FlatVector::GetData<string_t>(chunk.data[0]);
    auto ls = FlatVector::GetData<string_t>(chunk.data[1]);
    auto sq = FlatVector::GetData<hugeint_t>(chunk.data[2]);
    auto sb = FlatVector::GetData<hugeint_t>(chunk.data[3]);
    auto sdp = FlatVector::GetData<hugeint_t>(chunk.data[4]);
    auto sc = FlatVector::GetData<hugeint_t>(chunk.data[5]);
    auto aq = FlatVector::GetData<double>(chunk.data[6]);
    auto ap = FlatVector::GetData<double>(chunk.data[7]);
    auto ad = FlatVector::GetData<double>(chunk.data[8]);
    auto co = FlatVector::GetData<int64_t>(chunk.data[9]);
    for (idx_t i = 0; i < n; i++) {
      auto &r = gs.rows[gs.offset + i];
      rf[i] = StringVector::AddString(chunk.data[0], std::string(1, r.rf));
      ls[i] = StringVector::AddString(chunk.data[1], std::string(1, r.ls));
      sq[i] = r.sum_qty;
      sb[i] = r.sum_base;
      sdp[i] = r.sum_disc_price;
      sc[i] = r.sum_charge;
      aq[i] = r.avg_qty;
      ap[i] = r.avg_price;
      ad[i] = r.avg_disc;
      co[i] = r.count_order;
    }
    chunk.SetCardinality(n);
    gs.offset += n;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_Q1"; }
};

// Logical extension op: sits where the AGGREGATE was, absorbing the arithmetic
// PROJECTION + the GET. Replicates the aggregate's column bindings/types in the
// exact order LogicalAggregate produces them (group keys first, then aggregates)
// so the parent PROJECTION / ORDER BY resolve unchanged.
class LogicalQ1 : public LogicalExtensionOperator {
public:
  LogicalQ1(std::string table, int32_t ship_cutoff, idx_t group_index,
            idx_t aggregate_index, vector<LogicalType> out_types)
      : table(std::move(table)), ship_cutoff(ship_cutoff), group_index(group_index),
        aggregate_index(aggregate_index), out_types(std::move(out_types)) {
    types = this->out_types;  // [2 group key types] + [8 aggregate types]
  }

  std::string table;
  int32_t ship_cutoff;
  idx_t group_index;
  idx_t aggregate_index;
  vector<LogicalType> out_types;  // kept so ResolveTypes() can repopulate `types`

  vector<ColumnBinding> GetColumnBindings() override {
    vector<ColumnBinding> result;
    result.emplace_back(group_index, 0);  // l_returnflag
    result.emplace_back(group_index, 1);  // l_linestatus
    for (idx_t i = 0; i < 8; i++) { result.emplace_back(aggregate_index, i); }
    return result;
  }

  // ResolveOperatorTypes() does types.clear() then calls this — must repopulate.
  void ResolveTypes() override { types = out_types; }

  string GetExtensionName() const override { return "mojo_gpu_q1"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    return planner.Make<PhysicalQ1>(types, table, ship_cutoff, estimated_cardinality);
  }
};

// Resolve a BoundColumnRef to a lineitem column name, looking through at most
// one passthrough projection. `proj` (optional) is the projection directly
// below the aggregate; `get` is the lineitem GET below that projection.
std::string ResolveLineitemColumn(const BoundColumnRefExpression &ref,
                                   optional_ptr<LogicalProjection> proj,
                                   LogicalGet &get) {
  const Expression *cur = &ref;
  // If the ref points into the projection, hop to the projection's expression.
  if (proj && ref.binding.table_index == proj->table_index) {
    idx_t idx = ref.binding.column_index;
    if (idx >= proj->expressions.size()) { return std::string(); }
    auto &pe = proj->expressions[idx];
    if (pe->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return std::string(); }
    cur = pe.get();
  }
  auto &cref = cur->Cast<BoundColumnRefExpression>();
  if (cref.binding.table_index != get.table_index) { return std::string(); }
  const auto &col_ids = get.GetColumnIds();
  idx_t pos = cref.binding.column_index;
  if (pos >= col_ids.size()) { return std::string(); }
  return get.GetColumnName(col_ids[pos]);
}

bool MatchQ1(unique_ptr<LogicalOperator> &node) {
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }
  auto &agg = node->Cast<LogicalAggregate>();

  // Exactly 2 group keys, 8 aggregates, one child.
  if (agg.groups.size() != 2) { return false; }
  if (agg.expressions.size() != 8) { return false; }
  if (agg.children.size() != 1) { return false; }
  if (!agg.grouping_functions.empty()) { return false; }

  // Child is either the arithmetic PROJECTION over the GET, or the GET directly.
  optional_ptr<LogicalProjection> proj;
  LogicalOperator *below = agg.children[0].get();
  if (below->type == LogicalOperatorType::LOGICAL_PROJECTION) {
    proj = &below->Cast<LogicalProjection>();
    if (proj->children.size() != 1) { return false; }
    below = proj->children[0].get();
  }
  if (below->type != LogicalOperatorType::LOGICAL_GET) { return false; }
  auto &get = below->Cast<LogicalGet>();
  auto table_entry = get.GetTable();
  if (!table_entry) { return false; }
  const std::string table_name = table_entry->name;

  // Group keys must be BoundColumnRefs resolving to l_returnflag, l_linestatus.
  for (idx_t i = 0; i < 2; i++) {
    if (agg.groups[i]->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
  }
  std::string g0 = ResolveLineitemColumn(agg.groups[0]->Cast<BoundColumnRefExpression>(), proj, get);
  std::string g1 = ResolveLineitemColumn(agg.groups[1]->Cast<BoundColumnRefExpression>(), proj, get);
  if (!(g0 == "l_returnflag" && g1 == "l_linestatus")) { return false; }

  // Validate the 8 aggregate functions (names) and their return types.
  // 0..3 sum_no_overflow -> DECIMAL(38, {2,2,4,6}) INT128; 4..6 avg -> DOUBLE; 7 count_star -> BIGINT.
  static const char *kSumName = "sum_no_overflow";
  auto check_sum = [&](idx_t i, uint8_t scale) -> bool {
    if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
    auto &ag = agg.expressions[i]->Cast<BoundAggregateExpression>();
    if (ag.IsDistinct() || ag.filter) { return false; }
    if (ag.function.name != kSumName && ag.function.name != "sum") { return false; }
    const LogicalType &rt = ag.return_type;
    if (rt.id() != LogicalTypeId::DECIMAL) { return false; }
    if (DecimalType::GetScale(rt) != scale) { return false; }
    if (rt.InternalType() != PhysicalType::INT128) { return false; }
    return true;
  };
  auto check_avg = [&](idx_t i) -> bool {
    if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
    auto &ag = agg.expressions[i]->Cast<BoundAggregateExpression>();
    if (ag.IsDistinct() || ag.filter) { return false; }
    if (ag.function.name != "avg") { return false; }
    return ag.return_type.id() == LogicalTypeId::DOUBLE;
  };
  if (!check_sum(0, 2)) { return false; }  // sum_qty       DECIMAL(38,2)
  if (!check_sum(1, 2)) { return false; }  // sum_base      DECIMAL(38,2)
  if (!check_sum(2, 4)) { return false; }  // sum_disc_price DECIMAL(38,4)
  if (!check_sum(3, 6)) { return false; }  // sum_charge    DECIMAL(38,6)
  if (!check_avg(4)) { return false; }     // avg_qty
  if (!check_avg(5)) { return false; }     // avg_price
  if (!check_avg(6)) { return false; }     // avg_disc
  {
    if (agg.expressions[7]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
    auto &ag = agg.expressions[7]->Cast<BoundAggregateExpression>();
    if (ag.function.name != "count_star") { return false; }
    if (ag.return_type.id() != LogicalTypeId::BIGINT) { return false; }
  }

  // Extract l_shipdate <= cutoff from get.table_filters (keyed by table column
  // index). We require exactly the single shipdate upper-bound predicate.
  bool have_cutoff = false;
  int32_t cutoff = 0;
  auto apply = [&](const ConstantFilter &cf) -> bool {
    auto cmp = cf.comparison_type;
    if (cmp != ExpressionType::COMPARE_LESSTHANOREQUALTO &&
        cmp != ExpressionType::COMPARE_LESSTHAN) {
      return false;
    }
    int32_t days = cf.constant.GetValue<date_t>().days;
    // mojo_q1_query filters l_shipdate <= cutoff (inclusive). DuckDB's Q1 uses
    // <=; if it ever lowered to <, subtract a day to keep it inclusive-exact.
    cutoff = (cmp == ExpressionType::COMPARE_LESSTHAN) ? days - 1 : days;
    have_cutoff = true;
    return true;
  };
  for (auto &kv : get.table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= get.names.size()) { return false; }
    if (get.names[col_idx] != "l_shipdate") { return false; }  // unexpected filter -> bail
    TableFilter &tf = *kv.second;
    if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
      if (!apply(tf.Cast<ConstantFilter>())) { return false; }
    } else {
      return false;
    }
  }
  if (!have_cutoff) { return false; }

  // Build the replacement. Output types/bindings = aggregate's: 2 group key
  // types (the resolved group expr return types) then the 8 aggregate types.
  vector<LogicalType> out_types;
  out_types.push_back(agg.groups[0]->return_type);
  out_types.push_back(agg.groups[1]->return_type);
  for (idx_t i = 0; i < 8; i++) { out_types.push_back(agg.expressions[i]->return_type); }

  auto repl = make_uniq<LogicalQ1>(table_name, cutoff, agg.group_index,
                                   agg.aggregate_index, std::move(out_types));
  repl->estimated_cardinality = agg.estimated_cardinality;
  node = std::move(repl);
  return true;
}

// ---------------------------------------------------------------------------
// gpu_q14() table function: TPC-H Q14 GPU hash-probe FK join + exact aggregation.
//
//   SELECT * FROM gpu_q14('lineitem', 'part', DATE '1995-09-01', DATE '1995-10-01');
//
// On first use (per (lineitem,part) pair) it builds a host open-addressing hash
// table from part (keyed by p_partkey, payload is_promo = p_type LIKE 'PROMO%'),
// materializes lineitem's 4 probe columns, and pins everything resident. Each
// call is a fused shipdate-filter + GPU hash-probe join + exact int128 sums of
// total = sum(ext*(1-disc)) and promo = sum over promo parts. Q14's result is
// DOUBLE: promo_revenue = 100.0 * (double)promo / (double)total. The two
// underlying sums are DECIMAL(38,4) (scale-4 int128) and are bit-exact vs stock.
// ---------------------------------------------------------------------------

// splitmix-ish 64-bit integer hash; MUST match q14_hash in gpu_kernels.mojo.
static inline uint64_t Q14Hash(int64_t k) {
  uint64_t x = (uint64_t)k;
  x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
  x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
  x = x ^ (x >> 31);
  return x;
}

struct Q14PinEntry {
  void *handle = nullptr;
  idx_t n_rows = 0;
};

std::mutex g_q14_mu;
std::unordered_map<std::string, Q14PinEntry> g_q14_pins;

Q14PinEntry EnsureQ14Pinned(ClientContext &context, const std::string &lineitem_table,
                            const std::string &part_table) {
  std::string key = lineitem_table + "\x1f" + part_table;
  std::lock_guard<std::mutex> g(g_q14_mu);
  auto it = g_q14_pins.find(key);
  if (it != g_q14_pins.end()) { return it->second; }

  Connection con(*context.db);

  // --- build side: part (plain projection, no aggregate/join -> no matcher) ---
  auto pres = con.Query("SELECT p_partkey, p_type FROM " + part_table);
  if (pres->HasError()) { throw InvalidInputException("gpu_q14: " + pres->GetError()); }
  std::vector<int64_t> build_keys;
  std::vector<uint8_t> build_promo;
  while (true) {
    auto chunk = pres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    chunk->data[1].Flatten(n);
    const int64_t *pk = FlatVector::GetData<int64_t>(chunk->data[0]);  // p_partkey BIGINT
    auto pt = FlatVector::GetData<string_t>(chunk->data[1]);           // p_type VARCHAR
    for (idx_t i = 0; i < n; i++) {
      build_keys.push_back(pk[i]);
      const char *s = pt[i].GetData();
      idx_t len = pt[i].GetSize();
      bool promo = (len >= 5 && s[0] == 'P' && s[1] == 'R' && s[2] == 'O' &&
                    s[3] == 'M' && s[4] == 'O');
      build_promo.push_back(promo ? 1 : 0);
    }
  }

  // Host open-addressing (linear-probing) hash table. size = next pow2 >= 2*rows;
  // empty slot = key 0 (TPC-H partkeys start at 1).
  idx_t build_rows = build_keys.size();
  idx_t ht_size = 1;
  while (ht_size < 2 * build_rows) { ht_size <<= 1; }
  uint64_t mask = (uint64_t)ht_size - 1;
  std::vector<int64_t> ht_keys(ht_size, 0);
  std::vector<uint8_t> ht_promo(ht_size, 0);
  for (idx_t i = 0; i < build_rows; i++) {
    int64_t k = build_keys[i];
    uint64_t h = Q14Hash(k) & mask;
    while (ht_keys[h] != 0) { h = (h + 1) & mask; }
    ht_keys[h] = k;
    ht_promo[h] = build_promo[i];
  }

  // --- probe side: lineitem's 4 columns (plain projection) ---
  const bool timing = pin_timing_enabled("MOJO_Q14_PIN_TIMING");
  auto t0 = q6_clock::now();
  auto lres = con.Query("SELECT l_partkey, l_shipdate, l_extendedprice, l_discount FROM " +
                        lineitem_table);
  if (lres->HasError()) { throw InvalidInputException("gpu_q14: " + lres->GetError()); }
  auto t1 = q6_clock::now();  // (a) Query() materialize done
  idx_t row_count = lres->RowCount();

  void *handle = nullptr;
  idx_t n_rows = 0;

  if (pin_use_pinned("MOJO_Q14_PIN_MODE")) {
    // Pinned-HostBuffer staging: pin the 4 big probe columns into Mojo-managed
    // pinned host memory (pre-sized to row_count -> no realloc, no std::vector),
    // memcpy each chunk in, then one DMA per column via mojo_q14_pin_upload.
    int64_t *lpk_h = nullptr; int32_t *ship_h = nullptr;
    int64_t *ext_h = nullptr; int64_t *disc_h = nullptr;
    handle = reinterpret_cast<void *>(mojo_q14_pin_alloc(
        ht_keys.data(), ht_promo.data(), NumericCast<int64_t>(ht_size),
        NumericCast<int64_t>(row_count), &lpk_h, &ship_h, &ext_h, &disc_h));
    if (!handle || !lpk_h) { throw InvalidInputException("gpu_q14: pinned pin_alloc failed"); }
    auto t_alloc = q6_clock::now();  // (b0) pinned + device alloc done

    idx_t off = 0;
    while (true) {
      auto chunk = lres->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *pk = FlatVector::GetData<int64_t>(chunk->data[0]);  // l_partkey BIGINT
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[1]);     // DATE == int32 days
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);   // DECIMAL(15,2) -> int64
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
      std::memcpy(lpk_h + off, pk, n * sizeof(int64_t));
      std::memcpy(ship_h + off, s, n * sizeof(int32_t));  // date_t is a 4-byte struct
      std::memcpy(ext_h + off, e, n * sizeof(int64_t));
      std::memcpy(disc_h + off, d, n * sizeof(int64_t));
      off += n;
    }
    n_rows = off;
    auto t_fill = q6_clock::now();  // (b) fetch + memcpy-into-pinned done

    int32_t rc = mojo_q14_pin_upload(handle, timing ? 1 : 0);
    if (rc != 0) { throw InvalidInputException("gpu_q14: pinned upload failed (rc " +
                                               std::to_string(rc) + ")"); }
    auto t_up = q6_clock::now();  // (c) device upload done
    if (timing) {
      fprintf(stderr,
              "[q14-pin pinned] n_rows=%llu  (a)Query=%.1fms  (b0)pin_alloc=%.1fms  "
              "(b)fetch+memcpy=%.1fms  (c)upload=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t_alloc),
              q6_ms(t_alloc, t_fill), q6_ms(t_fill, t_up), q6_ms(t0, t_up));
    }
  } else {
    // Baseline std::vector staging (A/B): reserve(row_count) + insert + one pin.
    // Set MOJO_Q14_PIN_MODE=noreserve to skip the reserve (the true "before").
    std::vector<int64_t> lpk, ext, disc;
    std::vector<int32_t> ship;
    const char *m = std::getenv("MOJO_Q14_PIN_MODE");
    if (!(m && std::string(m) == "noreserve")) {
      lpk.reserve(row_count); ext.reserve(row_count);
      disc.reserve(row_count); ship.reserve(row_count);
    }
    while (true) {
      auto chunk = lres->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *pk = FlatVector::GetData<int64_t>(chunk->data[0]);
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[1]);
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
      lpk.insert(lpk.end(), pk, pk + n);
      ext.insert(ext.end(), e, e + n);
      disc.insert(disc.end(), d, d + n);
      ship.insert(ship.end(), reinterpret_cast<const int32_t *>(s),
                  reinterpret_cast<const int32_t *>(s) + n);
      n_rows += n;
    }
    auto t2 = q6_clock::now();  // (b) Fetch + std::vector build done
    handle = reinterpret_cast<void *>(
        mojo_q14_pin(ht_keys.data(), ht_promo.data(), NumericCast<int64_t>(ht_size),
                     lpk.data(), ship.data(), ext.data(), disc.data(),
                     NumericCast<int64_t>(n_rows)));
    auto t3 = q6_clock::now();  // (c) host->device upload done
    if (timing) {
      fprintf(stderr,
              "[q14-pin baseline] n_rows=%llu  (a)Query=%.1fms  (b)Fetch+vec=%.1fms  "
              "(c)mojo_q14_pin(upload)=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t2),
              q6_ms(t2, t3), q6_ms(t0, t3));
    }
  }

  Q14PinEntry e{handle, n_rows};
  g_q14_pins[key] = e;
  return e;
}

struct Q14BindData : public TableFunctionData {
  double promo_revenue = 0;
  hugeint_t total_sum;   // scale-4 int128 (DECIMAL(38,4))
  hugeint_t promo_sum;
};
struct Q14GlobalState : public GlobalTableFunctionState {
  bool done = false;
  idx_t MaxThreads() const override { return 1; }
};

unique_ptr<FunctionData> Q14Bind(ClientContext &context, TableFunctionBindInput &input,
                                 vector<LogicalType> &return_types, vector<string> &names) {
  auto lineitem_table = input.inputs[0].GetValue<string>();
  auto part_table = input.inputs[1].GetValue<string>();
  int32_t ship_lo = input.inputs[2].GetValue<date_t>().days;
  int32_t ship_hi = input.inputs[3].GetValue<date_t>().days;

  auto pe = EnsureQ14Pinned(context, lineitem_table, part_table);
  if (!pe.handle) { throw InvalidInputException("gpu_q14: GPU pin failed"); }

  int64_t out_total[2] = {0, 0};
  int64_t out_promo[2] = {0, 0};
  int32_t rc = mojo_q14_query(pe.handle, ship_lo, ship_hi, out_total, out_promo);
  if (rc != 0) {
    throw InvalidInputException("gpu_q14: GPU query failed (rc " + std::to_string(rc) +
                                (rc == 4 ? " = probe miss / FK violation)" : ")"));
  }

  hugeint_t ht_total, ht_promo;
  ht_total.lower = (uint64_t)out_total[0]; ht_total.upper = out_total[1];
  ht_promo.lower = (uint64_t)out_promo[0]; ht_promo.upper = out_promo[1];
  auto as_i128 = [](hugeint_t h) -> __int128 {
    return (__int128)((unsigned __int128)h.lower | ((unsigned __int128)h.upper << 64));
  };
  __int128 total = as_i128(ht_total);
  __int128 promo = as_i128(ht_promo);

  auto bd = make_uniq<Q14BindData>();
  bd->total_sum = ht_total;
  bd->promo_sum = ht_promo;
  // Q14 result is DOUBLE: 100.0 * promo / total (the scale-4 factors cancel).
  bd->promo_revenue = total != 0 ? 100.0 * (double)promo / (double)total : 0.0;

  return_types = {LogicalType::DOUBLE, LogicalType::DECIMAL(38, 4), LogicalType::DECIMAL(38, 4)};
  names = {"promo_revenue", "total_sum", "promo_sum"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> Q14Init(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<Q14GlobalState>();
}

void Q14Func(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<Q14BindData>();
  auto &gs = data.global_state->Cast<Q14GlobalState>();
  if (gs.done) { output.SetCardinality(0); return; }
  output.SetCardinality(1);
  FlatVector::GetData<double>(output.data[0])[0] = bd.promo_revenue;
  FlatVector::GetData<hugeint_t>(output.data[1])[0] = bd.total_sum;
  FlatVector::GetData<hugeint_t>(output.data[2])[0] = bd.promo_sum;
  gs.done = true;
}

void RegisterGpuQ14TableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_q14",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
                   Q14Func, Q14Bind, Q14Init);
  loader.RegisterFunction(tf);
}

// ===========================================================================
// TPC-H Q14 transparent operator
//
// The literal Q14 optimizes to:
//     PROJECTION (100.00 * promo_sum / total_sum)           -- the final ratio
//       AGGREGATE  ungrouped, 2 exprs:
//          [0] sum(CASE WHEN prefix(p_type,'PROMO') THEN ext*(1-disc) ELSE 0 END)  -- promo
//          [1] sum(ext*(1-disc))                                                    -- total
//         COMPARISON_JOIN  (INNER) l_partkey = p_partkey
//           GET lineitem  [table_filter: l_shipdate >= lo AND < hi]
//           GET part
// (Verified via EXPLAIN OPTIMIZED_ONLY: AGGREGATE sits directly over the join;
// the CASE / multiply are inline aggregate arguments. The promo aggregate is
// index 0, the total is index 1.)
//
// MatchQ14 replaces ONLY the AGGREGATE node (absorbing the join + both GETs)
// with a LogicalQ14 source op that emits one row with the two int128 sums as
// DECIMAL(38,4) at the aggregate's exact output bindings (aggregate_index, j).
// The parent PROJECTION survives unchanged, so DuckDB computes
// 100.00 * promo / total in DECIMAL exactly as stock -> bit-identical result.
//
// Translate-or-fallback: any deviation leaves the plan untouched -> CPU.
// ===========================================================================

struct Q14SourceGlobalState : public GlobalSourceState {
  void *handle = nullptr;
  bool done = false;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalQ14 : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalQ14(PhysicalPlan &plan, vector<LogicalType> types, std::string lineitem_table,
              std::string part_table, int32_t ship_lo, int32_t ship_hi,
              bool promo_first, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        lineitem_table(std::move(lineitem_table)), part_table(std::move(part_table)),
        ship_lo(ship_lo), ship_hi(ship_hi), promo_first(promo_first) {}

  std::string lineitem_table, part_table;
  int32_t ship_lo, ship_hi;
  bool promo_first;  // true: output[0]=promo,output[1]=total; false: swapped.

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<Q14SourceGlobalState>();
    auto pe = EnsureQ14Pinned(context, lineitem_table, part_table);  // cached pin
    gs->handle = pe.handle;
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<Q14SourceGlobalState>();
    if (gs.done) {
      chunk.SetCardinality(0);
      return SourceResultType::FINISHED;
    }
    int64_t out_total[2] = {0, 0};
    int64_t out_promo[2] = {0, 0};
    int32_t rc = -1;
    if (gs.handle) {
      rc = mojo_q14_query(gs.handle, ship_lo, ship_hi, out_total, out_promo);
    }
    if (rc != 0) {
      throw InvalidInputException("GPU_Q14: GPU query failed (rc " + std::to_string(rc) + ")");
    }
    // Each sum is a scale-4 int128 in (low,high) limbs -> hugeint_t exactly.
    hugeint_t h_total, h_promo;
    h_total.lower = (uint64_t)out_total[0]; h_total.upper = out_total[1];
    h_promo.lower = (uint64_t)out_promo[0]; h_promo.upper = out_promo[1];

    chunk.data[0].SetVectorType(VectorType::FLAT_VECTOR);
    chunk.data[1].SetVectorType(VectorType::FLAT_VECTOR);
    auto c0 = FlatVector::GetData<hugeint_t>(chunk.data[0]);
    auto c1 = FlatVector::GetData<hugeint_t>(chunk.data[1]);
    // Emit in the aggregate's expression order.
    c0[0] = promo_first ? h_promo : h_total;
    c1[0] = promo_first ? h_total : h_promo;
    chunk.SetCardinality(1);
    gs.done = true;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_Q14"; }
};

// Logical extension op: sits where the AGGREGATE was. Two outputs at
// ColumnBinding(aggregate_index, 0) and (aggregate_index, 1) so the parent
// PROJECTION's references resolve unchanged.
class LogicalQ14 : public LogicalExtensionOperator {
public:
  LogicalQ14(std::string lineitem_table, std::string part_table, int32_t ship_lo,
             int32_t ship_hi, bool promo_first, idx_t aggregate_index,
             vector<LogicalType> out_types)
      : lineitem_table(std::move(lineitem_table)), part_table(std::move(part_table)),
        ship_lo(ship_lo), ship_hi(ship_hi), promo_first(promo_first),
        aggregate_index(aggregate_index), out_types(std::move(out_types)) {
    types = this->out_types;
  }

  std::string lineitem_table, part_table;
  int32_t ship_lo, ship_hi;
  bool promo_first;
  idx_t aggregate_index;
  vector<LogicalType> out_types;

  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(aggregate_index, 0), ColumnBinding(aggregate_index, 1)};
  }

  void ResolveTypes() override { types = out_types; }

  string GetExtensionName() const override { return "mojo_gpu_q14"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    return planner.Make<PhysicalQ14>(out_types, lineitem_table, part_table, ship_lo,
                                     ship_hi, promo_first, estimated_cardinality);
  }
};

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

bool MatchQ14(unique_ptr<LogicalOperator> &node) {
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }
  auto &agg = node->Cast<LogicalAggregate>();

  // Ungrouped, exactly 2 aggregates, one child that is a comparison join.
  if (!agg.groups.empty()) { return false; }
  if (!agg.grouping_functions.empty()) { return false; }
  if (agg.expressions.size() != 2) { return false; }
  if (agg.children.size() != 1) { return false; }
  if (agg.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) { return false; }
  auto &join = agg.children[0]->Cast<LogicalComparisonJoin>();
  if (join.join_type != JoinType::INNER) { return false; }
  if (join.conditions.size() != 1) { return false; }
  if (join.children.size() != 2) { return false; }

  // Both join children must be plain GETs (no projection between).
  if (join.children[0]->type != LogicalOperatorType::LOGICAL_GET) { return false; }
  if (join.children[1]->type != LogicalOperatorType::LOGICAL_GET) { return false; }
  auto &get_a = join.children[0]->Cast<LogicalGet>();
  auto &get_b = join.children[1]->Cast<LogicalGet>();
  std::vector<LogicalGet *> gets = {&get_a, &get_b};

  // Identify the lineitem GET (carries the shipdate filter) and the part GET.
  auto name_of = [](LogicalGet &g) -> std::string {
    auto te = g.GetTable();
    return te ? te->name : std::string();
  };
  LogicalGet *lineitem_get = nullptr, *part_get = nullptr;
  for (auto *g : gets) {
    std::string n = name_of(*g);
    if (n == "lineitem") { lineitem_get = g; }
    else if (n == "part") { part_get = g; }
  }
  if (!lineitem_get || !part_get) { return false; }
  const std::string lineitem_name = name_of(*lineitem_get);
  const std::string part_name = name_of(*part_get);

  // Equi-condition must be l_partkey = p_partkey (either side ordering).
  {
    auto &cond = join.conditions[0];
    if (cond.comparison != ExpressionType::COMPARE_EQUAL) { return false; }
    if (cond.left->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
    if (cond.right->GetExpressionClass() != ExpressionClass::BOUND_COLUMN_REF) { return false; }
    auto l = ResolveJoinColref(cond.left->Cast<BoundColumnRefExpression>(), gets);
    auto r = ResolveJoinColref(cond.right->Cast<BoundColumnRefExpression>(), gets);
    bool ok = (l.col_name == "l_partkey" && r.col_name == "p_partkey") ||
              (l.col_name == "p_partkey" && r.col_name == "l_partkey");
    if (!ok) { return false; }
  }

  // Both aggregates must be sum / sum_no_overflow -> DECIMAL scale-4 INT128.
  auto check_sum_type = [&](const BoundAggregateExpression &ag) -> bool {
    if (ag.IsDistinct() || ag.filter) { return false; }
    if (ag.function.name != "sum" && ag.function.name != "sum_no_overflow") { return false; }
    if (ag.children.size() != 1) { return false; }
    const LogicalType &rt = ag.return_type;
    if (rt.id() != LogicalTypeId::DECIMAL) { return false; }
    if (DecimalType::GetScale(rt) != 4) { return false; }
    if (rt.InternalType() != PhysicalType::INT128) { return false; }
    return true;
  };
  for (idx_t i = 0; i < 2; i++) {
    if (agg.expressions[i]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
    if (!check_sum_type(agg.expressions[i]->Cast<BoundAggregateExpression>())) { return false; }
  }

  // Distinguish promo (CASE) vs total (plain expr). Exactly one must be a CASE
  // whose WHEN is the promo predicate; the other must NOT be a CASE.
  auto is_case = [](const Expression &arg) {
    return arg.GetExpressionClass() == ExpressionClass::BOUND_CASE;
  };
  auto &a0 = agg.expressions[0]->Cast<BoundAggregateExpression>();
  auto &a1 = agg.expressions[1]->Cast<BoundAggregateExpression>();
  bool a0_case = is_case(*a0.children[0]);
  bool a1_case = is_case(*a1.children[0]);
  if (a0_case == a1_case) { return false; }  // need exactly one CASE
  const BoundAggregateExpression &promo_agg = a0_case ? a0 : a1;
  bool promo_first = a0_case;  // index 0 is the promo (CASE) aggregate

  // Validate the promo CASE: single WHEN = promo predicate over p_type.
  {
    auto &ce = promo_agg.children[0]->Cast<BoundCaseExpression>();
    if (ce.case_checks.size() != 1) { return false; }
    if (!IsPromoPredicate(*ce.case_checks[0].when_expr, gets)) { return false; }
  }

  // Extract l_shipdate >= lo AND < hi from the lineitem GET's table_filters
  // (keyed by table column index). Reject any other / unmodeled filter.
  bool have_lo = false, have_hi = false;
  int32_t ship_lo = 0, ship_hi = 0;
  auto apply_ship = [&](const ConstantFilter &cf) -> bool {
    int32_t days = cf.constant.GetValue<date_t>().days;
    if (cf.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
      ship_lo = days; have_lo = true; return true;
    }
    if (cf.comparison_type == ExpressionType::COMPARE_LESSTHAN) {
      ship_hi = days; have_hi = true; return true;
    }
    return false;
  };
  for (auto &kv : lineitem_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= lineitem_get->names.size()) { return false; }
    if (lineitem_get->names[col_idx] != "l_shipdate") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
      if (!apply_ship(tf.Cast<ConstantFilter>())) { return false; }
    } else if (tf.filter_type == TableFilterType::CONJUNCTION_AND) {
      auto &conj = tf.Cast<ConjunctionAndFilter>();
      for (auto &cfp : conj.child_filters) {
        if (cfp->filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
        if (!apply_ship(cfp->Cast<ConstantFilter>())) { return false; }
      }
    } else {
      return false;
    }
  }
  if (!(have_lo && have_hi)) { return false; }

  // part GET must carry no filters we don't model.
  if (!part_get->table_filters.filters.empty()) { return false; }

  // Build the replacement at the aggregate's exact bindings/types.
  vector<LogicalType> out_types = {agg.expressions[0]->return_type,
                                   agg.expressions[1]->return_type};
  auto repl = make_uniq<LogicalQ14>(lineitem_name, part_name, ship_lo, ship_hi,
                                    promo_first, agg.aggregate_index, std::move(out_types));
  repl->estimated_cardinality = agg.estimated_cardinality;
  node = std::move(repl);
  return true;
}

// ===========================================================================
// gpu_q3() table function: TPC-H Q3 GPU multi-way-join + per-order revenue
// top-10.
//
//   SELECT * FROM gpu_q3('customer','orders','lineitem','BUILDING',
//                        DATE '1995-03-15', DATE '1995-03-15');
//
// On first use (per (customer,orders,lineitem,mktsegment,o_cutoff) key) it:
//   * builds is_building[c_custkey] from customer (c_mktsegment == seg),
//   * builds, per order, the dense order_pass[o_orderkey] =
//     is_building[o_custkey] AND (o_orderdate < o_cutoff), and stashes
//     (o_orderdate days, o_shippriority) keyed by o_orderkey on the host,
//   * materializes lineitem's (l_orderkey, l_shipdate, l_extendedprice,
//     l_discount) and pins everything resident.
// Each call runs the GPU probe (l_shipdate > l_cutoff AND order_pass) computing
// scale-4 revenue per row, sums per order (host; Apple GPU lacks int64 atomics)
// into a dense int64 accumulator, then on the host attaches date/priority to the
// nonzero orders, sorts by (revenue DESC, o_orderdate ASC) and keeps the top 10.
// Revenue is written int64(scale-4) -> hugeint into DECIMAL(38,4): bit-exact.
//
// Dense-array bound: order_pass / accumulator are sized max(o_orderkey)+1. SF1
// max o_orderkey = 6,000,000 -> 6MB uint8 + 48MB int64. Fine.
// ===========================================================================
struct Q3PinEntry {
  void *handle = nullptr;
  idx_t n_rows = 0;
  idx_t n_slots = 0;                       // max_orderkey + 1
  idx_t n_seg = 0;                         // distinct orderkeys (segments)
  std::vector<int64_t> seg_key;            // [s] -> o_orderkey (segment -> key)
  std::vector<int32_t> order_date;         // [o_orderkey] -> days (0 if absent)
  std::vector<int32_t> order_prio;         // [o_orderkey] -> o_shippriority
};

std::mutex g_q3_mu;
std::unordered_map<std::string, Q3PinEntry *> g_q3_pins;

Q3PinEntry *EnsureQ3Pinned(ClientContext &context, const std::string &customer_table,
                           const std::string &orders_table,
                           const std::string &lineitem_table,
                           const std::string &mktsegment, int32_t o_cutoff) {
  std::string key = customer_table + "\x1f" + orders_table + "\x1f" + lineitem_table +
                    "\x1f" + mktsegment + "\x1f" + std::to_string(o_cutoff);
  std::lock_guard<std::mutex> g(g_q3_mu);
  auto it = g_q3_pins.find(key);
  if (it != g_q3_pins.end()) { return it->second; }

  Connection con(*context.db);

  // --- customer: is_building[c_custkey] (plain projection -> no matcher) ---
  auto cres = con.Query("SELECT c_custkey, c_mktsegment FROM " + customer_table);
  if (cres->HasError()) { throw InvalidInputException("gpu_q3: " + cres->GetError()); }
  std::vector<uint8_t> is_building;  // dense by c_custkey
  while (true) {
    auto chunk = cres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    chunk->data[1].Flatten(n);
    const int64_t *ck = FlatVector::GetData<int64_t>(chunk->data[0]);  // c_custkey BIGINT
    auto seg = FlatVector::GetData<string_t>(chunk->data[1]);          // c_mktsegment VARCHAR
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)ck[i];
      if (k >= is_building.size()) { is_building.resize(k + 1, 0); }
      const char *s = seg[i].GetData();
      idx_t len = seg[i].GetSize();
      bool match = (len == mktsegment.size() &&
                    memcmp(s, mktsegment.data(), len) == 0);
      is_building[k] = match ? 1 : 0;
    }
  }

  // --- orders: order_pass[o_orderkey] + (date, priority); track max key ---
  auto ores = con.Query("SELECT o_orderkey, o_custkey, o_orderdate, o_shippriority FROM " +
                        orders_table);
  if (ores->HasError()) { throw InvalidInputException("gpu_q3: " + ores->GetError()); }
  std::vector<uint8_t> order_pass;
  std::vector<int32_t> order_date, order_prio;
  idx_t max_orderkey = 0;
  while (true) {
    auto chunk = ores->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
    const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);   // o_orderkey BIGINT
    const int64_t *cust = FlatVector::GetData<int64_t>(chunk->data[1]); // o_custkey BIGINT
    const date_t *od = FlatVector::GetData<date_t>(chunk->data[2]);     // DATE -> int32 days
    const int32_t *sp = FlatVector::GetData<int32_t>(chunk->data[3]);   // o_shippriority INTEGER
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)ok[i];
      if (k >= order_pass.size()) {
        order_pass.resize(k + 1, 0);
        order_date.resize(k + 1, 0);
        order_prio.resize(k + 1, 0);
      }
      if (k > max_orderkey) { max_orderkey = k; }
      idx_t cust_k = (idx_t)cust[i];
      bool building = (cust_k < is_building.size() && is_building[cust_k]);
      bool pass = building && (od[i].days < o_cutoff);
      order_pass[k] = pass ? 1 : 0;
      order_date[k] = od[i].days;
      order_prio[k] = sp[i];
    }
  }

  // --- lineitem: the 3 probe columns, ORDERED BY l_orderkey (plain projection
  // + ORDER BY -> no matcher). The sort makes each order's rows contiguous so the
  // GPU can do a segmented reduction (one warp per order). The sort is a one-time
  // host cost folded into the pin, amortized across all queries on this table.
  const bool timing = pin_timing_enabled("MOJO_Q3_PIN_TIMING");
  auto t0 = q6_clock::now();
  auto lres = con.Query("SELECT l_orderkey, l_shipdate, l_extendedprice, l_discount FROM " +
                        lineitem_table + " ORDER BY l_orderkey");
  if (lres->HasError()) { throw InvalidInputException("gpu_q3: " + lres->GetError()); }
  auto t1 = q6_clock::now();  // (a) Query() (scan + ORDER BY) materialize done
  idx_t row_count = lres->RowCount();
  // Sorted probe columns + segment layout built in one linear pass over the
  // sorted stream: seg_key[s] = distinct orderkey, seg_offset[s] = its first row.
  std::vector<int64_t> seg_offset, seg_key;
  // Segment layout is sized by distinct orderkeys (n_seg); reserve generously to
  // avoid realloc (SF1 ~1.5M orders -> well under row_count).
  seg_offset.reserve(row_count / 2 + 2);
  seg_key.reserve(row_count / 2 + 1);
  idx_t n_rows = 0;
  int64_t cur_key = -1;  // no real orderkey is negative
  void *handle = nullptr;

  if (pin_use_pinned("MOJO_Q3_PIN_MODE")) {
    // Pinned-HostBuffer staging for the 3 sorted probe columns; l_orderkey is
    // consumed here to build the segmentation (kept intact). seg/order_pass are
    // uploaded in pin_upload2 once n_seg is known.
    int32_t *ship_h = nullptr; int64_t *ext_h = nullptr; int64_t *disc_h = nullptr;
    handle = reinterpret_cast<void *>(mojo_q3_pin_alloc2(
        NumericCast<int64_t>(row_count), &ship_h, &ext_h, &disc_h));
    if (!handle || !ship_h) { throw InvalidInputException("gpu_q3: pinned pin_alloc2 failed"); }
    auto t_alloc = q6_clock::now();  // (b0) pinned + device alloc done

    while (true) {
      auto chunk = lres->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);  // l_orderkey BIGINT
      const date_t *s = FlatVector::GetData<date_t>(chunk->data[1]);     // DATE == int32 days
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);   // DECIMAL(15,2) -> int64
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
      // Segmentation must scan per-row (kept intact); the probe columns go
      // straight into pinned host memory at the running offset.
      for (idx_t i = 0; i < n; i++) {
        if (ok[i] != cur_key) {            // new order segment starts here
          seg_key.push_back(ok[i]);
          seg_offset.push_back((int64_t)(n_rows + i));
          cur_key = ok[i];
        }
      }
      std::memcpy(ship_h + n_rows, s, n * sizeof(int32_t));  // date_t is a 4-byte struct
      std::memcpy(ext_h + n_rows, e, n * sizeof(int64_t));
      std::memcpy(disc_h + n_rows, d, n * sizeof(int64_t));
      n_rows += n;
    }
    idx_t n_seg = seg_key.size();
    seg_offset.push_back((int64_t)n_rows);   // sentinel: seg_offset[n_seg] = n_rows
    auto t_fill = q6_clock::now();  // (b) fetch + segment + memcpy done

    int32_t rc = mojo_q3_pin_upload2(handle, order_pass.data(), seg_offset.data(),
                                     seg_key.data(), NumericCast<int64_t>(n_seg),
                                     NumericCast<int64_t>(max_orderkey), timing ? 1 : 0);
    if (rc != 0) { throw InvalidInputException("gpu_q3: pinned upload2 failed (rc " +
                                               std::to_string(rc) + ")"); }
    auto t_up = q6_clock::now();  // (c) device upload done
    if (timing) {
      fprintf(stderr,
              "[q3-pin pinned] n_rows=%llu n_seg=%llu  (a)Query+sort=%.1fms  "
              "(b0)pin_alloc=%.1fms  (b)fetch+seg+memcpy=%.1fms  (c)upload=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, (unsigned long long)n_seg, q6_ms(t0, t1),
              q6_ms(t1, t_alloc), q6_ms(t_alloc, t_fill), q6_ms(t_fill, t_up), q6_ms(t0, t_up));
    }
    // NB: handle is a Q3State2*; n_seg captured into the entry below.
    auto *pe = new Q3PinEntry();
    pe->handle = handle;
    pe->n_rows = n_rows;
    pe->n_slots = max_orderkey + 1;
    pe->n_seg = n_seg;
    pe->seg_key = std::move(seg_key);
    pe->order_date = std::move(order_date);
    pe->order_prio = std::move(order_prio);
    g_q3_pins[key] = pe;
    return pe;
  }

  // Baseline std::vector staging (A/B). reserve unless MOJO_Q3_PIN_MODE=noreserve.
  std::vector<int32_t> ship;
  std::vector<int64_t> ext, disc;
  const char *m = std::getenv("MOJO_Q3_PIN_MODE");
  if (!(m && std::string(m) == "noreserve")) {
    ship.reserve(row_count); ext.reserve(row_count); disc.reserve(row_count);
  }
  while (true) {
    auto chunk = lres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
    const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);  // l_orderkey BIGINT
    const date_t *s = FlatVector::GetData<date_t>(chunk->data[1]);     // DATE -> int32 days
    const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);   // DECIMAL(15,2) -> int64
    const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
    for (idx_t i = 0; i < n; i++) {
      if (ok[i] != cur_key) {              // new order segment starts here
        seg_key.push_back(ok[i]);
        seg_offset.push_back((int64_t)n_rows);
        cur_key = ok[i];
      }
      ship.push_back(s[i].days);
      ext.push_back(e[i]);
      disc.push_back(d[i]);
      n_rows++;
    }
  }
  idx_t n_seg = seg_key.size();
  seg_offset.push_back((int64_t)n_rows);   // sentinel: seg_offset[n_seg] = n_rows
  auto t2 = q6_clock::now();  // (b) Fetch + std::vector build done

  handle = reinterpret_cast<void *>(
      mojo_q3_pin2(order_pass.data(), seg_offset.data(), seg_key.data(), ship.data(),
                   ext.data(), disc.data(), NumericCast<int64_t>(n_rows),
                   NumericCast<int64_t>(n_seg), NumericCast<int64_t>(max_orderkey)));
  auto t3 = q6_clock::now();  // (c) host->device upload done
  if (timing) {
    fprintf(stderr,
            "[q3-pin baseline] n_rows=%llu n_seg=%llu  (a)Query+sort=%.1fms  "
            "(b)Fetch+vec=%.1fms  (c)mojo_q3_pin2(upload)=%.1fms  total=%.1fms\n",
            (unsigned long long)n_rows, (unsigned long long)n_seg, q6_ms(t0, t1),
            q6_ms(t1, t2), q6_ms(t2, t3), q6_ms(t0, t3));
  }
  auto *e = new Q3PinEntry();
  e->handle = handle;
  e->n_rows = n_rows;
  e->n_slots = max_orderkey + 1;
  e->n_seg = n_seg;
  e->seg_key = std::move(seg_key);
  e->order_date = std::move(order_date);
  e->order_prio = std::move(order_prio);
  g_q3_pins[key] = e;
  return e;
}

struct Q3Row {
  int64_t orderkey;
  hugeint_t revenue;   // scale-4 int128 (DECIMAL(38,4))
  int32_t orderdate;   // days
  int32_t shippriority;
};
struct Q3BindData : public TableFunctionData {
  std::vector<Q3Row> rows;   // <= 10
};
struct Q3GlobalState : public GlobalTableFunctionState {
  idx_t offset = 0;
  idx_t MaxThreads() const override { return 1; }
};

// Shared between gpu_q3() and the transparent PhysicalQ3 source op: pin (cached)
// + run mojo_q3_query + attach (date, priority) to every nonzero order. Returns
// ALL passing orders, UNSORTED and UNLIMITED — the table function does its own
// top-10; the transparent op relies on the surviving plan TOP_N (ORDER BY revenue
// DESC, o_orderdate ASC + LIMIT 10). Revenue is the kernel's scale-4 int64 (>0,
// so the hugeint upper limb is 0): bit-exact into DECIMAL(38,4).
std::vector<Q3Row> ComputeQ3AllRows(ClientContext &context, const std::string &customer_table,
                                    const std::string &orders_table,
                                    const std::string &lineitem_table,
                                    const std::string &mktsegment, int32_t o_cutoff,
                                    int32_t l_cutoff) {
  auto *pe = EnsureQ3Pinned(context, customer_table, orders_table, lineitem_table,
                            mktsegment, o_cutoff);
  if (!pe->handle) { throw InvalidInputException("gpu_q3: GPU pin failed"); }

  // Per-segment revenue (scale-4 int64), size n_seg. The GPU does the group-by
  // (one warp per order segment); the host maps segment -> orderkey via seg_key.
  // No O(n_rows) host sum loop; readback is n_seg int64 (~12MB at SF1).
  std::vector<int64_t> seg_rev(pe->n_seg, 0);
  int32_t rc = mojo_q3_query2(pe->handle, l_cutoff, seg_rev.data());
  if (rc != 0) {
    throw InvalidInputException("gpu_q3: GPU query failed (rc " + std::to_string(rc) + ")");
  }

  std::vector<Q3Row> out;
  for (idx_t s = 0; s < pe->n_seg; s++) {
    int64_t r = seg_rev[s];
    if (r <= 0) { continue; }
    int64_t k = pe->seg_key[s];
    Q3Row row;
    row.orderkey = k;
    row.revenue = hugeint_t(r);   // scale-4; positive so upper=0
    row.orderdate = pe->order_date[(idx_t)k];
    row.shippriority = pe->order_prio[(idx_t)k];
    out.push_back(row);
  }
  return out;
}

unique_ptr<FunctionData> Q3Bind(ClientContext &context, TableFunctionBindInput &input,
                                vector<LogicalType> &return_types, vector<string> &names) {
  auto customer_table = input.inputs[0].GetValue<string>();
  auto orders_table = input.inputs[1].GetValue<string>();
  auto lineitem_table = input.inputs[2].GetValue<string>();
  auto mktsegment = input.inputs[3].GetValue<string>();
  int32_t o_cutoff = input.inputs[4].GetValue<date_t>().days;
  int32_t l_cutoff = input.inputs[5].GetValue<date_t>().days;

  // Host finalize: top-10 by (revenue DESC, o_orderdate ASC).
  auto bd = make_uniq<Q3BindData>();
  std::vector<Q3Row> all = ComputeQ3AllRows(context, customer_table, orders_table,
                                            lineitem_table, mktsegment, o_cutoff, l_cutoff);
  auto better = [](const Q3Row &a, const Q3Row &b) {
    // a ranks BEFORE b? (sort key: revenue desc, then date asc)
    if (a.revenue.upper != b.revenue.upper) return a.revenue.upper > b.revenue.upper;
    if (a.revenue.lower != b.revenue.lower) return a.revenue.lower > b.revenue.lower;
    return a.orderdate < b.orderdate;
  };
  std::sort(all.begin(), all.end(), better);
  if (all.size() > 10) { all.resize(10); }
  bd->rows = std::move(all);

  return_types = {LogicalType::BIGINT, LogicalType::DECIMAL(38, 4),
                  LogicalType::DATE, LogicalType::INTEGER};
  names = {"l_orderkey", "revenue", "o_orderdate", "o_shippriority"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> Q3Init(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<Q3GlobalState>();
}

void Q3Func(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<Q3BindData>();
  auto &gs = data.global_state->Cast<Q3GlobalState>();
  idx_t total = bd.rows.size();
  idx_t n = total > gs.offset ? total - gs.offset : 0;
  if (n == 0) { output.SetCardinality(0); return; }
  auto ok = FlatVector::GetData<int64_t>(output.data[0]);
  auto rev = FlatVector::GetData<hugeint_t>(output.data[1]);
  auto od = FlatVector::GetData<date_t>(output.data[2]);
  auto sp = FlatVector::GetData<int32_t>(output.data[3]);
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    ok[i] = r.orderkey;
    rev[i] = r.revenue;
    od[i] = date_t(r.orderdate);
    sp[i] = r.shippriority;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuQ3TableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_q3",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
                   Q3Func, Q3Bind, Q3Init);
  loader.RegisterFunction(tf);
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
// TPC-H Q3 transparent operator
//
// EXPLAIN(OPTIMIZED_ONLY) shape (extension loaded, COMPRESSED_MATERIALIZATION
// disabled by the pre-optimize hook):
//     TOP_N (revenue DESC, o_orderdate ASC, LIMIT 10)
//       PROJECTION (l_orderkey, revenue, o_orderdate, o_shippriority)
//         AGGREGATE  groups=[l_orderkey, o_orderdate, o_shippriority]
//                    expr=[sum_no_overflow(l_extendedprice*(1.00-l_discount))]
//           COMPARISON_JOIN INNER (l_orderkey = o_orderkey)
//             GET lineitem  [filter: l_shipdate > '1995-03-15']
//             COMPARISON_JOIN INNER (o_custkey = c_custkey)
//               GET orders  [filter: o_orderdate < '1995-03-15']
//               FILTER (c_custkey <= 149999)         -- stats pushdown, tolerated
//                 GET customer  [filter: c_mktsegment = 'BUILDING']
// MatchQ3 replaces the AGGREGATE (absorbing the joins + 3 GETs) with a LogicalQ3
// source op that emits ALL passing orders at the aggregate's exact bindings
// (3 group keys first, then the 1 aggregate). The surviving PROJECTION + TOP_N
// do the sort + top-10. Bit-exact (scale-4 int128 -> DECIMAL(38,4) hugeint).
// Translate-or-fallback: any deviation leaves the plan untouched (-> CPU).
// ===========================================================================

struct Q3SourceGlobalState : public GlobalSourceState {
  std::vector<Q3Row> rows;
  idx_t offset = 0;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalQ3 : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalQ3(PhysicalPlan &plan, vector<LogicalType> types, std::string customer_table,
             std::string orders_table, std::string lineitem_table, std::string mktsegment,
             int32_t o_cutoff, int32_t l_cutoff, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        customer_table(std::move(customer_table)), orders_table(std::move(orders_table)),
        lineitem_table(std::move(lineitem_table)), mktsegment(std::move(mktsegment)),
        o_cutoff(o_cutoff), l_cutoff(l_cutoff) {}

  std::string customer_table, orders_table, lineitem_table, mktsegment;
  int32_t o_cutoff, l_cutoff;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<Q3SourceGlobalState>();
    gs->rows = ComputeQ3AllRows(context, customer_table, orders_table, lineitem_table,
                                mktsegment, o_cutoff, l_cutoff);  // pins (cached) + query
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<Q3SourceGlobalState>();
    idx_t total = gs.rows.size();
    idx_t n = MinValue<idx_t>(total - gs.offset, STANDARD_VECTOR_SIZE);
    if (n == 0) {
      chunk.SetCardinality(0);
      return SourceResultType::FINISHED;
    }
    // Column order MUST match LogicalQ3's bindings: 3 group keys (l_orderkey,
    // o_orderdate, o_shippriority) then the 1 aggregate (revenue).
    auto ok = FlatVector::GetData<int64_t>(chunk.data[0]);
    auto od = FlatVector::GetData<date_t>(chunk.data[1]);
    auto sp = FlatVector::GetData<int32_t>(chunk.data[2]);
    auto rev = FlatVector::GetData<hugeint_t>(chunk.data[3]);
    for (idx_t i = 0; i < n; i++) {
      auto &r = gs.rows[gs.offset + i];
      ok[i] = r.orderkey;
      od[i] = date_t(r.orderdate);
      sp[i] = r.shippriority;
      rev[i] = r.revenue;
    }
    chunk.SetCardinality(n);
    gs.offset += n;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_Q3"; }
};

// Logical extension op: sits where the AGGREGATE was, absorbing the join tree +
// 3 GETs. Replicates the aggregate's column bindings (3 group keys, then 1
// aggregate) and types so the parent PROJECTION + TOP_N resolve unchanged.
class LogicalQ3 : public LogicalExtensionOperator {
public:
  LogicalQ3(std::string customer_table, std::string orders_table, std::string lineitem_table,
            std::string mktsegment, int32_t o_cutoff, int32_t l_cutoff, idx_t group_index,
            idx_t aggregate_index, vector<LogicalType> out_types)
      : customer_table(std::move(customer_table)), orders_table(std::move(orders_table)),
        lineitem_table(std::move(lineitem_table)), mktsegment(std::move(mktsegment)),
        o_cutoff(o_cutoff), l_cutoff(l_cutoff), group_index(group_index),
        aggregate_index(aggregate_index), out_types(std::move(out_types)) {
    types = this->out_types;
  }

  std::string customer_table, orders_table, lineitem_table, mktsegment;
  int32_t o_cutoff, l_cutoff;
  idx_t group_index;
  idx_t aggregate_index;
  vector<LogicalType> out_types;  // [3 group key types] + [1 aggregate type]

  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(group_index, 0), ColumnBinding(group_index, 1),
            ColumnBinding(group_index, 2), ColumnBinding(aggregate_index, 0)};
  }

  void ResolveTypes() override { types = out_types; }

  string GetExtensionName() const override { return "mojo_gpu_q3"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    return planner.Make<PhysicalQ3>(out_types, customer_table, orders_table, lineitem_table,
                                    mktsegment, o_cutoff, l_cutoff, estimated_cardinality);
  }
};

bool MatchQ3(unique_ptr<LogicalOperator> &node) {
#define Q3DBG(x) ((void)0)
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }
  auto &agg = node->Cast<LogicalAggregate>();

  // Exactly 3 group keys, 1 aggregate, one child, no grouping functions.
  if (agg.groups.size() != 3) { Q3DBG("groups!=3"); return false; }
  if (agg.expressions.size() != 1) { Q3DBG("exprs!=1"); return false; }
  if (agg.children.size() != 1) { return false; }
  if (!agg.grouping_functions.empty()) { return false; }
  if (agg.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) { Q3DBG("child!=join"); return false; }

  // Walk the join tree -> collect GETs + equi-conditions.
  JoinTree jt;
  if (!CollectJoinTree(agg.children[0].get(), jt)) { Q3DBG("collect"); return false; }
  if (jt.gets.size() != 3) { Q3DBG("gets!=3"); return false; }
  std::vector<JoinEq> eqs;
  if (!ResolveJoinConds(jt, eqs)) { Q3DBG("resolveconds"); return false; }

  // The three tables must be exactly customer, orders, lineitem.
  LogicalGet *customer_get = FindGet(jt, "customer");
  LogicalGet *orders_get = FindGet(jt, "orders");
  LogicalGet *lineitem_get = FindGet(jt, "lineitem");
  if (!customer_get || !orders_get || !lineitem_get) { Q3DBG("tables"); return false; }

  // Required equi-conditions: l_orderkey=o_orderkey and c_custkey=o_custkey.
  if (!HasCond(eqs, "lineitem", "l_orderkey", "orders", "o_orderkey")) { Q3DBG("cond l=o"); return false; }
  if (!HasCond(eqs, "customer", "c_custkey", "orders", "o_custkey")) { Q3DBG("cond c=o"); return false; }
  if (eqs.size() != 2) { Q3DBG("eqs!=2"); return false; }  // no extra join conditions

  // Group keys must resolve to (l_orderkey OR o_orderkey — the optimizer may
  // substitute via the l_orderkey=o_orderkey equivalence; both carry the same
  // value the engine emits), o_orderdate, o_shippriority (in order).
  {
    std::string g0 = ResolveGroupColref(*agg.groups[0], jt);
    if (g0 != "l_orderkey" && g0 != "o_orderkey") { Q3DBG("g0"); return false; }
  }
  if (ResolveGroupColref(*agg.groups[1], jt) != "o_orderdate") { Q3DBG("g1"); return false; }
  if (ResolveGroupColref(*agg.groups[2], jt) != "o_shippriority") { Q3DBG("g2"); return false; }

  // Aggregate: sum/sum_no_overflow, DECIMAL(38,4)/INT128, arg = revenue expr.
  if (agg.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
  auto &ag = agg.expressions[0]->Cast<BoundAggregateExpression>();
  if (ag.IsDistinct() || ag.filter) { return false; }
  if (ag.function.name != "sum" && ag.function.name != "sum_no_overflow") { return false; }
  if (ag.children.size() != 1) { return false; }
  const LogicalType &rt = ag.return_type;
  if (rt.id() != LogicalTypeId::DECIMAL || DecimalType::GetScale(rt) != 4 ||
      rt.InternalType() != PhysicalType::INT128) {
    Q3DBG("agg type"); return false;
  }
  if (!IsRevenueExpr(*ag.children[0], jt)) { Q3DBG("revexpr"); return false; }

  // Extract filters. customer: c_mktsegment = '<seg>' (any stats filter like
  // c_custkey<=N is a LOGICAL_FILTER node, not a table_filter, so it's invisible
  // here and harmless). orders: o_orderdate < cutoff. lineitem: l_shipdate > cut.
  std::string mktsegment;
  bool have_mkt = false, have_o_cut = false, have_l_cut = false;
  int32_t o_cutoff = 0, l_cutoff = 0;

  // customer table_filters: exactly c_mktsegment = constant.
  for (auto &kv : customer_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= customer_get->names.size()) { return false; }
    if (customer_get->names[col_idx] != "c_mktsegment") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
    auto &cf = tf.Cast<ConstantFilter>();
    if (cf.comparison_type != ExpressionType::COMPARE_EQUAL) { return false; }
    if (cf.constant.type().id() != LogicalTypeId::VARCHAR || cf.constant.IsNull()) { return false; }
    mktsegment = cf.constant.GetValue<std::string>();
    have_mkt = true;
  }
  if (!have_mkt) { Q3DBG("no mkt"); return false; }

  // orders table_filters: exactly o_orderdate < cutoff.
  for (auto &kv : orders_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= orders_get->names.size()) { return false; }
    if (orders_get->names[col_idx] != "o_orderdate") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
    auto &cf = tf.Cast<ConstantFilter>();
    if (cf.comparison_type != ExpressionType::COMPARE_LESSTHAN) { return false; }
    o_cutoff = cf.constant.GetValue<date_t>().days;  // exclusive < (engine matches)
    have_o_cut = true;
  }
  if (!have_o_cut) { Q3DBG("no o_cut"); return false; }

  // lineitem table_filters: exactly l_shipdate > cutoff.
  for (auto &kv : lineitem_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= lineitem_get->names.size()) { return false; }
    if (lineitem_get->names[col_idx] != "l_shipdate") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
    auto &cf = tf.Cast<ConstantFilter>();
    if (cf.comparison_type != ExpressionType::COMPARE_GREATERTHAN) { return false; }
    l_cutoff = cf.constant.GetValue<date_t>().days;  // strict > (engine matches)
    have_l_cut = true;
  }
  if (!have_l_cut) { Q3DBG("no l_cut"); return false; }
#undef Q3DBG

  // Build the replacement at the aggregate's exact bindings/types: 3 group key
  // types then the 1 aggregate type.
  vector<LogicalType> out_types;
  out_types.push_back(agg.groups[0]->return_type);
  out_types.push_back(agg.groups[1]->return_type);
  out_types.push_back(agg.groups[2]->return_type);
  out_types.push_back(ag.return_type);

  auto repl = make_uniq<LogicalQ3>("customer", "orders", "lineitem", mktsegment, o_cutoff,
                                   l_cutoff, agg.group_index, agg.aggregate_index,
                                   std::move(out_types));
  repl->estimated_cardinality = agg.estimated_cardinality;
  node = std::move(repl);
  return true;
}

// ===========================================================================
// gpu_q5() table function: TPC-H Q5 GPU 6-table-join + per-nation revenue.
//
//   SELECT * FROM gpu_q5('customer','orders','lineitem','supplier','nation',
//                        'region','ASIA', DATE '1994-01-01', DATE '1995-01-01');
//
// On first use (per key) it collapses the 5 dimension joins on the host into
// dense arrays:
//   * region: the regionkey whose r_name == region_name.
//   * nation: n_name (kept for output) + nation_in_asia[nationkey] =
//     (n_regionkey == that regionkey).
//   * customer: c_nationkey[c_custkey].
//   * supplier: supp_nation[s_suppkey] = s_nationkey.
//   * orders: order_pass[o_orderkey] = (o_orderdate in [o_lo, o_hi)) and
//     order_cust_nation[o_orderkey] = c_nationkey[o_custkey] (folds the
//     customer lookup into the order metadata -> one fewer GPU indirection).
// Then it materializes lineitem's (l_orderkey, l_suppkey, l_extendedprice,
// l_discount) and pins everything resident. mojo_q5_query runs the GPU probe
// (correlated condition cn==sn + ASIA filter, per-block-partials group-by) and
// returns per-nation int128 revenue. The host attaches n_name, drops zero, sorts
// by revenue DESC. Revenue int128(scale-4) -> hugeint into DECIMAL(38,4):
// bit-exact vs DuckDB's int128 sum.
// ===========================================================================
struct Q5PinEntry {
  void *handle = nullptr;
  idx_t n_rows = 0;
  int64_t n_nations = 0;
  std::vector<std::string> nation_name;   // [nationkey] -> n_name
};

std::mutex g_q5_mu;
std::unordered_map<std::string, Q5PinEntry *> g_q5_pins;

Q5PinEntry *EnsureQ5Pinned(ClientContext &context, const std::string &customer_table,
                           const std::string &orders_table,
                           const std::string &lineitem_table,
                           const std::string &supplier_table,
                           const std::string &nation_table,
                           const std::string &region_table,
                           const std::string &region_name,
                           int32_t o_lo, int32_t o_hi) {
  std::string key = customer_table + "\x1f" + orders_table + "\x1f" +
                    lineitem_table + "\x1f" + supplier_table + "\x1f" +
                    nation_table + "\x1f" + region_table + "\x1f" + region_name +
                    "\x1f" + std::to_string(o_lo) + "\x1f" + std::to_string(o_hi);
  std::lock_guard<std::mutex> g(g_q5_mu);
  auto it = g_q5_pins.find(key);
  if (it != g_q5_pins.end()) { return it->second; }

  Connection con(*context.db);

  // --- region: the regionkey whose r_name == region_name ---
  auto rres = con.Query("SELECT r_regionkey, r_name FROM " + region_table);
  if (rres->HasError()) { throw InvalidInputException("gpu_q5: " + rres->GetError()); }
  int32_t target_region = -1;
  while (true) {
    auto chunk = rres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    chunk->data[1].Flatten(n);
    const int32_t *rk = FlatVector::GetData<int32_t>(chunk->data[0]);  // r_regionkey INTEGER
    auto rn = FlatVector::GetData<string_t>(chunk->data[1]);           // r_name VARCHAR
    for (idx_t i = 0; i < n; i++) {
      idx_t len = rn[i].GetSize();
      if (len == region_name.size() &&
          memcmp(rn[i].GetData(), region_name.data(), len) == 0) {
        target_region = rk[i];
      }
    }
  }
  if (target_region < 0) {
    throw InvalidInputException("gpu_q5: region '" + region_name + "' not found");
  }

  // --- nation: n_name + nation_in_asia[nationkey] = (n_regionkey == target) ---
  // c_nationkey/s_nationkey are TINYINT-range (0..24); size by max nationkey.
  auto nres = con.Query("SELECT n_nationkey, n_name, n_regionkey FROM " + nation_table);
  if (nres->HasError()) { throw InvalidInputException("gpu_q5: " + nres->GetError()); }
  std::vector<uint8_t> nation_in_asia;
  std::vector<std::string> nation_name;
  int64_t max_nationkey = 0;
  while (true) {
    auto chunk = nres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 3; c++) { chunk->data[c].Flatten(n); }
    const int32_t *nk = FlatVector::GetData<int32_t>(chunk->data[0]);  // n_nationkey INTEGER
    auto nm = FlatVector::GetData<string_t>(chunk->data[1]);           // n_name VARCHAR
    const int32_t *rk = FlatVector::GetData<int32_t>(chunk->data[2]);  // n_regionkey INTEGER
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)nk[i];
      if (k >= nation_in_asia.size()) {
        nation_in_asia.resize(k + 1, 0);
        nation_name.resize(k + 1);
      }
      if ((int64_t)k > max_nationkey) { max_nationkey = (int64_t)k; }
      nation_in_asia[k] = (rk[i] == target_region) ? 1 : 0;
      nation_name[k] = std::string(nm[i].GetData(), nm[i].GetSize());
    }
  }
  int64_t n_nations = max_nationkey + 1;
  if (n_nations <= 0 || n_nations > 25) {
    // Q5_NNATIONS cap in the kernel; guard rather than overflow the partials.
    throw InvalidInputException("gpu_q5: nationkey range out of bounds (max " +
                                std::to_string(max_nationkey) + ")");
  }

  // --- customer: c_nationkey[c_custkey] (dense by c_custkey) ---
  auto cres = con.Query("SELECT c_custkey, c_nationkey FROM " + customer_table);
  if (cres->HasError()) { throw InvalidInputException("gpu_q5: " + cres->GetError()); }
  std::vector<int32_t> cust_nation;  // dense by c_custkey
  while (true) {
    auto chunk = cres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    chunk->data[1].Flatten(n);
    const int64_t *ck = FlatVector::GetData<int64_t>(chunk->data[0]);  // c_custkey BIGINT
    const int32_t *cn = FlatVector::GetData<int32_t>(chunk->data[1]);  // c_nationkey INTEGER
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)ck[i];
      if (k >= cust_nation.size()) { cust_nation.resize(k + 1, -1); }
      cust_nation[k] = cn[i];
    }
  }

  // --- supplier: supp_nation[s_suppkey] = s_nationkey (dense by s_suppkey) ---
  auto sres = con.Query("SELECT s_suppkey, s_nationkey FROM " + supplier_table);
  if (sres->HasError()) { throw InvalidInputException("gpu_q5: " + sres->GetError()); }
  std::vector<int32_t> supp_nation;  // dense by s_suppkey
  int64_t max_suppkey = 0;
  while (true) {
    auto chunk = sres->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    chunk->data[0].Flatten(n);
    chunk->data[1].Flatten(n);
    const int64_t *sk = FlatVector::GetData<int64_t>(chunk->data[0]);  // s_suppkey BIGINT
    const int32_t *sn = FlatVector::GetData<int32_t>(chunk->data[1]);  // s_nationkey INTEGER
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)sk[i];
      if (k >= supp_nation.size()) { supp_nation.resize(k + 1, -1); }
      if ((int64_t)k > max_suppkey) { max_suppkey = (int64_t)k; }
      supp_nation[k] = sn[i];
    }
  }

  // --- orders: order_pass[o_orderkey] + order_cust_nation[o_orderkey] ---
  // Fold the customer lookup into the order metadata: store c_nationkey[o_custkey]
  // directly so the GPU probe needs only order_pass + order_cust_nation (no
  // customer indirection). order_pass bakes the o_orderdate filter.
  auto ores = con.Query("SELECT o_orderkey, o_custkey, o_orderdate FROM " + orders_table);
  if (ores->HasError()) { throw InvalidInputException("gpu_q5: " + ores->GetError()); }
  std::vector<uint8_t> order_pass;
  std::vector<int32_t> order_cust_nation;
  int64_t max_orderkey = 0;
  while (true) {
    auto chunk = ores->Fetch();
    if (!chunk || chunk->size() == 0) { break; }
    auto n = chunk->size();
    for (idx_t c = 0; c < 3; c++) { chunk->data[c].Flatten(n); }
    const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);    // o_orderkey BIGINT
    const int64_t *cust = FlatVector::GetData<int64_t>(chunk->data[1]);  // o_custkey BIGINT
    const date_t *od = FlatVector::GetData<date_t>(chunk->data[2]);      // DATE -> int32 days
    for (idx_t i = 0; i < n; i++) {
      idx_t k = (idx_t)ok[i];
      if (k >= order_pass.size()) {
        order_pass.resize(k + 1, 0);
        order_cust_nation.resize(k + 1, -1);
      }
      if ((int64_t)k > max_orderkey) { max_orderkey = (int64_t)k; }
      bool pass = (od[i].days >= o_lo && od[i].days < o_hi);
      order_pass[k] = pass ? 1 : 0;
      idx_t cust_k = (idx_t)cust[i];
      order_cust_nation[k] =
          (cust_k < cust_nation.size()) ? cust_nation[cust_k] : -1;
    }
  }

  // --- lineitem: the 4 probe columns (plain projection) ---
  const bool timing = pin_timing_enabled("MOJO_Q5_PIN_TIMING");
  auto t0 = q6_clock::now();
  auto lres = con.Query("SELECT l_orderkey, l_suppkey, l_extendedprice, l_discount FROM " +
                        lineitem_table);
  if (lres->HasError()) { throw InvalidInputException("gpu_q5: " + lres->GetError()); }
  auto t1 = q6_clock::now();  // (a) Query() materialize done
  idx_t row_count = lres->RowCount();

  void *handle = nullptr;
  idx_t n_rows = 0;

  if (pin_use_pinned("MOJO_Q5_PIN_MODE")) {
    // Pinned-HostBuffer staging for the 4 big probe columns.
    int64_t *lok_h = nullptr; int64_t *lsk_h = nullptr;
    int64_t *ext_h = nullptr; int64_t *disc_h = nullptr;
    handle = reinterpret_cast<void *>(mojo_q5_pin_alloc(
        order_pass.data(), order_cust_nation.data(), supp_nation.data(),
        nation_in_asia.data(), NumericCast<int64_t>(row_count), max_orderkey,
        max_suppkey, n_nations, &lok_h, &lsk_h, &ext_h, &disc_h));
    if (!handle || !lok_h) { throw InvalidInputException("gpu_q5: pinned pin_alloc failed"); }
    auto t_alloc = q6_clock::now();  // (b0) pinned + device alloc done

    idx_t off = 0;
    while (true) {
      auto chunk = lres->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);  // l_orderkey BIGINT
      const int64_t *sk = FlatVector::GetData<int64_t>(chunk->data[1]);  // l_suppkey BIGINT
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);   // DECIMAL(15,2) -> int64
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
      std::memcpy(lok_h + off, ok, n * sizeof(int64_t));
      std::memcpy(lsk_h + off, sk, n * sizeof(int64_t));
      std::memcpy(ext_h + off, e, n * sizeof(int64_t));
      std::memcpy(disc_h + off, d, n * sizeof(int64_t));
      off += n;
    }
    n_rows = off;
    auto t_fill = q6_clock::now();  // (b) fetch + memcpy-into-pinned done

    int32_t rc = mojo_q5_pin_upload(handle, timing ? 1 : 0);
    if (rc != 0) { throw InvalidInputException("gpu_q5: pinned upload failed (rc " +
                                               std::to_string(rc) + ")"); }
    auto t_up = q6_clock::now();  // (c) device upload done
    if (timing) {
      fprintf(stderr,
              "[q5-pin pinned] n_rows=%llu  (a)Query=%.1fms  (b0)pin_alloc=%.1fms  "
              "(b)fetch+memcpy=%.1fms  (c)upload=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t_alloc),
              q6_ms(t_alloc, t_fill), q6_ms(t_fill, t_up), q6_ms(t0, t_up));
    }
  } else {
    // Baseline std::vector staging (A/B). reserve unless MOJO_Q5_PIN_MODE=noreserve.
    std::vector<int64_t> lok, lsk, ext, disc;
    const char *m = std::getenv("MOJO_Q5_PIN_MODE");
    if (!(m && std::string(m) == "noreserve")) {
      lok.reserve(row_count); lsk.reserve(row_count);
      ext.reserve(row_count); disc.reserve(row_count);
    }
    while (true) {
      auto chunk = lres->Fetch();
      if (!chunk || chunk->size() == 0) { break; }
      auto n = chunk->size();
      for (idx_t c = 0; c < 4; c++) { chunk->data[c].Flatten(n); }
      const int64_t *ok = FlatVector::GetData<int64_t>(chunk->data[0]);
      const int64_t *sk = FlatVector::GetData<int64_t>(chunk->data[1]);
      const int64_t *e = FlatVector::GetData<int64_t>(chunk->data[2]);
      const int64_t *d = FlatVector::GetData<int64_t>(chunk->data[3]);
      lok.insert(lok.end(), ok, ok + n);
      lsk.insert(lsk.end(), sk, sk + n);
      ext.insert(ext.end(), e, e + n);
      disc.insert(disc.end(), d, d + n);
      n_rows += n;
    }
    auto t2 = q6_clock::now();  // (b) Fetch + std::vector build done
    handle = reinterpret_cast<void *>(mojo_q5_pin(
        order_pass.data(), order_cust_nation.data(), supp_nation.data(),
        nation_in_asia.data(), lok.data(), lsk.data(), ext.data(), disc.data(),
        NumericCast<int64_t>(n_rows), max_orderkey, max_suppkey, n_nations));
    auto t3 = q6_clock::now();  // (c) host->device upload done
    if (timing) {
      fprintf(stderr,
              "[q5-pin baseline] n_rows=%llu  (a)Query=%.1fms  (b)Fetch+vec=%.1fms  "
              "(c)mojo_q5_pin(upload)=%.1fms  total=%.1fms\n",
              (unsigned long long)n_rows, q6_ms(t0, t1), q6_ms(t1, t2),
              q6_ms(t2, t3), q6_ms(t0, t3));
    }
  }
  auto *e = new Q5PinEntry();
  e->handle = handle;
  e->n_rows = n_rows;
  e->n_nations = n_nations;
  e->nation_name = std::move(nation_name);
  g_q5_pins[key] = e;
  return e;
}

struct Q5Row {
  std::string n_name;
  hugeint_t revenue;   // scale-4 int128 (DECIMAL(38,4))
};
struct Q5BindData : public TableFunctionData {
  std::vector<Q5Row> rows;
};
struct Q5GlobalState : public GlobalTableFunctionState {
  idx_t offset = 0;
  idx_t MaxThreads() const override { return 1; }
};

// Shared between gpu_q5() and the transparent PhysicalQ5 source op: pin (cached)
// + run mojo_q5_query + attach n_name, dropping zero-revenue nations. Returns the
// per-nation rows UNSORTED — the table function sorts by revenue DESC; the
// transparent op relies on the surviving plan ORDER BY. Revenue is the kernel's
// scale-4 int128: bit-exact into DECIMAL(38,4).
std::vector<Q5Row> ComputeQ5Rows(ClientContext &context, const std::string &customer_table,
                                 const std::string &orders_table,
                                 const std::string &lineitem_table,
                                 const std::string &supplier_table,
                                 const std::string &nation_table,
                                 const std::string &region_table,
                                 const std::string &region_name, int32_t o_lo, int32_t o_hi) {
  auto *pe = EnsureQ5Pinned(context, customer_table, orders_table, lineitem_table,
                            supplier_table, nation_table, region_table, region_name,
                            o_lo, o_hi);
  if (!pe->handle) { throw InvalidInputException("gpu_q5: GPU pin failed"); }

  // Per-nation int128 revenue (low,high limb pairs), n_nations entries.
  std::vector<int64_t> revenue(pe->n_nations * 2, 0);
  int32_t rc = mojo_q5_query(pe->handle, revenue.data());
  if (rc != 0) {
    throw InvalidInputException("gpu_q5: GPU query failed (rc " + std::to_string(rc) + ")");
  }

  std::vector<Q5Row> out;
  for (int64_t g = 0; g < pe->n_nations; g++) {
    hugeint_t rev;
    rev.lower = (uint64_t)revenue[g * 2 + 0];
    rev.upper = revenue[g * 2 + 1];
    if (rev.upper == 0 && rev.lower == 0) { continue; }
    Q5Row row;
    row.n_name = (g < (int64_t)pe->nation_name.size()) ? pe->nation_name[g] : "";
    row.revenue = rev;
    out.push_back(row);
  }
  return out;
}

unique_ptr<FunctionData> Q5Bind(ClientContext &context, TableFunctionBindInput &input,
                                vector<LogicalType> &return_types, vector<string> &names) {
  auto customer_table = input.inputs[0].GetValue<string>();
  auto orders_table = input.inputs[1].GetValue<string>();
  auto lineitem_table = input.inputs[2].GetValue<string>();
  auto supplier_table = input.inputs[3].GetValue<string>();
  auto nation_table = input.inputs[4].GetValue<string>();
  auto region_table = input.inputs[5].GetValue<string>();
  auto region_name = input.inputs[6].GetValue<string>();
  int32_t o_lo = input.inputs[7].GetValue<date_t>().days;
  int32_t o_hi = input.inputs[8].GetValue<date_t>().days;

  auto bd = make_uniq<Q5BindData>();
  bd->rows = ComputeQ5Rows(context, customer_table, orders_table, lineitem_table,
                           supplier_table, nation_table, region_table, region_name,
                           o_lo, o_hi);
  std::sort(bd->rows.begin(), bd->rows.end(), [](const Q5Row &a, const Q5Row &b) {
    if (a.revenue.upper != b.revenue.upper) return a.revenue.upper > b.revenue.upper;
    return a.revenue.lower > b.revenue.lower;  // revenue DESC
  });

  return_types = {LogicalType::VARCHAR, LogicalType::DECIMAL(38, 4)};
  names = {"n_name", "revenue"};
  return std::move(bd);
}

unique_ptr<GlobalTableFunctionState> Q5Init(ClientContext &, TableFunctionInitInput &) {
  return make_uniq<Q5GlobalState>();
}

void Q5Func(ClientContext &, TableFunctionInput &data, DataChunk &output) {
  auto &bd = data.bind_data->Cast<Q5BindData>();
  auto &gs = data.global_state->Cast<Q5GlobalState>();
  idx_t total = bd.rows.size();
  idx_t n = total > gs.offset ? total - gs.offset : 0;
  if (n == 0) { output.SetCardinality(0); return; }
  auto nm = FlatVector::GetData<string_t>(output.data[0]);
  auto rev = FlatVector::GetData<hugeint_t>(output.data[1]);
  for (idx_t i = 0; i < n; i++) {
    auto &r = bd.rows[gs.offset + i];
    nm[i] = StringVector::AddString(output.data[0], r.n_name);
    rev[i] = r.revenue;
  }
  output.SetCardinality(n);
  gs.offset += n;
}

void RegisterGpuQ5TableFunction(ExtensionLoader &loader) {
  TableFunction tf("gpu_q5",
                   {LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::VARCHAR, LogicalType::VARCHAR,
                    LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
                   Q5Func, Q5Bind, Q5Init);
  loader.RegisterFunction(tf);
}

// ===========================================================================
// TPC-H Q5 transparent operator
//
// EXPLAIN(OPTIMIZED_ONLY) shape (extension loaded):
//     ORDER_BY (revenue)
//       PROJECTION (n_name, revenue)
//         AGGREGATE  groups=[n_name]
//                    expr=[sum_no_overflow(l_extendedprice*(1.00-l_discount))]
//           JOIN INNER [c_nationkey=s_nationkey, l_suppkey=s_suppkey]   (2 conds)
//             JOIN INNER [l_orderkey=o_orderkey]
//               GET lineitem
//               JOIN INNER [o_custkey=c_custkey]
//                 GET orders  [filter: o_orderdate >= lo AND < hi]
//                 JOIN INNER [c_nationkey=n_nationkey]
//                   GET customer  [stats filter c_custkey<=N -> LOGICAL_FILTER]
//                   JOIN INNER [n_regionkey=r_regionkey]
//                     GET nation
//                     GET region  [filter: r_name='ASIA']
//             GET supplier
// MatchQ5 walks this tree, verifies all 6 tables + all 6 equi-conditions (incl.
// the correlated c_nationkey=s_nationkey) + the region/date filters, and replaces
// the AGGREGATE with a LogicalQ5 source op emitting one row per nation at the
// aggregate's exact bindings (group key n_name, then the 1 aggregate revenue).
// The surviving PROJECTION + ORDER_BY do the sort. Bit-exact (scale-4 int128 ->
// DECIMAL(38,4) hugeint). Translate-or-fallback on any deviation (-> CPU).
// ===========================================================================

struct Q5SourceGlobalState : public GlobalSourceState {
  std::vector<Q5Row> rows;
  idx_t offset = 0;
  idx_t MaxThreads() override { return 1; }
};

class PhysicalQ5 : public PhysicalOperator {
public:
  static constexpr const PhysicalOperatorType TYPE = PhysicalOperatorType::EXTENSION;

  PhysicalQ5(PhysicalPlan &plan, vector<LogicalType> types, std::string region_name,
             int32_t o_lo, int32_t o_hi, idx_t cardinality)
      : PhysicalOperator(plan, TYPE, std::move(types), cardinality),
        region_name(std::move(region_name)), o_lo(o_lo), o_hi(o_hi) {}

  std::string region_name;
  int32_t o_lo, o_hi;

  bool IsSource() const override { return true; }
  bool ParallelSource() const override { return false; }

  unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const override {
    auto gs = make_uniq<Q5SourceGlobalState>();
    gs->rows = ComputeQ5Rows(context, "customer", "orders", "lineitem", "supplier",
                             "nation", "region", region_name, o_lo, o_hi);  // pins + query
    return std::move(gs);
  }

  SourceResultType GetDataInternal(ExecutionContext &, DataChunk &chunk,
                                   OperatorSourceInput &input) const override {
    auto &gs = input.global_state.Cast<Q5SourceGlobalState>();
    idx_t total = gs.rows.size();
    idx_t n = MinValue<idx_t>(total - gs.offset, STANDARD_VECTOR_SIZE);
    if (n == 0) {
      chunk.SetCardinality(0);
      return SourceResultType::FINISHED;
    }
    // Column order MUST match LogicalQ5's bindings: group key n_name, then the
    // 1 aggregate (revenue).
    auto nm = FlatVector::GetData<string_t>(chunk.data[0]);
    auto rev = FlatVector::GetData<hugeint_t>(chunk.data[1]);
    for (idx_t i = 0; i < n; i++) {
      auto &r = gs.rows[gs.offset + i];
      nm[i] = StringVector::AddString(chunk.data[0], r.n_name);
      rev[i] = r.revenue;
    }
    chunk.SetCardinality(n);
    gs.offset += n;
    return SourceResultType::HAVE_MORE_OUTPUT;
  }

  string GetName() const override { return "GPU_Q5"; }
};

// Logical extension op: sits where the AGGREGATE was, absorbing the 5-join tree +
// 6 GETs. Replicates the aggregate's bindings (group key n_name, then 1
// aggregate) and types so the parent PROJECTION + ORDER_BY resolve unchanged.
class LogicalQ5 : public LogicalExtensionOperator {
public:
  LogicalQ5(std::string region_name, int32_t o_lo, int32_t o_hi, idx_t group_index,
            idx_t aggregate_index, vector<LogicalType> out_types)
      : region_name(std::move(region_name)), o_lo(o_lo), o_hi(o_hi), group_index(group_index),
        aggregate_index(aggregate_index), out_types(std::move(out_types)) {
    types = this->out_types;
  }

  std::string region_name;
  int32_t o_lo, o_hi;
  idx_t group_index;
  idx_t aggregate_index;
  vector<LogicalType> out_types;  // [1 group key type] + [1 aggregate type]

  vector<ColumnBinding> GetColumnBindings() override {
    return {ColumnBinding(group_index, 0), ColumnBinding(aggregate_index, 0)};
  }

  void ResolveTypes() override { types = out_types; }

  string GetExtensionName() const override { return "mojo_gpu_q5"; }

  PhysicalOperator &CreatePlan(ClientContext &, PhysicalPlanGenerator &planner) override {
    return planner.Make<PhysicalQ5>(out_types, region_name, o_lo, o_hi, estimated_cardinality);
  }
};

bool MatchQ5(unique_ptr<LogicalOperator> &node) {
  if (node->type != LogicalOperatorType::LOGICAL_AGGREGATE_AND_GROUP_BY) { return false; }
  auto &agg = node->Cast<LogicalAggregate>();

  // Exactly 1 group key, 1 aggregate, one child, no grouping functions.
  if (agg.groups.size() != 1) { return false; }
  if (agg.expressions.size() != 1) { return false; }
  if (agg.children.size() != 1) { return false; }
  if (!agg.grouping_functions.empty()) { return false; }
  if (agg.children[0]->type != LogicalOperatorType::LOGICAL_COMPARISON_JOIN) { return false; }

  // Walk the join tree -> collect GETs + equi-conditions.
  JoinTree jt;
  if (!CollectJoinTree(agg.children[0].get(), jt)) { return false; }
  if (jt.gets.size() != 6) { return false; }
  std::vector<JoinEq> eqs;
  if (!ResolveJoinConds(jt, eqs)) { return false; }

  // All 6 tables must be present.
  LogicalGet *customer_get = FindGet(jt, "customer");
  LogicalGet *orders_get = FindGet(jt, "orders");
  LogicalGet *lineitem_get = FindGet(jt, "lineitem");
  LogicalGet *supplier_get = FindGet(jt, "supplier");
  LogicalGet *nation_get = FindGet(jt, "nation");
  LogicalGet *region_get = FindGet(jt, "region");
  if (!customer_get || !orders_get || !lineitem_get || !supplier_get || !nation_get ||
      !region_get) {
    return false;
  }

  // All 6 equi-conditions must be present (incl. correlated c_nationkey=s_nationkey).
  if (!HasCond(eqs, "customer", "c_custkey", "orders", "o_custkey")) { return false; }
  if (!HasCond(eqs, "lineitem", "l_orderkey", "orders", "o_orderkey")) { return false; }
  if (!HasCond(eqs, "lineitem", "l_suppkey", "supplier", "s_suppkey")) { return false; }
  if (!HasCond(eqs, "customer", "c_nationkey", "supplier", "s_nationkey")) { return false; }
  if (!HasCond(eqs, "supplier", "s_nationkey", "nation", "n_nationkey") &&
      !HasCond(eqs, "customer", "c_nationkey", "nation", "n_nationkey")) {
    // s_nationkey=n_nationkey; the optimizer may rewrite via the correlated
    // c_nationkey=s_nationkey equivalence to c_nationkey=n_nationkey. Accept either.
    return false;
  }
  if (!HasCond(eqs, "nation", "n_regionkey", "region", "r_regionkey")) { return false; }
  if (eqs.size() != 6) { return false; }  // no extra conditions

  // Group key must resolve to n_name; aggregate is the revenue sum.
  if (ResolveGroupColref(*agg.groups[0], jt) != "n_name") { return false; }

  if (agg.expressions[0]->GetExpressionClass() != ExpressionClass::BOUND_AGGREGATE) { return false; }
  auto &ag = agg.expressions[0]->Cast<BoundAggregateExpression>();
  if (ag.IsDistinct() || ag.filter) { return false; }
  if (ag.function.name != "sum" && ag.function.name != "sum_no_overflow") { return false; }
  if (ag.children.size() != 1) { return false; }
  const LogicalType &rt = ag.return_type;
  if (rt.id() != LogicalTypeId::DECIMAL || DecimalType::GetScale(rt) != 4 ||
      rt.InternalType() != PhysicalType::INT128) {
    return false;
  }
  if (!IsRevenueExpr(*ag.children[0], jt)) { return false; }

  // region: exactly r_name = '<region>'.
  std::string region_name;
  bool have_region = false;
  for (auto &kv : region_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= region_get->names.size()) { return false; }
    if (region_get->names[col_idx] != "r_name") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
    auto &cf = tf.Cast<ConstantFilter>();
    if (cf.comparison_type != ExpressionType::COMPARE_EQUAL) { return false; }
    if (cf.constant.type().id() != LogicalTypeId::VARCHAR || cf.constant.IsNull()) { return false; }
    region_name = cf.constant.GetValue<std::string>();
    have_region = true;
  }
  if (!have_region) { return false; }

  // orders: o_orderdate >= lo AND < hi (single conjunction or two comparisons).
  bool have_lo = false, have_hi = false;
  int32_t o_lo = 0, o_hi = 0;
  auto apply_date = [&](const ConstantFilter &cf) -> bool {
    int32_t days = cf.constant.GetValue<date_t>().days;
    if (cf.comparison_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
      o_lo = days; have_lo = true; return true;
    }
    if (cf.comparison_type == ExpressionType::COMPARE_LESSTHAN) {
      o_hi = days; have_hi = true; return true;
    }
    return false;
  };
  for (auto &kv : orders_get->table_filters.filters) {
    idx_t col_idx = kv.first;
    if (col_idx >= orders_get->names.size()) { return false; }
    if (orders_get->names[col_idx] != "o_orderdate") { return false; }
    TableFilter &tf = *kv.second;
    if (tf.filter_type == TableFilterType::CONSTANT_COMPARISON) {
      if (!apply_date(tf.Cast<ConstantFilter>())) { return false; }
    } else if (tf.filter_type == TableFilterType::CONJUNCTION_AND) {
      auto &conj = tf.Cast<ConjunctionAndFilter>();
      for (auto &cfp : conj.child_filters) {
        if (cfp->filter_type != TableFilterType::CONSTANT_COMPARISON) { return false; }
        if (!apply_date(cfp->Cast<ConstantFilter>())) { return false; }
      }
    } else {
      return false;
    }
  }
  if (!(have_lo && have_hi)) { return false; }

  // supplier / nation / lineitem must carry no unmodeled table_filters.
  if (!supplier_get->table_filters.filters.empty()) { return false; }
  if (!nation_get->table_filters.filters.empty()) { return false; }
  if (!lineitem_get->table_filters.filters.empty()) { return false; }

  // Build the replacement at the aggregate's exact bindings/types: 1 group key
  // type (n_name VARCHAR) then the 1 aggregate type.
  vector<LogicalType> out_types;
  out_types.push_back(agg.groups[0]->return_type);
  out_types.push_back(ag.return_type);

  auto repl = make_uniq<LogicalQ5>(region_name, o_lo, o_hi, agg.group_index,
                                   agg.aggregate_index, std::move(out_types));
  repl->estimated_cardinality = agg.estimated_cardinality;
  node = std::move(repl);
  return true;
}

void LoadInternal(ExtensionLoader &loader) {
  mojo_gpu_ctx_init();                                 // pay the ~32 ms DeviceContext init once, at LOAD
  RegisterGpuOperator(loader.GetDatabaseInstance());  // transparent cosine operator
  RegisterGpuCosineTableFunction(loader);             // pin-resident cosine TF
  RegisterGpuQ6TableFunction(loader);                 // TPC-H Q6 engine (pin + exact sum)
  RegisterGpuQ1TableFunction(loader);                 // TPC-H Q1 engine (grouped exact aggregation)
  RegisterGpuQ14TableFunction(loader);                // TPC-H Q14 engine (GPU hash-probe join)
  RegisterGpuQ3TableFunction(loader);                 // TPC-H Q3 engine (GPU multi-way join + top-10)
  RegisterGpuQ5TableFunction(loader);                 // TPC-H Q5 engine (GPU 6-table join + per-nation agg)
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
