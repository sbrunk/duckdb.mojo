// Probe: does a scanned FLOAT[K] array column's data memory flow
// through DBConfig.allocator?
//
// This gates the whole "DuckDB buffers ARE unified memory" idea.
// We open a DB with a custom logging Allocator that records every live range it
// hands out, register a vectorized UDF `probe(v FLOAT[K]) -> BIGINT` that returns
// 1 when the input array column's child data pointer falls inside one of those
// ranges (0 otherwise), and aggregate over a storage-backed table.
//
//   in-allocator rows == total rows  -> column data IS our memory  (allocator route viable)
//   in-allocator rows ~ 0            -> column comes from BufferManager blocks that
//                                       bypass DBConfig.allocator   -> use pin-resident route
//
// Build/run: see bench/README or `pixi run gpu-op-probe`.

#include "duckdb.hpp"
#include "duckdb/common/allocator.hpp"
#include "duckdb/common/types/vector.hpp"

#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <map>
#include <mutex>

using namespace duckdb;

// ---------------------------------------------------------------------------
// Logging allocator: malloc-backed, records [ptr, ptr+size) live ranges.
// ---------------------------------------------------------------------------
namespace {

struct LogAllocData : public PrivateAllocatorData {
  std::mutex mu;
  std::map<uintptr_t, idx_t> live;  // start -> size
  std::atomic<uint64_t> total_bytes{0};
  std::atomic<uint64_t> n_allocs{0};

  void add(data_ptr_t p, idx_t size) {
    std::lock_guard<std::mutex> g(mu);
    live[reinterpret_cast<uintptr_t>(p)] = size;
    total_bytes += size;
    n_allocs++;
  }
  void remove(data_ptr_t p) {
    if (!p) return;
    std::lock_guard<std::mutex> g(mu);
    live.erase(reinterpret_cast<uintptr_t>(p));
  }
  // Is `p` inside any currently-live allocation?
  bool contains(const void *p) {
    auto addr = reinterpret_cast<uintptr_t>(p);
    std::lock_guard<std::mutex> g(mu);
    auto it = live.upper_bound(addr);  // first start > addr
    if (it == live.begin()) return false;
    --it;                              // greatest start <= addr
    return addr < it->first + it->second;
  }
};

// global so the UDF can reach it (the Allocator owns the PrivateAllocatorData,
// but we keep a raw pointer for the probe lookup).
LogAllocData *g_log = nullptr;

data_ptr_t log_allocate(PrivateAllocatorData *pd, idx_t size) {
  auto p = reinterpret_cast<data_ptr_t>(malloc(size));
  pd->Cast<LogAllocData>().add(p, size);
  return p;
}
void log_free(PrivateAllocatorData *pd, data_ptr_t p, idx_t size) {
  pd->Cast<LogAllocData>().remove(p);
  free(p);
}
data_ptr_t log_reallocate(PrivateAllocatorData *pd, data_ptr_t p, idx_t old_size, idx_t size) {
  auto &d = pd->Cast<LogAllocData>();
  d.remove(p);
  auto np = reinterpret_cast<data_ptr_t>(realloc(p, size));
  d.add(np, size);
  return np;
}

// ---------------------------------------------------------------------------
// probe(v FLOAT[K]) -> BIGINT : 1 if the array child data ptr is in our allocator
// ---------------------------------------------------------------------------
void probe_function(DataChunk &args, ExpressionState &, Vector &result) {
  auto count = args.size();
  auto &arr = args.data[0];                  // ARRAY(FLOAT, K)
  auto &child = ArrayVector::GetEntry(arr);  // FLOAT child vector
  const float *child_ptr = FlatVector::GetData<float>(child);
  bool in_alloc = g_log && g_log->contains(child_ptr);

  result.SetVectorType(VectorType::FLAT_VECTOR);
  auto out = FlatVector::GetData<int64_t>(result);
  for (idx_t i = 0; i < count; i++) out[i] = in_alloc ? 1 : 0;
}

}  // namespace

int main() {
  const int K = 1024;
  const int ROWS = 200000;

  DBConfig config;
  auto priv = make_uniq<LogAllocData>();
  g_log = priv.get();
  config.allocator =
      make_uniq<Allocator>(log_allocate, log_free, log_reallocate, std::move(priv));

  // Persist to disk so the scan goes through the real storage/BufferManager path
  // (an in-memory table would not exercise block I/O).
  std::remove("/tmp/alloc_probe.db");
  DuckDB db("/tmp/alloc_probe.db", &config);
  Connection con(db);

  printf("== building emb(v FLOAT[%d]), %d rows ==\n", K, ROWS);
  con.Query("CREATE TABLE emb(id BIGINT, v FLOAT[" + std::to_string(K) + "])");
  // Fill with a generated array per row.
  auto fill = con.Query(
      "INSERT INTO emb SELECT i, "
      "(SELECT array_agg(CAST((i*7+g)%97 AS FLOAT)) FROM range(" + std::to_string(K) + ") t(g)) "
      "FROM range(" + std::to_string(ROWS) + ") s(i)");
  if (fill->HasError()) { printf("insert error: %s\n", fill->GetError().c_str()); return 1; }
  con.Query("CHECKPOINT");

  con.CreateVectorizedFunction("probe", {LogicalType::ARRAY(LogicalType::FLOAT, K)},
                               LogicalType::BIGINT, probe_function);

  uint64_t bytes_before = g_log->total_bytes.load();
  uint64_t allocs_before = g_log->n_allocs.load();

  printf("== scanning: SELECT sum(probe(v)), count(*) FROM emb ==\n");
  auto r = con.Query("SELECT sum(probe(v)), count(*) FROM emb");
  if (r->HasError()) { printf("query error: %s\n", r->GetError().c_str()); return 1; }

  auto chunk = r->Fetch();
  int64_t in_alloc = chunk->GetValue(0, 0).GetValue<int64_t>();
  int64_t total = chunk->GetValue(1, 0).GetValue<int64_t>();

  uint64_t bytes_during = g_log->total_bytes.load() - bytes_before;
  uint64_t allocs_during = g_log->n_allocs.load() - allocs_before;
  double col_bytes = double(ROWS) * K * sizeof(float);

  printf("\n--- RESULT ---\n");
  printf("rows whose array data is IN our allocator: %lld / %lld  (%.1f%%)\n",
         (long long)in_alloc, (long long)total, total ? 100.0 * in_alloc / total : 0.0);
  printf("allocator activity during scan: %llu allocs, %.1f MB\n",
         (unsigned long long)allocs_during, bytes_during / 1e6);
  printf("(full column is ~%.1f MB)\n", col_bytes / 1e6);
  printf("\nGATE: %s\n",
         in_alloc == total
             ? "PASS - scanned column memory flows through DBConfig.allocator -> allocator route viable"
             : "FAIL - column bypasses DBConfig.allocator -> use Sirius-style pin-resident route");
  return 0;
}
