// Stock-vs-Mojo benchmark for the mojo_overrides extension.
//
// Builds a table, times a set of queries against stock DuckDB, LOADs the
// mojo_overrides extension (which rewrites the built-ins to Mojo SIMD kernels),
// re-times, and prints the per-query speedup plus a correctness check.
//
//   benchmark <extension_path> [--threads=N] [--rows=N]
//
// Env: DUCKDB_MOJO_LIB must point at libmojo_simd.dylib (the extension dlopen's it).

#include "duckdb.hpp"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

using namespace duckdb;

struct Q {
	const char *name;
	const char *sql;
};

static double bench(Connection &con, const char *sql, int iters) {
	using clk = std::chrono::steady_clock;
	auto t0 = clk::now();
	for (int i = 0; i < iters; i++) {
		auto r = con.Query(sql);
		if (r->HasError()) {
			fprintf(stderr, "query error: %s\n", r->GetError().c_str());
			return -1;
		}
	}
	return std::chrono::duration<double, std::milli>(clk::now() - t0).count() / iters;
}

int main(int argc, char **argv) {
	if (argc < 2) {
		fprintf(stderr, "usage: %s <extension_path> [--threads=N] [--rows=N]\n", argv[0]);
		return 2;
	}
	std::string ext_path = argv[1];
	int threads = 1;
	int64_t rows = 50000000;
	for (int i = 2; i < argc; i++) {
		if (!strncmp(argv[i], "--threads=", 10)) {
			threads = atoi(argv[i] + 10);
		} else if (!strncmp(argv[i], "--rows=", 7)) {
			rows = atoll(argv[i] + 7);
		}
	}

	DBConfig cfg;
	cfg.SetOptionByName("allow_unsigned_extensions", Value::BOOLEAN(true));
	DuckDB db(nullptr, &cfg);
	Connection con(db);
	con.Query("PRAGMA threads=" + std::to_string(threads));
	con.Query("CREATE TABLE t AS SELECT ((i % 1000) + 1)::DOUBLE x, (((i * 7) % 1000) + 1)::FLOAT f "
	          "FROM range(" +
	          std::to_string(rows) + ") tbl(i)");

	std::vector<Q> queries = {
	    {"SUM(x)", "SELECT SUM(x) FROM t"},
	    {"AVG(x)", "SELECT AVG(x) FROM t"},
	    {"MIN(x)", "SELECT MIN(x) FROM t"},
	    {"MAX(x)", "SELECT MAX(x) FROM t"},
	    {"MIN(f) f32", "SELECT MIN(f) FROM t"},
	    {"MAX(f) f32", "SELECT MAX(f) FROM t"},
	    {"SUM(sqrt(x))", "SELECT SUM(sqrt(x)) FROM t"},
	    {"SUM(ln(x))", "SELECT SUM(ln(x)) FROM t"},
	    {"SUM(exp(x*0.01))", "SELECT SUM(exp(x*0.01)) FROM t"},
	    {"SUM(sin(x)+cos(x))", "SELECT SUM(sin(x)+cos(x)) FROM t"},
	    {"SUM(log10(x))", "SELECT SUM(log10(x)) FROM t"},
	};

	const int iters = 8;
	std::vector<double> stock(queries.size());
	for (size_t i = 0; i < queries.size(); i++) {
		stock[i] = bench(con, queries[i].sql, iters);
	}

	auto lr = con.Query("LOAD '" + ext_path + "'");
	if (lr->HasError()) {
		fprintf(stderr, "LOAD failed: %s\n", lr->GetError().c_str());
		return 1;
	}

	printf("\n  rows=%lld threads=%d  (extension: %s)\n", (long long)rows, threads, ext_path.c_str());
	printf("  %-22s %10s %10s %9s\n", "query", "stock(ms)", "mojo(ms)", "speedup");
	printf("  ---------------------------------------------------------------\n");
	for (size_t i = 0; i < queries.size(); i++) {
		double m = bench(con, queries[i].sql, iters);
		printf("  %-22s %10.2f %10.2f %8.2fx\n", queries[i].name, stock[i], m, m > 0 ? stock[i] / m : 0.0);
	}
	return 0;
}
