"""Q1-shaped DENSE grouped + selective-filter phantom-group probe.

The generic engine only ROUTES a DENSE_GROUP int128 aggregate when the descriptor
classifies KIND_Q1 (exactly 2 VARCHAR fact group keys + 8 aggregates, descriptor.mojo
~699). A 1-key/1-agg grouped sum stays KIND_UNKNOWN and declines to stock. So to
exercise the DENSE assembly path we mimic TPC-H Q1's shape: group by
(l_returnflag, l_linestatus) with 8 aggregates and a `WHERE l_shipdate <= D` filter.

We arrange the data so the group ('N','O') has l_shipdate AFTER the filter cutoff for
EVERY row -- the filter fully excludes it. Stock omits ('N','O'); if the operator
emits a phantom ('N','O') row (all-zero aggregates), that is a wrong result.

All columns are NOT NULL so the nullable gate is irrelevant.

Run: pixi run mojo run -I packages/mojo-gpu-operator/src \
        packages/mojo-gpu-operator/bench/q1_dense_filter_phantom_probe.mojo
"""

from duckdb import DuckDB
from duckdb.config import Config
from std.os import getenv, setenv
from std.sys import has_accelerator


comptime DDL = (
    "CREATE TABLE lineitem("
    " l_returnflag VARCHAR NOT NULL,"
    " l_linestatus VARCHAR NOT NULL,"
    " l_quantity DECIMAL(15,2) NOT NULL,"
    " l_extendedprice DECIMAL(15,2) NOT NULL,"
    " l_discount DECIMAL(15,2) NOT NULL,"
    " l_tax DECIMAL(15,2) NOT NULL,"
    " l_shipdate DATE NOT NULL)"
)

# Three (flag,status) groups:
#   ('A','F'): shipdate 1994 (kept by <= 1998-09-02)
#   ('R','F'): shipdate 1995 (kept)
#   ('N','O'): shipdate 1999 (EXCLUDED by the cutoff) -> fully filtered group
comptime DML = (
    "INSERT INTO lineitem SELECT"
    " CASE WHEN i%3=0 THEN 'A' WHEN i%3=1 THEN 'R' ELSE 'N' END,"
    " CASE WHEN i%3=2 THEN 'O' ELSE 'F' END,"
    " (1 + i%50)::DECIMAL(15,2),"
    " (100 + i%900)::DECIMAL(15,2),"
    " ((i%10)::DECIMAL(15,2))/100,"
    " ((i%8)::DECIMAL(15,2))/100,"
    " CASE WHEN i%3=2 THEN DATE '1999-01-01'"
    "      WHEN i%3=1 THEN DATE '1995-06-01'"
    "      ELSE DATE '1994-03-01' END"
    " FROM range(6000) r(i)"
)


comptime Q1 = (
    "SELECT l_returnflag, l_linestatus,"
    " sum(l_quantity) AS sum_qty,"
    " sum(l_extendedprice) AS sum_base_price,"
    " sum(l_extendedprice*(1-l_discount)) AS sum_disc_price,"
    " sum(l_extendedprice*(1-l_discount)*(1+l_tax)) AS sum_charge,"
    " avg(l_quantity) AS avg_qty,"
    " avg(l_extendedprice) AS avg_price,"
    " avg(l_discount) AS avg_disc,"
    " count(*) AS count_order"
    " FROM lineitem"
    " WHERE l_shipdate <= DATE '1998-09-02'"
    " GROUP BY l_returnflag, l_linestatus"
    " ORDER BY l_returnflag, l_linestatus"
)


def _run(label: String) raises -> List[String]:
    var ext = getenv(
        "GPU_OP_EXT",
        "packages/mojo-gpu-operator/build/mojo_gpu_operator.duckdb_extension",
    )
    var config = Config({"allow_unsigned_extensions": "true"})
    var con = DuckDB.connect(":memory:", config^)
    _ = con.execute("LOAD '" + ext + "'")
    _ = con.execute(DDL)
    _ = con.execute(DML)
    var res = con.execute(Q1)
    var out: List[String] = []
    var chunk = res.fetch_chunk()
    var nrows = Int(chunk.__len__())
    for r in range(nrows):
        var f = chunk.get[Optional[String]](col=0, row=r)
        var s = chunk.get[Optional[String]](col=1, row=r)
        var cnt = chunk.get[Optional[Int64]](col=9, row=r)
        var fs = f.value() if Bool(f) else String("<NULL>")
        var ss = s.value() if Bool(s) else String("<NULL>")
        var cs = String(cnt.value()) if Bool(cnt) else String("<NULL>")
        out.append("(" + fs + "," + ss + ") count_order=" + cs)
    print("[" + label + "] rows =", nrows)
    for r in range(len(out)):
        print("   ", out[r])
    return out^


def main() raises:
    if not has_accelerator():
        print("no GPU accelerator -> skipping")
        return

    print("=== OPERATOR (default) ===")
    var op = _run("operator")

    print("\n=== STOCK (GPU_OP_GENERIC=off) ===")
    _ = setenv("GPU_OP_GENERIC", "off")
    var stock = _run("stock")

    print("\n=== VERDICT ===")
    print("operator rows =", len(op), " stock rows =", len(stock))
    if len(op) != len(stock):
        print(
            "MISMATCH: operator emits a different row count -> PHANTOM"
            " fully-filtered group."
        )
    else:
        var same = True
        for r in range(len(op)):
            if op[r] != stock[r]:
                same = False
        print("MATCH" if same else "MISMATCH (same count, diff values)")
