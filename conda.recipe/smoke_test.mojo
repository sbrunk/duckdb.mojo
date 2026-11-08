import duckdb
from std.testing import assert_equal


def main() raises:
    # Offline smoke test for the packaged bindings: open an in-memory database,
    # run a trivial query, and verify the result. Exercises the dlopen of
    # libduckdb (the runtime dependency) without touching the network.
    var con = duckdb.connect()
    assert_equal(con.execute("SELECT 40 + 2 AS answer").get[Int32]()[0], 42)
