from duckdb import *
from testing import *


def test_connection():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT 42")
    assert_equal(
        result.fetch_chunk().get[Int32Val](col=0, row=0).value().value, 42
    )


## TODO figure out why this works in general but not as test
## Error when running as test:
## error: Execution was interrupted, reason: internal c++ exception breakpoint(-6)..
# def test_failure():
#     con = DuckDB.connect(":memory:")
#     with assert_raises():
#         _ = con.execute("invalid statement")
