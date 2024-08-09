from duckdb import *
from testing import *

def test_logical_type():

    var bigint = LogicalType(DuckDBType.bigint)
    var list = bigint.create_list_type()
    var child = list.list_type_child_type()

    assert_equal(bigint, child)
    assert_not_equal(bigint, list)
    assert_not_equal(child, list)
