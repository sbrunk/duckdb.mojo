from duckdb import *
from testing import *
from testing.suite import TestSuite


def test_logical_type():
    var bigint = LogicalType(DuckDBType.bigint)
    var list = bigint.create_list_type()
    var child = list.list_type_child_type()

    # Compare by type_id since child is borrowed and bigint is owned
    assert_equal(bigint.get_type_id(), child.get_type_id())
    assert_not_equal(bigint.get_type_id(), list.get_type_id())
    assert_not_equal(child.get_type_id(), list.get_type_id())


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
