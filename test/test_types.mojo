from duckdb import *
from testing import assert_equal, assert_false, assert_raises


def test_types():
    con = DuckDB.connect(":memory:")

    result = con.execute("SELECT true")
    assert_equal(result.fetch_chunk().get(bool_, row=0, col=0).value(), True)

    with assert_raises(contains="Expected type tinyint but got boolean"):
        result = con.execute("SELECT true")
        _ = result.fetch_chunk().get(int8, row=0, col=0).value()

    with assert_raises(contains="Expected type varchar but got boolean"):
        result = con.execute("SELECT true")
        _ = result.fetch_chunk().get(string, row=0, col=0).value()

    result = con.execute("SELECT -42::TINYINT")
    assert_equal(result.fetch_chunk().get(int8, row=0, col=0).value(), -42)

    result = con.execute("SELECT 42::UTINYINT")
    assert_equal(result.fetch_chunk().get(uint8, row=0, col=0).value(), 42)

    result = con.execute("SELECT -42::SMALLINT")
    assert_equal(result.fetch_chunk().get(int16, row=0, col=0).value(), -42)

    result = con.execute("SELECT 42::USMALLINT")
    assert_equal(result.fetch_chunk().get(uint16, row=0, col=0).value(), 42)

    result = con.execute("SELECT -42::INTEGER")
    assert_equal(result.fetch_chunk().get(int32, row=0, col=0).value(), -42)

    result = con.execute("SELECT 42::UINTEGER")
    assert_equal(result.fetch_chunk().get(uint32, row=0, col=0).value(), 42)

    result = con.execute("SELECT -42::BIGINT")
    assert_equal(result.fetch_chunk().get(int64, row=0, col=0).value(), -42)

    result = con.execute("SELECT 42::UBIGINT")
    assert_equal(result.fetch_chunk().get(uint64, row=0, col=0).value(), 42)

    result = con.execute("SELECT 42.0::FLOAT")
    assert_equal(result.fetch_chunk().get(float32, row=0, col=0).value(), 42.0)

    result = con.execute("SELECT 42.0::DOUBLE")
    assert_equal(result.fetch_chunk().get(float64, row=0, col=0).value(), 42.0)

    result = con.execute("SELECT TIMESTAMP '1992-09-20 11:30:00.123456789'")
    assert_equal(
        result.fetch_chunk().get(timestamp, row=0, col=0).value(),
        Timestamp(
            716988600123456
        ),  # SELECT epoch_us(TIMESTAMP '1992-09-20 11:30:00.123456789');
    )

    result = con.execute("SELECT DATE '1992-09-20'")
    assert_equal(result.fetch_chunk().get(date, row=0, col=0).value(), Date(8298))

    result = con.execute("SELECT TIME '1992-09-20 11:30:00.123456'")
    assert_equal(
        result.fetch_chunk().get(time, row=0, col=0).value(),
        Time(41400123456),  # SELECT epoch_us(TIME '11:30:00.123456');
    )

    result = con.execute("SELECT 'hello'")
    assert_equal(result.fetch_chunk().get(string, row=0, col=0).value(), "hello")

    result = con.execute("SELECT 'hello longer string'")
    assert_equal(
        result.fetch_chunk().get(string, row=0, col=0).value(), "hello longer string"
    )

def test_list():
    con = DuckDB.connect(":memory:")

    # A list of int
    result = con.execute("SELECT unnest([[1, 2, 3], [4, 5, 6]])")
    chunk = result.fetch_chunk()
    lists = chunk.get(list(int32), col=0)
    assert_equal(len(lists), 2)

    for row_idx in range(2):
        var l = lists[row_idx].value()
        assert_equal(len(l), 3)
        for list_idx in range(3):
            var list_value = l[list_idx].value()
            assert_equal(list_value, row_idx * 3 + list_idx + 1)

    # A list with nulls
    result = con.execute("SELECT [1, null, 3]")
    chunk = result.fetch_chunk()
    list_with_nulls = chunk.get(list(int32), col=0)[0].value()
    assert_equal(len(list_with_nulls), 3)

    assert_equal(list_with_nulls[0].value(), 1)
    assert_false(list_with_nulls[1])  # NULL gives us an empty optional
    assert_equal(list_with_nulls[2].value(), 3)

    # A list of lists of int
    result = con.execute("SELECT unnest([[[1, 2], [3, 4]], [[5, 6]]])")
    chunk = result.fetch_chunk()
    nested_lists = chunk.get(list(list(int32)), col=0)

    assert_equal(len(nested_lists), 2)
    assert_equal(len(nested_lists[0].value()), 2)
    assert_equal(len(nested_lists[0].value()[0].value()), 2)
    assert_equal(len(nested_lists[0].value()[1].value()), 2)
    assert_equal(len(nested_lists[1].value()), 1)
    assert_equal(len(nested_lists[1].value()[0].value()), 2)

    for row_idx in range(len(nested_lists)):
        print("row ", row_idx)
        sublists = nested_lists[row_idx].value()
        print("len sublists ", len(sublists))
        for list_idx in range(len(sublists)):
            print("list ", list_idx)
            sublist = sublists[list_idx].value()
            print("len sublist ", len(sublist))
            assert_equal(len(sublist), 2)
            for elem_idx in range(len(sublist)):
                list_value = sublist[elem_idx].value()
                print("val ", list_value)
                assert_equal(
                    list_value, row_idx * 4 + list_idx * 2 + elem_idx + 1
                )

    # A list of strings
    result = con.execute(
        "SELECT unnest([['a', 'b'], ['cdefghijklmnopqrstuvwxyz']])"
    )
    chunk = result.fetch_chunk()
    string_lists = chunk.get(list(string), col=0)
    assert_equal(len(string_lists), 2)

    assert_equal(string_lists[0].value()[0].value(), "a")
    assert_equal(string_lists[0].value()[1].value(), "b")

    assert_equal(
        string_lists[1].value()[0].value(),
        "cdefghijklmnopqrstuvwxyz",
    )

    ## TODO test remaining types


def test_null():
    con = DuckDB.connect(":memory:")
    result = con.execute("SELECT null")
    assert_false(result.fetch_chunk().get(int32, row=0, col=0))
