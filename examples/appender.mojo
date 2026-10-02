"""Bulk-load rows with the Appender.

    pixi run mojo run examples/appender.mojo
"""

from duckdb import *


@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String


def main() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")

    var appender = Appender(con, "people")
    appender.append_row(Person(1, "Mark"))
    appender.append_row(Person(2, "Hannes"))
    appender.append_rows([Person(3, "Pedro"), Person(4, "Laurens")])
    appender.close()

    con.execute("SELECT * FROM people ORDER BY id").show()
