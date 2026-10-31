"""Bulk-load rows with the Appender.

    pixi run mojo run examples/appender.mojo
"""

import duckdb
from duckdb import Appender


# Struct fields map to table columns by name.
@fieldwise_init
struct Person(Copyable, Movable):
    var id: Int32
    var name: String


def main() raises:
    var con = duckdb.connect()
    _ = con.execute("CREATE TABLE people (id INTEGER, name VARCHAR)")

    var appender = Appender(con, "people")
    appender.append_row(Person(1, "Mark"))
    appender.append_row(Person(2, "Hannes"))
    # Tuples map to columns by position.
    appender.append_row((Int32(3), String("Pedro")))
    appender.close()

    con.execute("SELECT * FROM people ORDER BY id").show()
