import duckdb


# Rows map to struct fields by column name.
@fieldwise_init
struct StationCount(Copyable, Defaultable, Movable, Writable):
    var station: String
    var num_services: Int64

    def __init__(out self):
        self.station = ""
        self.num_services = 0


def main() raises:
    var con = duckdb.connect()
    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """)

    var query = """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name" AS station, count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """

    # Print the result as a table
    con.execute(query).show()

    # Read all rows, then access cells by column and row
    var result = con.execute(query).fetchall()
    for row in result.rows():
        print(row.get[String](0), " ", row.get[Int64](1))

    # Stream chunks and read columns
    for chunk in con.execute(query).chunks():
        var names = chunk.column[String](0)
        for i in range(len(chunk)):
            print(names.get(i).value())

    # Rows as tuples
    for t in con.execute(query).get[Tuple[String, Int64]]():
        print(t[0], ": ", t[1])

    # Rows as structs
    for s in con.execute(query).get[StationCount]():
        print(s)
