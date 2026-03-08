from duckdb import *

# Define a struct matching the query columns — fields map to columns by position.
@fieldwise_init
struct StationCount(Writable, Copyable, Movable):
    var station: String
    var num_services: Int64

def main() raises:
    var con = DuckDB.connect(":memory:")
    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """)

    var query = """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name", count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """

    # Iterate over rows directly
    for row in con.execute(query):
        print(row.get[String](col=0), " ", row.get[Int64](col=1))

    # Iterate over chunks, then rows within each chunk
    for chunk in con.execute(query).chunks():
        for row in chunk:
            print(row.get[String](col=0), " ", row.get[Int64](col=1))

    # Decode directly into tuples
    for row in con.execute(query):
        var t = row.get_tuple[String, Int64]()
        print(t[0], ": ", t[1])

    # Typed struct access
    var result = con.execute(query).fetchall()
    var stations: List[StationCount] = result.get[StationCount]()
    for i in range(len(stations)):
        print(stations[i])
