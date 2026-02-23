from duckdb import *


# Define a struct matching the query columns — fields map to columns by position.
@fieldwise_init
struct StationCount(Writable, Copyable, Movable):
    var station: String
    var num_services: Int64

    fn write_to[W: Writer](self, mut writer: W):
        writer.write(self.station, ": ", self.num_services)


def main():
    var con = DuckDB.connect(":memory:")

    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

    CREATE TABLE train_services AS
    FROM 'https://blobs.duckdb.org/nl-railway/services-2025-03.csv.gz';
    """
    )

    var result = con.execute(
        """
    -- Get the top-3 busiest train stations
    SELECT "Stop:Station name", count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """
    )
    var chunk = result.fetch_chunk()

    # --- Per-column typed access ---
    print("Per-column access:")
    for row in range(len(chunk)):
        print(
            chunk.get[String](col=0, row=row),
            " ",
            chunk.get[Int64](col=1, row=row),
        )

    # --- Typed struct access — deserialize whole rows at once ---
    var stations = chunk.get[StationCount]()

    print("\nStruct access:")
    for i in range(len(stations)):
        print(stations[i])
