from duckdb import *


def main():
    var con = DuckDB.connect(":memory:")

    _ = con.execute(
        """
    CREATE TABLE train_services AS
    FROM 's3://duckdb-blobs/train_services.parquet';
    """
    )

    var result = con.execute(
        """
    -- Get the top-3 busiest train stations
    SELECT station_name, count(*) AS num_services
    FROM train_services
    GROUP BY ALL
    ORDER BY num_services DESC
    LIMIT 3;
    """
    )

    for i in range(result.column_count()):
        print(result.column_name(i), end=" ")
    print()

    for chunk in result.chunk_iterator():
        for i in range(len(chunk)):
            print(
                chunk.get_string(col=0, row=i).value(),
                " ",
                chunk.get_int64(col=1, row=i).value(),
            )
