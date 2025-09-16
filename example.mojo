from duckdb import *

def main():
    var con = DuckDB.connect(":memory:")

    _ = con.execute("""
    SET autoinstall_known_extensions=1;
    SET autoload_known_extensions=1;

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
    ).fetch_all()

    for col in result.columns():
        print(col)

    print()
    print("Length: " + String(len(result)))
    print()

    for row in range(len(result)):
        print(
            result.get(varchar, col=0, row=row).value(),
            " ",
            result.get(bigint, col=1, row=row).value(),
        )
