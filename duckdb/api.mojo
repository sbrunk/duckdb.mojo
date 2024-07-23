struct DuckDB:
    @staticmethod
    fn connect(db_path: String) raises -> Connection:
        return Connection(db_path)
