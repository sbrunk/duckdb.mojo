"""Small SQL-building helpers shared across the client API.

Kept in a dependency-free leaf module so both `connection` and `functions`
can use it without creating an import cycle.
"""


def _sql_quote(value: String) -> String:
    """Quote ``value`` as a DuckDB single-quoted string literal.

    Embedded single quotes are escaped by doubling them, per SQL string-literal
    rules.  Use for interpolating file paths into ``read_csv('...')`` and
    friends.

    Args:
        value: The raw string to quote.

    Returns:
        The value wrapped in single quotes with internal quotes doubled.
    """
    return String("'", value.replace("'", "''"), "'")
