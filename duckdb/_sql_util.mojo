"""Small SQL-building helpers shared across the client API.

Kept in a dependency-free leaf module so both `connection` and `functions`
can use it.
"""


def _quote_literal(value: String) -> String:
    """Quote ``value`` as a DuckDB single-quoted **string literal** (``'...'``).

    Embedded single quotes are escaped by doubling them, per SQL string-literal
    rules. Used for interpolating values (file paths, filter constants) into
    SQL text.

    Args:
        value: The raw string to quote.

    Returns:
        The value wrapped in single quotes with internal quotes doubled.
    """
    return String("'", value.replace("'", "''"), "'")


def _quote_ident(name: String) -> String:
    """Quote ``name`` as a DuckDB double-quoted **identifier** (``"..."``).

    Embedded double quotes are escaped by doubling them. Used for table,
    view, and column names so that names containing spaces, reserved words,
    or special characters compose into valid SQL.

    Args:
        name: The raw identifier.

    Returns:
        The name wrapped in double quotes with internal quotes doubled.
    """
    return String('"', name.replace('"', '""'), '"')


def _quote_qualified(name: String) -> String:
    """Quote a possibly schema-qualified identifier.

    ``schema.table`` becomes ``"schema"."table"`` so a qualified name resolves
    to the right schema. If there are no dots, or any dot-separated part is
    empty (a leading/trailing dot), the whole name is quoted as a single
    identifier instead of producing an invalid empty identifier.
    """
    if "." not in name:
        return _quote_ident(name)
    var parts = name.split(".")
    for p in parts:
        if p.byte_length() == 0:
            return _quote_ident(name)
    var out = String("")
    var first = True
    for p in parts:
        if not first:
            out += "."
        out += _quote_ident(String(p))
        first = False
    return out^


def _sql_quote(value: String) -> String:
    """Deprecated alias for `_quote_literal` (kept for existing call sites)."""
    return _quote_literal(value)
