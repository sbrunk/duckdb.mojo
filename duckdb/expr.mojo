"""Safe SQL-fragment helpers for building queries and `Relation` transforms.

These return plain ``String`` fragments. They centralize the two kinds of
quoting (identifiers vs literals) so composed SQL is correct and injection-safe
by default:

```mojo
from duckdb import col, lit, fn_, star

con.sql("FROM people").filter(col("name") + " = " + lit("O'Brien"))
con.table("orders").select(star(), fn_("upper", col("status")))
```

Use `lit` for *values* (it quotes/escapes) and `col` for *names* (it
double-quotes identifiers).  Numbers and booleans render without quotes.
"""

from duckdb._sql_util import _quote_ident, _quote_literal, _quote_qualified


def col(name: String) -> String:
    """Reference a column as a quoted identifier (``"name"``).

    Dotted names (``"t.col"``) are split and each part quoted, yielding
    ``"t"."col"`` so qualified references compose correctly. A name with a
    leading/trailing dot (or no dots) is quoted as a single identifier.

    Args:
        name: The (optionally dotted) column name.

    Returns:
        The quoted identifier fragment.
    """
    return _quote_qualified(name)


def lit(value: String) -> String:
    """A SQL string literal (``'value'``), single quotes escaped."""
    return _quote_literal(value)


def lit(value: Bool) -> String:
    """A SQL boolean literal (``true`` / ``false``)."""
    return String("true") if value else String("false")


def lit(value: Int) -> String:
    """A SQL integer literal."""
    return String(value)


def lit(value: Float64) -> String:
    """A SQL double literal."""
    return String(value)


def sql_null() -> String:
    """The SQL ``NULL`` literal."""
    return String("NULL")


def fn_(name: String) -> String:
    """A zero-argument SQL function call (``name()``)."""
    return String(name, "()")


def fn_(name: String, *args: String) -> String:
    """A SQL function call ``name(arg0, arg1, ...)``.

    Arguments are inserted verbatim. Wrap values with `lit` and names with
    `col` as appropriate.

    Args:
        name: The function name (inserted verbatim, not quoted).
        args: Already-formatted SQL fragments for each argument.
    """
    var out = String(name, "(")
    for i in range(len(args)):
        if i > 0:
            out += ", "
        out += args[i]
    out += ")"
    return out^


def star() -> String:
    """The ``*`` projection."""
    return String("*")


def star(*, exclude: List[String]) -> String:
    """``* EXCLUDE (a, b, ...)``: all columns except the named ones."""
    if len(exclude) == 0:
        return String("*")
    var out = String("* EXCLUDE (")
    for i in range(len(exclude)):
        if i > 0:
            out += ", "
        out += _quote_ident(exclude[i])
    out += ")"
    return out^
