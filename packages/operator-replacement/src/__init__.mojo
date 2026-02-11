"""DuckDB Operator Replacement Extension - Unstable API.

This module provides bindings to the DuckDB operator replacement extension,
which allows replacing any scalar function or operator at query optimization time.

Warning: This uses internal DuckDB APIs and is not part of the stable C API.
"""

from ._liboperator_replacement import (
    register_function_replacement,
    register_operator_replacement,
)
