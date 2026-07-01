"""DuckDB Operator Replacement Extension - Unstable API.

This module provides bindings to the DuckDB operator replacement extension,
which allows replacing any scalar function or operator at query optimization time.

Warning: This uses internal DuckDB APIs and is not part of the stable C API.
"""

from .operator_replacement import OperatorReplacementLib
