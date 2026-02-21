"""Compile-time API level for DuckDB function access gating.

When running as a DuckDB extension, only a subset of the C API may be
available depending on whether the extension was loaded with the stable or
unstable API struct. ``ApiLevel`` encodes this at compile time so that
calling an unstable function from a stable-only extension is a compile
error rather than a runtime crash.

Three levels are defined:

- ``CLIENT`` — standalone client binary; full API via dlopen/dlsym.
- ``EXT_STABLE`` — extension loaded with the stable API struct; unstable
  functions are **not** available.
- ``EXT_UNSTABLE`` — extension loaded with the unstable API struct; full
  API available.
"""


@fieldwise_init
struct ApiLevel(Equatable, Writable, Stringable, ImplicitlyCopyable):
    """Compile-time tag indicating which DuckDB C API surface is available.

    Used as a parameter on ``Connection``, ``Vector``, ``ScalarFunction``,
    etc. to gate access to unstable functions at compile time.

    The default value is ``CLIENT`` (full access), so standalone / client
    code is completely unaffected.
    """

    var _value: UInt8

    comptime CLIENT = ApiLevel(0)
    """Full client mode — all functions available (dlopen/dlsym)."""

    comptime EXT_STABLE = ApiLevel(1)
    """Extension with stable API only — unstable functions are blocked."""

    comptime EXT_UNSTABLE = ApiLevel(2)
    """Extension with unstable API — all functions available."""

    @always_inline
    fn includes_unstable(self) -> Bool:
        """Returns True when unstable C API functions are available.

        True for ``CLIENT`` and ``EXT_UNSTABLE``, False for ``EXT_STABLE``.
        """
        return self._value != 1

    # ── Equatable ───────────────────────────────────────────────────────

    fn __eq__(self, other: Self) -> Bool:
        return self._value == other._value

    fn __ne__(self, other: Self) -> Bool:
        return self._value != other._value

    # ── Writable / Stringable ───────────────────────────────────────────

    @always_inline("nodebug")
    fn __str__(self) -> String:
        return String.write(self)

    fn write_to[W: Writer](self, mut writer: W):
        if self == Self.CLIENT:
            writer.write("CLIENT")
        elif self == Self.EXT_STABLE:
            writer.write("EXT_STABLE")
        elif self == Self.EXT_UNSTABLE:
            writer.write("EXT_UNSTABLE")
        else:
            writer.write("ApiLevel(", self._value, ")")
