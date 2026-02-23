"""DuckDB startup configuration."""

from duckdb._libduckdb import *
from duckdb.api import DuckDB


struct Config(Movable):
    """A `Config` holds startup options that are passed to `duckdb_open_ext`
    when creating a `Database`.  All values are strings, matching the C API
    convention.

    Create a `Config`, call `set()` for each option, then pass it to
    ``Database()`` or ``DuckDB.connect()``.  DuckDB copies the config
    internally, so the ``Config`` can be reused or will be cleaned up
    normally when it goes out of scope.

    Example:
    ```mojo
    from duckdb import DuckDB, Config

    # Using the Config struct directly:
    var config = Config()
    config.set("threads", "4")
    config.set("memory_limit", "8GB")
    var con = DuckDB.connect(":memory:", config)

    # Or using a Dict for convenience:
    var options = {"threads": "4", "memory_limit": "8GB"}
    var con2 = DuckDB.connect(":memory:", Config(options))

    # Or inline:
    var con3 = DuckDB.connect(
        ":memory:",
        config={"threads": "4", "memory_limit": "8GB"},
    )
    ```
    """

    var _config: duckdb_config

    fn __init__(out self) raises:
        """Create an empty configuration."""
        self._config = duckdb_config()
        ref libduckdb = DuckDB().libduckdb()
        if libduckdb.duckdb_create_config(UnsafePointer(to=self._config)) == DuckDBError:
            raise Error("Failed to create DuckDB config")

    fn __init__(out self, options: Dict[String, String]) raises:
        """Create a configuration from a dictionary of option name/value pairs.

        Args:
            options: A dictionary mapping config option names to their values.
                     All values are strings (e.g., ``"threads": "4"``).
        """
        self = Config()
        for entry in options.items():
            self.set(entry.key, entry.value)

    fn __moveinit__(out self, deinit take: Self):
        self._config = take._config

    fn __del__(deinit self):
        ref libduckdb = DuckDB().libduckdb()
        libduckdb.duckdb_destroy_config(UnsafePointer(to=self._config))

    fn set(mut self, name: String, value: String) raises:
        """Set a configuration option.

        Args:
            name:  The option name (e.g. ``"threads"``, ``"memory_limit"``).
            value: The option value as a string (e.g. ``"4"``, ``"8GB"``).

        Raises:
            Error: If the option name is unknown or the value is invalid.
        """
        ref libduckdb = DuckDB().libduckdb()
        var _name = name
        var _value = value
        if (
            libduckdb.duckdb_set_config(
                self._config,
                _name.as_c_string_slice().unsafe_ptr(),
                _value.as_c_string_slice().unsafe_ptr(),
            )
        ) == DuckDBError:
            raise Error(
                "Invalid configuration: could not set '"
                + name
                + "' to '"
                + value
                + "'"
            )

    @staticmethod
    fn available_options() raises -> Dict[String, String]:
        """Return all available configuration options as ``{name: description}``.

        This queries the DuckDB library for every known startup flag.

        Example:
        ```mojo
        from duckdb import Config
        for entry in Config.available_options().items():
            print(entry.key, "-", entry.value)
        ```
        """
        ref libduckdb = DuckDB().libduckdb()
        var count = libduckdb.duckdb_config_count()
        var result = Dict[String, String]()
        for i in range(count):
            var name_ptr = UnsafePointer[c_char, ImmutAnyOrigin]()
            var desc_ptr = UnsafePointer[c_char, ImmutAnyOrigin]()
            if (
                libduckdb.duckdb_get_config_flag(
                    UInt(i),
                    UnsafePointer(to=name_ptr),
                    UnsafePointer(to=desc_ptr),
                )
            ) == DuckDBSuccess:
                var name = String(unsafe_from_utf8_ptr=name_ptr)
                var desc = String(unsafe_from_utf8_ptr=desc_ptr)
                result[name] = desc
        return result^

    fn _handle(self) -> duckdb_config:
        """Return the underlying duckdb_config handle.

        The Config retains ownership — the handle is only borrowed.
        DuckDB copies its contents during ``duckdb_open_ext``.
        """
        return self._config
