from duckdb import *
from duckdb.config import Config
from duckdb.database import Database
from std.testing import *
from std.testing.suite import TestSuite


def test_config_create() raises:
    """Test creating an empty configuration."""
    var config = Config()
    _ = config


def test_config_set_option() raises:
    """Test setting a configuration option."""
    var config = Config()
    config.set("threads", "2")


def test_config_set_multiple_options() raises:
    """Test setting multiple configuration options."""
    var config = Config()
    config.set("threads", "2")
    config.set("memory_limit", "256MB")


def test_config_set_invalid_value() raises:
    """Test that setting an invalid value for a valid option raises an error."""
    var config = Config()
    # access_mode only accepts specific values
    with assert_raises(contains="Invalid configuration"):
        config.set("access_mode", "INVALID_MODE")


def test_config_from_dict() raises:
    """Test creating a Config from a dictionary."""
    var config = Config({"threads": "2", "memory_limit": "256MB"})


def test_config_available_options() raises:
    """Test listing available configuration options."""
    var options = Config.available_options()
    # DuckDB should have a substantial number of config options
    assert_true(len(options) > 10, "Expected more than 10 config options")
    # Check some well-known options exist
    assert_true("threads" in options, "Expected 'threads' in config options")
    assert_true(
        "memory_limit" in options, "Expected 'memory_limit' in config options"
    )
    assert_true(
        "access_mode" in options, "Expected 'access_mode' in config options"
    )


def test_database_with_config() raises:
    """Test creating a database with a Config."""
    var config = Config()
    config.set("threads", "2")
    var db = Database(":memory:", config)
    _ = db


def test_connection_with_config() raises:
    """Test creating a Connection with a Config."""
    var config = Config()
    config.set("threads", "2")
    config.set("memory_limit", "256MB")
    var con = Connection(":memory:", config^)
    # Verify the config took effect by querying current_setting
    var result = con.execute("SELECT current_setting('threads')::VARCHAR AS threads")
    var chunk = result.fetch_chunk()
    var threads_val = chunk.get[String](col=0, row=0)
    assert_equal(threads_val, "2")


def test_connect_with_config() raises:
    """Test DuckDB.connect with a Config object."""
    var config = Config()
    config.set("threads", "1")
    var con = DuckDB.connect(":memory:", config)
    var result = con.execute("SELECT current_setting('threads')::VARCHAR AS threads")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "1")


def test_connect_with_dict() raises:
    """Test DuckDB.connect with a dictionary config."""
    var con = DuckDB.connect(
        ":memory:", config={"threads": "2", "memory_limit": "256MB"}
    )
    var result = con.execute("SELECT current_setting('threads')::VARCHAR AS threads")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "2")


def test_config_memory_limit() raises:
    """Test configuring the memory limit."""
    var config = Config()
    config.set("memory_limit", "100MB")
    var con = Connection(":memory:", config)
    var result = con.execute(
        "SELECT current_setting('memory_limit') AS memory_limit"
    )
    var chunk = result.fetch_chunk()
    # DuckDB normalizes memory values, so check it's set (exact format may vary)
    var mem_val = chunk.get[String](col=0, row=0)
    assert_true(len(mem_val) > 0, "memory_limit should be set")


def test_config_access_mode_read_write() raises:
    """Test configuring access mode to read-write."""
    var config = Config()
    config.set("access_mode", "READ_WRITE")
    var con = Connection(":memory:", config)
    # Verify we can write
    _ = con.execute("CREATE TABLE test_rw (x INTEGER)")
    _ = con.execute("INSERT INTO test_rw VALUES (42)")
    var result = con.execute("SELECT x FROM test_rw")
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[Int32](col=0, row=0), 42)


def test_config_default_order() raises:
    """Test configuring default order."""
    var config = Config()
    config.set("default_order", "DESCENDING")
    var con = Connection(":memory:", config)
    var result = con.execute(
        "SELECT current_setting('default_order') AS default_order"
    )
    var chunk = result.fetch_chunk()
    assert_equal(chunk.get[String](col=0, row=0), "DESC")


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
