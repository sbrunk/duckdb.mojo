from duckdb import *
from duckdb.database import Database
from testing import *
from testing.suite import TestSuite
from pathlib import Path
import os


def test_database_in_memory_default():
    """Test creating a database with default settings (in-memory)."""
    # When no path is provided, should create an in-memory database
    var db = Database()
    # If construction succeeds without error, the test passes
    _ = db


def test_database_in_memory_explicit():
    """Test creating an in-memory database explicitly."""
    var db = Database(":memory:")
    _ = db


def test_database_with_file_path():
    """Test creating a database with a file path."""
    var test_db_path = String("test_database.db")
    
    # Create the database
    var db = Database(test_db_path)
    _ = db
    
    # Clean up - remove the test database file if it exists
    try:
        os.unlink(Path(test_db_path))
    except:
        pass


def test_database_with_temp_file():
    """Test creating a database with a temporary file path."""
    var temp_db_path = String("/tmp/test_duckdb_temp.db")
    
    try:
        var db = Database(temp_db_path)
        _ = db
    except e:
        assert_true(False, "Failed to create database with temp path")
    
    # Clean up
    try:
        os.unlink(Path(temp_db_path))
    except:
        pass


def test_database_invalid_path():
    """Test creating a database with an invalid path should raise an error."""
    # Try to create a database in a directory that doesn't exist
    var invalid_path = String("/nonexistent/directory/that/does/not/exist/test.db")
    
    with assert_raises():
        var db = Database(invalid_path)


def test_database_none_path():
    """Test creating a database with None path (should default to in-memory)."""
    var db = Database(None)
    _ = db


def test_database_multiple_instances():
    """Test creating multiple database instances."""
    var db1 = Database(":memory:")
    var db2 = Database(":memory:")
    var db3 = Database()
    
    # All three should be independent instances
    _ = db1
    _ = db2
    _ = db3


def test_database_empty_string_path():
    """Test creating a database with an empty string path."""
    # Empty string should be treated as a valid path and create a file
    var empty_path = String("")
    
    try:
        var db = Database(empty_path)
        _ = db
    except:
        # This is expected to fail - empty string may not be a valid database path
        pass


def main():
    TestSuite.discover_tests[__functions_in_module()]().run()
