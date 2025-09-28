cmake_minimum_required(VERSION 3.20)
project(duckdb_mojo_helpers VERSION 0.0.1 LANGUAGES C)

# Find DuckDB directly (conda packages don't provide .pc files)
find_path(DUCKDB_INCLUDE_DIR duckdb.h
    HINTS ${CMAKE_PREFIX_PATH}
    PATH_SUFFIXES include
)

find_library(DUCKDB_LIBRARY
    NAMES duckdb libduckdb
    HINTS ${CMAKE_PREFIX_PATH}
    PATH_SUFFIXES lib
)

if(NOT DUCKDB_INCLUDE_DIR OR NOT DUCKDB_LIBRARY)
    message(FATAL_ERROR "DuckDB not found. Make sure libduckdb-devel is installed.")
endif()

message(STATUS "Found DuckDB include: ${DUCKDB_INCLUDE_DIR}")
message(STATUS "Found DuckDB library: ${DUCKDB_LIBRARY}")

# Create shared library
add_library(duckdb_mojo_helpers SHARED
    src/duckdb_mojo_helpers.c
)

# Include directories
target_include_directories(duckdb_mojo_helpers PRIVATE ${DUCKDB_INCLUDE_DIR})

# Link libraries
target_link_libraries(duckdb_mojo_helpers PRIVATE ${DUCKDB_LIBRARY})

# Set properties
set_target_properties(duckdb_mojo_helpers PROPERTIES
    C_STANDARD 11
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
)

# Installation
install(TARGETS duckdb_mojo_helpers
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)
install(TARGETS duckdb_mojo_helpers
    LIBRARY DESTINATION lib
    RUNTIME DESTINATION bin
)
