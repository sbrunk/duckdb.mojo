cmake_minimum_required(VERSION 3.20)
project(duckdb-operator-replacement VERSION 0.0.1 LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Find DuckDB
find_library(DUCKDB_LIBRARY NAMES duckdb REQUIRED)
find_path(DUCKDB_INCLUDE_DIR NAMES duckdb.h REQUIRED)

# Build shared library
add_library(duckdb_operator_replacement SHARED
    duckdb_operator_replacement.cpp
    duckdb_operator_replacement_wrapper.cpp
    test_functions.cpp
)

target_include_directories(duckdb_operator_replacement
    PRIVATE
        ${DUCKDB_INCLUDE_DIR}
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

target_link_libraries(duckdb_operator_replacement
    PRIVATE
        ${DUCKDB_LIBRARY}
)

# Installation rules
install(TARGETS duckdb_operator_replacement
    LIBRARY DESTINATION lib
    ARCHIVE DESTINATION lib
    RUNTIME DESTINATION bin
)

install(FILES
    duckdb_operator_replacement.h
    DESTINATION include
)
