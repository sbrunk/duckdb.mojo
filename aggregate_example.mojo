from duckdb import *
from duckdb._libduckdb import *
import benchmark
import math
from memory import UnsafePointer, memcpy
from sys import has_accelerator

# GPU imports
from gpu.host import DeviceContext
from gpu import block_dim, block_idx, thread_idx
from gpu.primitives import warp
from gpu.sync import barrier

# GPU-Ready Aggregate Function for DuckDB
# Demonstrates custom aggregate function with parallel state management

# Simple POD state for DuckDB-managed memory
struct SimpleAggState:
    """Aggregate state - must be Plain Old Data (no Lists/Strings)."""
    var count: Int
    var min_value: Float32


# GPU Kernel for computing min of sin(a) * cos(b) + sqrt(a * b)
fn compute_and_reduce_kernel(
    a_data: UnsafePointer[Float32, MutAnyOrigin],
    b_data: UnsafePointer[Float32, MutAnyOrigin],
    result: UnsafePointer[Float32, MutAnyOrigin],
    size: Int
) -> NoneType:
    """GPU kernel: each thread computes value, then manual warp reduction finds minimum."""
    var idx = Int(block_idx.x * block_dim.x + thread_idx.x)
    
    var thread_min = Float32(1e9)
    
    # Each thread processes one element (with bounds check)
    if idx < size:
        var a_val = a_data[idx]
        var b_val = b_data[idx]
        thread_min = math.sin(a_val) * math.cos(b_val) + math.sqrt(a_val * b_val)
    
    # Manual warp reduction using shuffle down
    var offset = 16
    while offset > 0:
        var other = warp.shuffle_down(thread_min, offset)
        if other < thread_min:
            thread_min = other
        offset = offset // 2
    
    # First thread in each warp writes to global memory
    var lane_id = Int(thread_idx.x % 32)
    if lane_id == 0:
        var warp_id = Int(thread_idx.x // 32)
        var warps_per_block = (Int(block_dim.x) + 31) // 32
        var warp_result_idx = Int(block_idx.x) * warps_per_block + warp_id
        result[warp_result_idx] = thread_min
    
    return None


fn aggregate_state_size(info: duckdb_function_info) -> idx_t:
    """Returns the size of the aggregate state."""
    return 1024  # Large enough for SimpleAggState


fn aggregate_initialize(info: duckdb_function_info, state: duckdb_aggregate_state) -> NoneType:
    """Initialize aggregate state - called ONCE per parallel state."""
    var state_ptr = state.bitcast[SimpleAggState]()
    state_ptr[].count = 0
    state_ptr[].min_value = Float32(1e9)
    return None


fn aggregate_update(
    info: duckdb_function_info,
    input: duckdb_data_chunk,
    states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin]
) -> NoneType:
    """Update state with input chunk - called for EACH chunk."""
    ref lib = DuckDB().libduckdb()
    var state_ptr = states[0].bitcast[SimpleAggState]()
    
    var input_size = Int(lib.duckdb_data_chunk_get_size(input))
    
    var a = lib.duckdb_data_chunk_get_vector(input, 0)
    var b = lib.duckdb_data_chunk_get_vector(input, 1)
    
    var a_data = lib.duckdb_vector_get_data(a).bitcast[Float32]()
    var b_data = lib.duckdb_vector_get_data(b).bitcast[Float32]()
    
    # Process this chunk and track minimum
    for i in range(input_size):
        var a_val = a_data[i]
        var b_val = b_data[i]
        var computed = math.sin(a_val) * math.cos(b_val) + math.sqrt(a_val * b_val)
        if computed < state_ptr[].min_value:
            state_ptr[].min_value = computed
        state_ptr[].count += 1
    
    return None


fn aggregate_finalize(
    info: duckdb_function_info,
    states: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin],
    result: duckdb_vector,
    count: idx_t,
    offset: idx_t
) -> NoneType:
    """Finalize - called ONCE to produce final result."""
    ref lib = DuckDB().libduckdb()
    
    # Combine all parallel states
    var total_count = 0
    var final_min = Float32(1e9)
    
    for i in range(Int(count)):
        var state_ptr = states[i].bitcast[SimpleAggState]()
        total_count += state_ptr[].count
        if state_ptr[].min_value < final_min:
            final_min = state_ptr[].min_value
    
    # Write result
    var result_data = lib.duckdb_vector_get_data(result).bitcast[Float32]()
    result_data[offset] = final_min
    
    return None


fn aggregate_combine(
    info: duckdb_function_info,
    state: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin],
    other_state: UnsafePointer[duckdb_aggregate_state, MutExternalOrigin],
    count: idx_t
) -> NoneType:
    """Combines two parallel states - DuckDB's combine semantics."""
    # CRITICAL: In DuckDB, other_state is the target accumulator, state is source
    var source_ptr = state[0].bitcast[SimpleAggState]()
    var target_ptr = other_state[0].bitcast[SimpleAggState]()
    
    # Merge source into target
    if source_ptr[].min_value < target_ptr[].min_value:
        target_ptr[].min_value = source_ptr[].min_value
    target_ptr[].count += source_ptr[].count
    
    return None


fn register_aggregate(lib: LibDuckDB, connection: duckdb_connection):
    """Registers the 'compute_min' aggregate function."""
    var function = lib.duckdb_create_aggregate_function()
    
    # Set function name
    var func_name = String("compute_min")
    lib.duckdb_aggregate_function_set_name(function, func_name.as_c_string_slice().unsafe_ptr())

    var type = lib.duckdb_create_logical_type(DUCKDB_TYPE_FLOAT)
    
    lib.duckdb_aggregate_function_add_parameter(function, type)
    lib.duckdb_aggregate_function_add_parameter(function, type)
    lib.duckdb_aggregate_function_set_return_type(function, type)
    
    lib.duckdb_destroy_logical_type(UnsafePointer(to=type))

    # Set all aggregate callbacks
    lib.duckdb_aggregate_function_set_functions(
        function,
        aggregate_state_size,
        aggregate_initialize,
        aggregate_update,
        aggregate_combine,  # Essential for parallel execution
        aggregate_finalize
    )

    var status = lib.duckdb_register_aggregate_function(connection, function)
    if status != DuckDBSuccess:
        print("❌ Failed to register aggregate function")

    var function_ptr = UnsafePointer(to=function)
    lib.duckdb_destroy_aggregate_function(function_ptr)


fn exec(con: Connection, sql: String) raises:
    """Execute SQL and fetch result (for benchmarking)."""
    try:
        _ = con.execute(sql).fetch_chunk()
    except e:
        print(e)


fn main() raises:
    var con = DuckDB.connect(":memory:")
    register_aggregate(DuckDB().libduckdb(), con._conn)
    
    # Create test table with 1M rows
    _ = con.execute(
    """
    CREATE TABLE big_table AS 
    SELECT (random() * 100)::FLOAT AS a, (random() * 100)::FLOAT AS b 
    FROM range(1_000_000_000) t(i);
    """
    )

    fn regular() capturing raises:
        exec(con, "SELECT min(sin(a) * cos(b) + sqrt(a * b)) FROM big_table;")

    fn aggregate() capturing raises:
        exec(con, "SELECT compute_min(a, b) FROM big_table;")
        
    # Initialize GPU context
    var ctx = DeviceContext()
        
    # Compile kernel first
    var kernel = ctx.compile_function_unchecked[compute_and_reduce_kernel]()

    fn gpu_aggregate() capturing raises:
        @parameter
        if not has_accelerator():
            print("GPU not available")
            return
        
        # Fetch data from DuckDB to CPU
        var chunk = con.execute("SELECT a, b FROM big_table;").fetch_all()
        # var chunk = result.fetch_all()
        ref lib = DuckDB().libduckdb()
        
        var size = Int(lib.duckdb_data_chunk_get_size(chunk._chunk))
        
        var a_vec = lib.duckdb_data_chunk_get_vector(chunk._chunk, 0)
        var b_vec = lib.duckdb_data_chunk_get_vector(chunk._chunk, 1)
        
        var a_data = lib.duckdb_vector_get_data(a_vec).bitcast[Float32]()
        var b_data = lib.duckdb_vector_get_data(b_vec).bitcast[Float32]()
        
        
        # Allocate device buffers
        var a_device = ctx.enqueue_create_buffer[DType.float32](size)
        var b_device = ctx.enqueue_create_buffer[DType.float32](size)
        
        # Create host buffers and copy data to device
        var a_host = ctx.enqueue_create_host_buffer[DType.float32](size)
        var b_host = ctx.enqueue_create_host_buffer[DType.float32](size)
        ctx.synchronize()
        
        # Copy from DuckDB memory to host buffers
        for i in range(size):
            a_host[i] = a_data[i]
            b_host[i] = b_data[i]
        
        # Copy to device
        ctx.enqueue_copy(src_buf=a_host, dst_buf=a_device)
        ctx.enqueue_copy(src_buf=b_host, dst_buf=b_device)
        
        # Calculate grid dimensions
        var threads_per_block = 256
        var num_blocks = (size + threads_per_block - 1) // threads_per_block
        var warps_per_block = (threads_per_block + 31) // 32
        var total_warps = num_blocks * warps_per_block
        
        # Allocate buffer for partial results (one per warp)
        var partial_result = ctx.enqueue_create_buffer[DType.float32](total_warps)
        
        
        # Launch kernel
        ctx.enqueue_function_unchecked(
            kernel,
            a_device,
            b_device,
            partial_result,
            size,
            grid_dim=num_blocks,
            block_dim=threads_per_block
        )
        
        # Copy partial results back to CPU and find final minimum
        var partial_host = ctx.enqueue_create_host_buffer[DType.float32](total_warps)
        ctx.enqueue_copy(src_buf=partial_result, dst_buf=partial_host)
        ctx.synchronize()
        
        # CPU-side final reduction
        var final_min = Float32(1e9)
        for i in range(total_warps):
            if partial_host[i] < final_min:
                final_min = partial_host[i]

    print("Regular SQL (element-wise in vectors):")
    benchmark.run[regular](max_iters=10).print(unit="ms")

    print("\nAggregate function (accumulate all, then compute):")
    benchmark.run[aggregate](max_iters=10).print(unit="ms")
    
    @parameter
    if has_accelerator():
        print("\nGPU aggregate function (parallel GPU computation):")
        benchmark.run[gpu_aggregate](max_iters=10).print(unit="ms")
    
    print("\nAll three produce same result!")
    print("- Regular: SQL engine processes element-wise")
    print("- Aggregate: CPU processes all data in custom function")  
    print("- GPU: Parallel GPU threads process data with warp reductions")
