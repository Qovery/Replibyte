# RepliByte Performance Tuning Guide

This guide covers performance optimizations and tuning options for RepliByte to handle large databases efficiently.

## Performance Issues Identified and Fixed

### Memory Management Issues
1. **Excessive Vec cloning**: Fixed by using `std::mem::take()` instead of `.clone()` in `full_dump.rs`
2. **Unoptimized allocations**: Added `Vec::with_capacity()` calls throughout the codebase
3. **Buffer pool**: Added reusable buffer pool to reduce GC pressure

### I/O and Processing Bottlenecks
1. **Progress bar overhead**: Reduced update frequency from 50μs to 10ms
2. **Channel buffer size**: Increased from 1 to 10 for better throughput
3. **SQL parsing optimization**: Improved UTF-8 handling and buffer management

## Environment Variable Tuning

You can tune RepliByte's performance using these environment variables:

```bash
# Buffer size for data chunking (default: 100MB)
export REPLIBYTE_BUFFER_SIZE=134217728  # 128MB

# Query vector pre-allocation size (default: 1000)
export REPLIBYTE_QUERY_CAPACITY=2000

# Inter-thread communication buffer (default: 10)
export REPLIBYTE_CHANNEL_BUFFER=20
```

## Performance Recommendations by Database Size

### Small Databases (< 1GB)
- Default settings should work well
- Consider reducing buffer size to save memory:
  ```bash
  export REPLIBYTE_BUFFER_SIZE=33554432  # 32MB
  ```

### Medium Databases (1GB - 10GB)
- Increase buffer size and query capacity:
  ```bash
  export REPLIBYTE_BUFFER_SIZE=209715200  # 200MB
  export REPLIBYTE_QUERY_CAPACITY=2000
  ```

### Large Databases (10GB - 100GB)
- Maximize buffer sizes and enable all optimizations:
  ```bash
  export REPLIBYTE_BUFFER_SIZE=524288000   # 500MB
  export REPLIBYTE_QUERY_CAPACITY=5000
  export REPLIBYTE_CHANNEL_BUFFER=50
  ```

### Very Large Databases (> 100GB)
- Consider these additional optimizations:
  ```bash
  export REPLIBYTE_BUFFER_SIZE=1073741824  # 1GB
  export REPLIBYTE_QUERY_CAPACITY=10000
  export REPLIBYTE_CHANNEL_BUFFER=100
  ```

## System-Level Optimizations

### Memory
- Ensure sufficient RAM (at least 2x buffer size + 1GB for the OS)
- Consider increasing swap if memory is limited
- Monitor memory usage: `htop` or `ps aux | grep replibyte`

### Storage
- Use SSD storage for temporary files (subset operations)
- Ensure adequate disk space (3x database size as safety margin)
- For S3/GCP: Use same region as your datastore to reduce latency

### Network
- For cloud datastores: Use instances in the same region/availability zone
- Consider network bandwidth limits when setting buffer sizes
- Monitor network usage during transfers

## Monitoring Performance

### Built-in Metrics
RepliByte shows progress bars and transfer rates. Monitor these for:
- Consistent transfer rates (not declining over time)
- Memory usage staying stable
- No excessive pausing between chunks

### System Monitoring
```bash
# Monitor CPU and memory usage
htop

# Monitor I/O
iotop

# Monitor network (Linux)
iftop

# Check for memory leaks
valgrind --tool=memcheck ./replibyte -c config.yaml dump create
```

### Benchmarking
Run the included benchmarks to test performance on your hardware:

```bash
cd replibyte/
cargo bench

# Run specific benchmark categories
cargo bench query_parsing
cargo bench memory_allocation
cargo bench buffer_operations
```

## Troubleshooting Performance Issues

### Symptom: Slow startup
- **Cause**: Database connection issues or authentication delays
- **Solution**: Check database connectivity and credentials

### Symptom: Memory usage keeps growing
- **Cause**: Potential memory leak or buffer size too small
- **Solution**: 
  - Monitor with `ps aux | grep replibyte`
  - Try smaller buffer size
  - Update to latest version with memory fixes

### Symptom: Transfer speed decreases over time
- **Cause**: Network throttling or storage I/O limits
- **Solution**:
  - Check network bandwidth limits
  - Monitor storage IOPS
  - Consider using multiple smaller transfers

### Symptom: High CPU usage during parsing
- **Cause**: Complex SQL queries or large number of transformations
- **Solution**:
  - Reduce number of transformations
  - Consider preprocessing queries
  - Use fewer concurrent operations

## Advanced Configuration

For advanced users, you can create a `performance.toml` file:

```toml
[performance]
buffer_size = 134217728
query_vector_capacity = 2000
channel_buffer_size = 20
progress_update_interval = 10

[performance.buffer_pool]
max_buffers = 20
buffer_capacity = 16384
enabled = true

[performance.parser]
parser_stack_capacity = 32
line_buffer_capacity = 2048
main_buffer_capacity = 16384
```

Load with: `replibyte --performance-config performance.toml -c config.yaml dump create`

## Reporting Performance Issues

When reporting performance issues, please include:

1. **System specs**: CPU, RAM, storage type
2. **Database specs**: Type, size, complexity
3. **Configuration**: Buffer sizes, environment variables
4. **Timing data**: How long operations take
2. **Resource usage**: CPU, memory, I/O stats during operation
6. **Logs**: Any error messages or warnings

Use the benchmark suite to gather baseline performance data:

```bash
cargo bench 2>&1 | tee performance_report.txt
```

## Contributing Performance Improvements

See `replibyte/benches/performance_benchmarks.rs` for the benchmarking framework.

Key areas for improvement:
1. SQL parsing performance (currently in `dump-parser`)
2. Memory allocation patterns
3. I/O buffering strategies
4. Compression/encryption overhead
5. Network transfer optimization

Run benchmarks before and after changes:
```bash
cargo bench --bench performance_benchmarks
```