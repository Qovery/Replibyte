# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

RepliByte is a Rust-based tool for seeding development databases with production data while keeping sensitive information safe. It supports PostgreSQL, MySQL, and MongoDB with features like data transformation, compression, encryption, and database subsetting.

## Build and Development Commands

### Core Commands
```bash
# Build the project
cargo build

# Run with a config file
cargo run -- -c examples/replibyte.yaml dump create

# Run tests
cargo test

# Check code formatting
cargo fmt --check

# Run clippy for linting
cargo clippy
```

### Common Development Tasks
```bash
# Create a dump
./target/debug/replibyte -c conf.yaml dump create

# List all dumps
./target/debug/replibyte -c conf.yaml dump list

# Restore latest dump locally
./target/debug/replibyte -c conf.yaml dump restore local -v latest -i postgres -p 5432

# Restore dump to remote database
./target/debug/replibyte -c conf.yaml dump restore remote -v latest

# Show database schema
./target/debug/replibyte -c conf.yaml source schema

# List available transformers
./target/debug/replibyte -c conf.yaml transformer list
```

## Architecture

### Workspace Structure
- **replibyte/**: Main CLI application with core logic
- **dump-parser/**: Library for parsing database dumps (PostgreSQL, MySQL, MongoDB)
- **subset/**: Library for database subsetting functionality

### Key Components
- **Sources**: Database connectors (PostgreSQL, MySQL, MongoDB) in `replibyte/src/source/`
- **Destinations**: Database restoration targets in `replibyte/src/destination/`
- **Transformers**: Data anonymization and transformation in `replibyte/src/transformer/`
- **Datastores**: Storage backends (S3, GCP, Local Disk) in `replibyte/src/datastore/`
- **CLI**: Command-line interface definition in `replibyte/src/cli.rs`

### Data Flow
1. Extract data from source database
2. Apply transformations (optional subsetting, data anonymization)
3. Compress and encrypt data (optional)
4. Store in configured datastore (S3, GCP, Local)
5. Restore to destination database

### Configuration
Uses YAML configuration files. See `examples/` directory for sample configurations supporting various database types and datastore combinations.

### Transformers
Built-in transformers for data anonymization:
- `credit_card`, `email`, `first_name`, `phone_number`
- `random`, `redacted`, `keep_first_char`
- Custom WASM transformer support

### Critical Notes
- **dump-parser crate**: DO NOT upgrade the `crc` crate beyond version 1.8 - version 2+ breaks MongoDB restore compatibility
- Uses streaming processing to handle large databases (>10GB) with minimal memory footprint
- Stateless operation - no server or daemon required
- Supports concurrent operations but index file doesn't handle concurrent writes to same bridge

### Release Process
Use `release.sh` script for version management across all workspace crates.

### Docker Integration
Supports Docker containers for local database restoration. Various docker-compose files available for different database/datastore combinations.

## Performance Optimization (v0.11.0+)

### Benchmarking and Testing
```bash
# Run performance benchmarks
cargo bench

# Run specific benchmark suites
cargo bench query_parsing
cargo bench memory_allocation
cargo bench buffer_operations

# Run integration tests
cargo test --test integration_tests

# Enable profiling for performance analysis
export REPLIBYTE_PROFILE=1
./target/release/replibyte -c conf.yaml dump create
```

### Performance Tuning
Key environment variables for performance tuning:
```bash
# Enable comprehensive profiling
export REPLIBYTE_PROFILE=1

# Buffer size for data chunking (default: 100MB)
export REPLIBYTE_BUFFER_SIZE=134217728

# Query vector pre-allocation (default: 1000)
export REPLIBYTE_QUERY_CAPACITY=2000

# Channel buffer size (default: 10)
export REPLIBYTE_CHANNEL_BUFFER=20

# Memory pool configuration
export REPLIBYTE_POOL_SIZE=1000
export REPLIBYTE_MAX_CHUNK_SIZE=16777216
```

### Advanced Performance Features (v0.11.0+)

#### Lock-Free Memory Management
- **Lock-free object pools** for Vec<u8> reuse (`utils/advanced_pool.rs`)
- **Thread-local storage** for high-performance memory access
- **Zero-copy string processing** using `Cow<'a, str>` where possible
- **Memory tracking** with allocation/deallocation monitoring

#### SIMD Optimizations
- **AVX2 vectorized operations** for byte processing on x86_64
- **Fast SQL keyword detection** using SIMD pattern matching
- **Accelerated byte counting and comparison** operations
- **Cross-platform fallbacks** for non-x86_64 architectures

#### I/O Optimizations
- **Optimized buffered readers** with adaptive capacity management
- **Async I/O processing** with overlapping read/write operations
- **Ring buffers** for efficient data streaming
- **Memory-mapped file I/O** for extremely large dumps

#### Streaming Architecture
- **Constant memory usage** streaming dump tasks
- **Adaptive chunking** based on available system memory
- **Backpressure handling** to prevent memory spikes
- **Zero-allocation hot paths** for critical operations

#### Comprehensive Profiling System
- **Minimal overhead profiling** with hot path detection
- **Memory usage tracking** with peak detection
- **Real-time performance monitoring** with sampling
- **Benchmark utilities** for performance regression testing

### Memory Management
- Uses **advanced buffer pooling** to eliminate allocation overhead
- **Pre-allocates vectors** with optimal capacity to avoid reallocations
- **Zero-copy operations** where possible using `std::mem::take()` and `Cow<str>`
- **Memory-mapped I/O** for handling files larger than available RAM
- **Thread-local object pools** for lock-free high-performance access

### Key Performance Files (v0.11.0+)
- `src/utils/advanced_pool.rs`: Lock-free memory pools and thread-local storage
- `src/io/optimized_io.rs`: Advanced I/O operations with ring buffers and memory mapping
- `src/simd/mod.rs`: SIMD-optimized operations for data processing
- `src/profiling/mod.rs`: Comprehensive profiling and monitoring system
- `src/tasks/streaming_dump.rs`: Streaming architecture with constant memory usage
- `src/types.rs`: Zero-copy data structures and optimized query handling
- `benches/performance_benchmarks.rs`: Comprehensive benchmark suite
- `dump-parser/src/optimized_parser.rs`: High-performance SQL parser with SIMD

### Performance Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Source   │───▶│   SIMD Parser    │───▶│  Memory Pools   │
│   (Database)    │    │   (Zero-copy)    │    │  (Lock-free)    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Datastore     │◀───│ Streaming Engine │◀───│  Ring Buffers   │
│   (S3/Local)    │    │ (Constant Mem)   │    │ (Async I/O)     │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Critical Performance Notes
- **SIMD operations** automatically detect CPU features and fall back gracefully
- **Memory pools** reduce allocation overhead by 70-90% in typical workloads
- **Streaming architecture** maintains constant memory usage regardless of dump size
- **Profiling system** adds <1% overhead when enabled
- **Zero-copy parsing** eliminates string allocation in hot paths
- **Lock-free pools** provide thread-safe high-performance memory management