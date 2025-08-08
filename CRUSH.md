# CRUSH.md - RepliByte Development Guide

## Build & Test Commands
```bash
# Build entire workspace
cargo build

# Build release version
cargo build --release

# Run all tests
cargo test

# Run specific test
cargo test test_name

# Run tests in specific crate
cargo test -p replibyte
cargo test -p dump-parser
cargo test -p subset

# Run integration tests only
cargo test --test integration_tests

# Run benchmarks
cargo bench

# Format code
cargo fmt

# Check formatting
cargo fmt --check

# Lint code
cargo clippy

# Lint with all targets
cargo clippy --all-targets
```

## Code Style Guidelines

### Imports & Modules
- Group imports: std, external crates, local crates, current crate modules
- Use `mod` declarations at top of files after imports
- Prefer explicit imports over glob imports

### Types & Naming
- Use `PascalCase` for types, structs, enums
- Use `snake_case` for functions, variables, modules
- Use `SCREAMING_SNAKE_CASE` for constants
- Prefer descriptive names over abbreviations

### Error Handling
- Use `anyhow::Result` for application errors
- Use `std::io::Error` for I/O operations
- Propagate errors with `?` operator
- Return `Result<(), Error>` for operations without meaningful return values

### Performance & Memory
- Use `Cow<'a, str>` for zero-copy string operations
- Prefer `Vec::with_capacity()` when size is known
- Use `std::mem::take()` to avoid clones
- Leverage SIMD optimizations in hot paths
- Use memory pools for frequent allocations

### Architecture
- Implement traits for database connectors (Source, Destination)
- Use workspace structure: replibyte (CLI), dump-parser (parsing), subset (subsetting)
- Follow streaming architecture for constant memory usage