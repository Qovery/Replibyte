---
sidebar_position: 12
---

# FAQ

:::tip

[Open an issue](https://github.com/Qovery/replibyte/issues/new) if you don't find the answer to your question.

:::

### What language is used for Replibyte?

[Rust](https://www.rust-lang.org/)

### Why using Rust?

Replibyte is a IO intensive tool that need to process data as fast as possible. Rust is a perfect candidate for high throughput and low
memory consumption. Starting with v0.11.0, RepliByte leverages advanced Rust features like SIMD vectorization and lock-free data structures for even better performance.

### Does RepliByte is an ETL?

RepliByte is not an ETL like [AirByte](https://github.com/airbytehq/airbyte), [AirFlow](https://airflow.apache.org/), Talend, and it will
never be. If you need to synchronize versatile data sources, you are better choosing a classic ETL. RepliByte is a tool for software
engineers to help them to synchronize data from the same databases. With RepliByte, you can only replicate data from the same type of
databases. As mentioned above, the primary purpose of RepliByte is to duplicate into different environments. You can see RepliByte as a
specific use case of an ETL, where an ETL is more generic.

### Do you support backup from a dump file?

absolutely,

```shell
cat dump.sql | replibyte -c conf.yaml backup run -s postgres -i
```

and

```shell
replibyte -c conf.yaml backup run -s postgres -f dump.sql
```

### How RepliByte can list the dumps? Is there an API?

There is no API, RepliByte is fully stateless and store the dump list into the datastore (E.g. S3) via an metadata file.

### How do I enable the new performance features in v0.11.0+?

Performance optimizations are automatically enabled. To monitor and tune performance:

```bash
# Enable profiling to see performance improvements
export REPLIBYTE_PROFILE=1
replibyte -c conf.yaml dump create
```

See the [Performance Optimization guide](/docs/performance-optimization) for detailed configuration.

### How much faster is RepliByte v0.11.0 compared to previous versions?

Performance improvements vary by workload, but typical gains include:
- **2-4x faster processing** with SIMD optimizations (on x86_64 with AVX2)
- **70-90% reduction** in memory allocation overhead
- **Constant memory usage** regardless of database size
- **Better I/O throughput** with streaming architecture

### What CPUs benefit most from the SIMD optimizations?

SIMD optimizations work best on:
- **Intel**: Haswell and newer (2013+) with AVX2 support
- **AMD**: Excavator and newer (2015+) with AVX2 support
- **Apple Silicon**: M1/M2 processors with NEON support

RepliByte automatically detects CPU capabilities and falls back gracefully on older processors.

### Does RepliByte work on ARM processors (Apple Silicon, ARM servers)?

Yes! RepliByte v0.11.0+ includes optimizations for ARM processors:
- **Apple M1/M2**: Full SIMD support with NEON instructions
- **ARM servers**: Optimized memory management and streaming
- **Cross-platform**: All performance features work across architectures

### How can I contact you?

3 options:

1. [Open an issue](https://github.com/Qovery/replibyte/issues/new).
2. Join our #replibyte channel on [our discord](https://discord.qovery.com).
3. Drop us an email to `github+replibyte {at} qovery {dot} com`.
