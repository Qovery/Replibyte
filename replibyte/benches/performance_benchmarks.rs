use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use std::io::BufReader;
use std::io::Cursor;

use dump_parser::utils::list_sql_queries_from_dump_reader;

fn benchmark_query_parsing(c: &mut Criterion) {
    let mut group = c.benchmark_group("query_parsing");

    // Generate test SQL of different sizes
    let small_sql = "INSERT INTO users (id, name) VALUES (1, 'test');".repeat(100);
    let medium_sql = "INSERT INTO users (id, name, email, created_at) VALUES (1, 'test user', 'test@example.com', NOW());".repeat(1000);
    let large_sql = "INSERT INTO users (id, name, email, created_at, description) VALUES (1, 'test user', 'test@example.com', NOW(), 'This is a longer description field that contains more data to simulate real-world usage patterns');".repeat(10000);

    for (name, sql) in [
        ("small", &small_sql),
        ("medium", &medium_sql),
        ("large", &large_sql),
    ] {
        group.bench_with_input(BenchmarkId::new("dump_parser", name), sql, |b, sql| {
            b.iter(|| {
                let reader = BufReader::new(Cursor::new(sql.as_bytes()));
                list_sql_queries_from_dump_reader(reader, |_query| {
                    dump_parser::utils::ListQueryResult::Continue
                })
                .unwrap();
            });
        });
    }

    group.finish();
}

fn benchmark_memory_allocation(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");

    // Test Vec allocation patterns
    group.bench_function("vec_new_vs_with_capacity", |b| {
        b.iter(|| {
            let mut queries_new = Vec::new();
            let mut queries_capacity = Vec::with_capacity(1000);

            for i in 0..1000 {
                let query = format!("INSERT INTO test VALUES ({});", i);
                queries_new.push(query.clone());
                queries_capacity.push(query);
            }

            black_box((queries_new, queries_capacity));
        });
    });

    group.finish();
}

fn benchmark_buffer_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("buffer_operations");

    // Test different buffer sizes for chunking
    let test_data: Vec<u8> = (0..1_000_000).map(|i| (i % 256) as u8).collect();

    for buffer_size in [1024 * 1024, 10 * 1024 * 1024, 100 * 1024 * 1024] {
        group.bench_with_input(
            BenchmarkId::new("chunking", buffer_size),
            &buffer_size,
            |b, &size| {
                b.iter(|| {
                    let mut chunks = Vec::new();
                    let mut current_size = 0;
                    let mut current_chunk = Vec::new();

                    for byte in &test_data {
                        if current_size >= size {
                            chunks.push(current_chunk.clone());
                            current_chunk.clear();
                            current_size = 0;
                        }
                        current_chunk.push(*byte);
                        current_size += 1;
                    }

                    if !current_chunk.is_empty() {
                        chunks.push(current_chunk);
                    }

                    black_box(chunks.len());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    benchmark_query_parsing,
    benchmark_memory_allocation,
    benchmark_buffer_operations
);
criterion_main!(benches);
