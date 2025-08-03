use criterion::{black_box, criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use dump_parser::postgres::{get_column_names_from_insert_into_query, tokenize};
use dump_parser::postgres::optimized::OptimizedPostgresParser;
use dump_parser::mysql::{get_column_names_from_insert_into_query as mysql_get_columns, tokenize as mysql_tokenize};
use dump_parser::mysql::optimized::OptimizedMySQLParser;

/// Generate realistic test SQL queries for benchmarking
fn generate_test_queries() -> (Vec<String>, Vec<String>) {
    let postgres_queries = vec![
        // Simple INSERT
        r#"INSERT INTO users (id, name) VALUES (1, 'John Doe');"#.to_string(),
        
        // Complex INSERT with many columns
        r#"INSERT INTO "public"."user_profiles" ("user_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "zip_code", "country", "created_at", "updated_at") VALUES (12345, 'John', 'Doe', 'john.doe@example.com', '+1-555-0123', '123 Main Street', 'Anytown', 'CA', '90210', 'USA', '2023-01-15 10:30:00', '2023-01-15 10:30:00');"#.to_string(),
        
        // INSERT with escaped strings
        r#"INSERT INTO posts (title, content, author) VALUES ('Database ''Performance'' Tips', 'Here are some tips:\n1. Use indexes\n2. Optimize queries\n3. Monitor performance', 'admin');"#.to_string(),
        
        // Large batch INSERT
        format!(r#"INSERT INTO products (id, name, description, price, category) VALUES {};"#,
            (1..=100).map(|i| format!("({}, 'Product {}', 'Description for product {} with lots of text to simulate real-world data sizes', {}.99, 'Category {}')", i, i, i, i * 10, i % 5))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        
        // Complex query with subqueries and joins
        r#"INSERT INTO order_items (order_id, product_id, quantity, price) SELECT o.id, p.id, 1, p.price FROM orders o CROSS JOIN products p WHERE o.status = 'pending' AND p.available = true LIMIT 1000;"#.to_string(),
    ];
    
    let mysql_queries = vec![
        // Simple INSERT with backticks
        r#"INSERT INTO `users` (`id`, `name`) VALUES (1, 'John Doe');"#.to_string(),
        
        // Complex INSERT with MySQL-specific features
        r#"INSERT INTO `user_profiles` (`user_id`, `first_name`, `last_name`, `email`, `phone`, `address`, `city`, `state`, `zip_code`, `country`, `created_at`, `updated_at`) VALUES (12345, 'John', 'Doe', 'john.doe@example.com', '+1-555-0123', '123 Main Street', 'Anytown', 'CA', '90210', 'USA', NOW(), NOW());"#.to_string(),
        
        // INSERT with MySQL comments
        r#"INSERT INTO posts (title, content, author) VALUES ('Database Performance Tips', 'Here are some tips:\n1. Use indexes\n2. Optimize queries', 'admin'); -- This is a comment"#.to_string(),
        
        // Large batch INSERT with MySQL syntax
        format!(r#"INSERT INTO `products` (`id`, `name`, `description`, `price`, `category`) VALUES {};"#,
            (1..=100).map(|i| format!("({}, 'Product {}', 'Description for product {} with lots of text', {}.99, 'Category {}')", i, i, i, i * 10, i % 5))
                .collect::<Vec<_>>()
                .join(", ")
        ),
        
        // MySQL-specific syntax with double quotes
        r#"INSERT INTO "order_summary" ("order_id", "total", "status") VALUES (1001, 299.99, "completed");"#.to_string(),
    ];
    
    (postgres_queries, mysql_queries)
}

fn benchmark_postgres_tokenization(c: &mut Criterion) {
    let (postgres_queries, _) = generate_test_queries();
    let mut group = c.benchmark_group("postgres_tokenization");
    
    for (i, query) in postgres_queries.iter().enumerate() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        
        // Benchmark original tokenizer
        group.bench_with_input(
            BenchmarkId::new("original", i),
            query,
            |b, query| {
                b.iter(|| {
                    let result = tokenize(black_box(query));
                    black_box(result);
                });
            },
        );
        
        // Benchmark optimized tokenizer
        group.bench_with_input(
            BenchmarkId::new("optimized", i),
            query,
            |b, query| {
                let mut parser = OptimizedPostgresParser::new(query.len() * 2);
                b.iter(|| {
                    let result = parser.tokenize_optimized(black_box(query));
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_mysql_tokenization(c: &mut Criterion) {
    let (_, mysql_queries) = generate_test_queries();
    let mut group = c.benchmark_group("mysql_tokenization");
    
    for (i, query) in mysql_queries.iter().enumerate() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        
        // Benchmark original tokenizer
        group.bench_with_input(
            BenchmarkId::new("original", i),
            query,
            |b, query| {
                b.iter(|| {
                    let result = mysql_tokenize(black_box(query));
                    black_box(result);
                });
            },
        );
        
        // Benchmark optimized tokenizer
        group.bench_with_input(
            BenchmarkId::new("optimized", i),
            query,
            |b, query| {
                let mut parser = OptimizedMySQLParser::new(query.len() * 2);
                b.iter(|| {
                    let result = parser.tokenize_optimized(black_box(query));
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_postgres_column_extraction(c: &mut Criterion) {
    let (postgres_queries, _) = generate_test_queries();
    let mut group = c.benchmark_group("postgres_column_extraction");
    
    // Filter only INSERT queries for column extraction
    let insert_queries: Vec<&String> = postgres_queries
        .iter()
        .filter(|q| q.trim_start().to_uppercase().starts_with("INSERT"))
        .collect();
    
    for (i, query) in insert_queries.iter().enumerate() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        
        // Benchmark original column extraction
        group.bench_with_input(
            BenchmarkId::new("original", i),
            query,
            |b, query| {
                b.iter(|| {
                    let result = get_column_names_from_insert_into_query(black_box(query));
                    black_box(result);
                });
            },
        );
        
        // Benchmark optimized column extraction
        group.bench_with_input(
            BenchmarkId::new("optimized", i),
            query,
            |b, query| {
                let mut parser = OptimizedPostgresParser::new(query.len() * 2);
                b.iter(|| {
                    let result = parser.extract_insert_columns_fast(black_box(query));
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_mysql_column_extraction(c: &mut Criterion) {
    let (_, mysql_queries) = generate_test_queries();
    let mut group = c.benchmark_group("mysql_column_extraction");
    
    // Filter only INSERT queries for column extraction
    let insert_queries: Vec<&String> = mysql_queries
        .iter()
        .filter(|q| q.trim_start().to_uppercase().starts_with("INSERT"))
        .collect();
    
    for (i, query) in insert_queries.iter().enumerate() {
        group.throughput(Throughput::Bytes(query.len() as u64));
        
        // Benchmark original column extraction
        group.bench_with_input(
            BenchmarkId::new("original", i),
            query,
            |b, query| {
                b.iter(|| {
                    let result = mysql_get_columns(black_box(query));
                    black_box(result);
                });
            },
        );
        
        // Benchmark optimized column extraction
        group.bench_with_input(
            BenchmarkId::new("optimized", i),
            query,
            |b, query| {
                let mut parser = OptimizedMySQLParser::new(query.len() * 2);
                b.iter(|| {
                    let result = parser.extract_insert_columns_fast(black_box(query));
                    black_box(result);
                });
            },
        );
    }
    
    group.finish();
}

fn benchmark_memory_allocation_patterns(c: &mut Criterion) {
    let mut group = c.benchmark_group("memory_allocation");
    
    let test_query = r#"INSERT INTO "users" ("id", "first_name", "last_name", "email", "phone", "address", "city", "state", "zip", "country") VALUES (1, 'John', 'Doe', 'john@example.com', '555-0123', '123 Main St', 'Anytown', 'CA', '90210', 'USA');"#;
    
    // Benchmark allocation patterns
    group.bench_function("vec_new_vs_with_capacity", |b| {
        b.iter(|| {
            // Simulate old allocation pattern
            let mut tokens_new = Vec::new();
            for _ in 0..1000 {
                tokens_new.push(format!("token_{}", tokens_new.len()));
            }
            
            // Simulate new allocation pattern
            let mut tokens_capacity = Vec::with_capacity(1000);
            for _ in 0..1000 {
                tokens_capacity.push(format!("token_{}", tokens_capacity.len()));
            }
            
            black_box((tokens_new.len(), tokens_capacity.len()));
        });
    });
    
    // Benchmark reuse patterns
    group.bench_function("parser_reuse", |b| {
        let mut parser = OptimizedPostgresParser::new(test_query.len() * 2);
        b.iter(|| {
            for _ in 0..10 {
                let result = parser.tokenize_optimized(black_box(test_query));
                black_box(result);
            }
        });
    });
    
    group.finish();
}

fn benchmark_simd_operations(c: &mut Criterion) {
    let mut group = c.benchmark_group("simd_operations");
    
    let test_data = "SELECT * FROM users WHERE name = 'John' INSERT INTO products VALUES (1, 'test') UPDATE users SET name = 'Jane'".repeat(1000);
    
    // Benchmark pattern finding
    group.bench_function("pattern_finding", |b| {
        let mut parser = OptimizedPostgresParser::new(test_data.len());
        b.iter(|| {
            let result = parser.tokenize_optimized(black_box(&test_data));
            black_box(result);
        });
    });
    
    // Benchmark whitespace skipping
    let whitespace_heavy = format!("   \t\n\r   INSERT   \t\n   INTO   \t\n   users   \t\n   VALUES   \t\n   ");
    group.bench_function("whitespace_skipping", |b| {
        let mut parser = OptimizedPostgresParser::new(whitespace_heavy.len());
        b.iter(|| {
            let result = parser.tokenize_optimized(black_box(&whitespace_heavy));
            black_box(result);
        });
    });
    
    group.finish();
}

criterion_group!(
    parser_benches,
    benchmark_postgres_tokenization,
    benchmark_mysql_tokenization,
    benchmark_postgres_column_extraction,
    benchmark_mysql_column_extraction,
    benchmark_memory_allocation_patterns,
    benchmark_simd_operations
);

criterion_main!(parser_benches);