use std::time::Instant;
use dump_parser::postgres::{get_column_names_from_insert_into_query, tokenize};
use dump_parser::postgres::optimized::OptimizedPostgresParser;
use dump_parser::mysql::{get_column_names_from_insert_into_query as mysql_get_columns, tokenize as mysql_tokenize};
use dump_parser::mysql::optimized::OptimizedMySQLParser;

fn main() {
    println!("=== RepliByte Parser Performance Comparison ===\n");
    
    // Test data
    let postgres_queries = vec![
        r#"INSERT INTO users (id, name) VALUES (1, 'John Doe');"#,
        r#"INSERT INTO "public"."user_profiles" ("user_id", "first_name", "last_name", "email", "phone", "address", "city", "state", "zip_code", "country", "created_at", "updated_at") VALUES (12345, 'John', 'Doe', 'john.doe@example.com', '+1-555-0123', '123 Main Street', 'Anytown', 'CA', '90210', 'USA', '2023-01-15 10:30:00', '2023-01-15 10:30:00');"#,
        &format!("INSERT INTO products (id, name, description, price) VALUES {};", 
            (1..=1000).map(|i| format!("({}, 'Product {}', 'Description for product {}', {}.99)", i, i, i, i * 10))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    
    let mysql_queries = vec![
        r#"INSERT INTO `users` (`id`, `name`) VALUES (1, 'John Doe');"#,
        r#"INSERT INTO `user_profiles` (`user_id`, `first_name`, `last_name`, `email`, `phone`, `address`, `city`, `state`, `zip_code`, `country`, `created_at`, `updated_at`) VALUES (12345, 'John', 'Doe', 'john.doe@example.com', '+1-555-0123', '123 Main Street', 'Anytown', 'CA', '90210', 'USA', NOW(), NOW());"#,
        &format!("INSERT INTO `products` (`id`, `name`, `description`, `price`) VALUES {};", 
            (1..=1000).map(|i| format!("({}, 'Product {}', 'Description for product {}', {}.99)", i, i, i, i * 10))
                .collect::<Vec<_>>()
                .join(", ")
        ),
    ];
    
    benchmark_postgres_parsing(&postgres_queries);
    benchmark_mysql_parsing(&mysql_queries);
    benchmark_column_extraction(&postgres_queries, &mysql_queries);
}

fn benchmark_postgres_parsing(queries: &[&str]) {
    println!("🔍 PostgreSQL Tokenization Benchmarks:");
    println!("{:-<60}", "");
    
    for (i, query) in queries.iter().enumerate() {
        let query_size = query.len();
        let iterations = if query_size > 10000 { 100 } else { 1000 };
        
        // Benchmark original tokenizer
        let start = Instant::now();
        for _ in 0..iterations {
            let _tokens = tokenize(query).unwrap();
        }
        let original_duration = start.elapsed();
        
        // Benchmark optimized tokenizer
        let mut parser = OptimizedPostgresParser::new(query.len() * 2);
        let start = Instant::now();
        for _ in 0..iterations {
            let _tokens = parser.tokenize_optimized(query).unwrap();
        }
        let optimized_duration = start.elapsed();
        
        let speedup = original_duration.as_nanos() as f64 / optimized_duration.as_nanos() as f64;
        let throughput_original = (query_size as f64 * iterations as f64) / original_duration.as_secs_f64() / 1_000_000.0; // MB/s
        let throughput_optimized = (query_size as f64 * iterations as f64) / optimized_duration.as_secs_f64() / 1_000_000.0; // MB/s
        
        println!("Query {} ({} bytes):", i + 1, query_size);
        println!("  Original:  {:>8.2}ms ({:>6.1} MB/s)", original_duration.as_secs_f64() * 1000.0, throughput_original);
        println!("  Optimized: {:>8.2}ms ({:>6.1} MB/s)", optimized_duration.as_secs_f64() * 1000.0, throughput_optimized);
        println!("  Speedup:   {:>7.2}x", speedup);
        println!();
    }
}

fn benchmark_mysql_parsing(queries: &[&str]) {
    println!("🔍 MySQL Tokenization Benchmarks:");
    println!("{:-<60}", "");
    
    for (i, query) in queries.iter().enumerate() {
        let query_size = query.len();
        let iterations = if query_size > 10000 { 100 } else { 1000 };
        
        // Benchmark original tokenizer
        let start = Instant::now();
        for _ in 0..iterations {
            let _tokens = mysql_tokenize(query).unwrap();
        }
        let original_duration = start.elapsed();
        
        // Benchmark optimized tokenizer
        let mut parser = OptimizedMySQLParser::new(query.len() * 2);
        let start = Instant::now();
        for _ in 0..iterations {
            let _tokens = parser.tokenize_optimized(query).unwrap();
        }
        let optimized_duration = start.elapsed();
        
        let speedup = original_duration.as_nanos() as f64 / optimized_duration.as_nanos() as f64;
        let throughput_original = (query_size as f64 * iterations as f64) / original_duration.as_secs_f64() / 1_000_000.0; // MB/s
        let throughput_optimized = (query_size as f64 * iterations as f64) / optimized_duration.as_secs_f64() / 1_000_000.0; // MB/s
        
        println!("Query {} ({} bytes):", i + 1, query_size);
        println!("  Original:  {:>8.2}ms ({:>6.1} MB/s)", original_duration.as_secs_f64() * 1000.0, throughput_original);
        println!("  Optimized: {:>8.2}ms ({:>6.1} MB/s)", optimized_duration.as_secs_f64() * 1000.0, throughput_optimized);
        println!("  Speedup:   {:>7.2}x", speedup);
        println!();
    }
}

fn benchmark_column_extraction(postgres_queries: &[&str], mysql_queries: &[&str]) {
    println!("🎯 Column Extraction Benchmarks:");
    println!("{:-<60}", "");
    
    // PostgreSQL column extraction
    println!("PostgreSQL:");
    for (i, query) in postgres_queries.iter().enumerate() {
        let iterations = 1000;
        
        // Benchmark original column extraction
        let start = Instant::now();
        for _ in 0..iterations {
            let _columns = get_column_names_from_insert_into_query(query).unwrap();
        }
        let original_duration = start.elapsed();
        
        // Benchmark optimized column extraction
        let mut parser = OptimizedPostgresParser::new(query.len() * 2);
        let start = Instant::now();
        for _ in 0..iterations {
            let _columns = parser.extract_insert_columns_fast(query).unwrap();
        }
        let optimized_duration = start.elapsed();
        
        let speedup = original_duration.as_nanos() as f64 / optimized_duration.as_nanos() as f64;
        
        println!("  Query {}: {:.2}ms → {:.2}ms ({:.2}x speedup)", 
                i + 1, 
                original_duration.as_secs_f64() * 1000.0,
                optimized_duration.as_secs_f64() * 1000.0,
                speedup);
    }
    
    println!();
    
    // MySQL column extraction
    println!("MySQL:");
    for (i, query) in mysql_queries.iter().enumerate() {
        let iterations = 1000;
        
        // Benchmark original column extraction
        let start = Instant::now();
        for _ in 0..iterations {
            let _columns = mysql_get_columns(query).unwrap();
        }
        let original_duration = start.elapsed();
        
        // Benchmark optimized column extraction
        let mut parser = OptimizedMySQLParser::new(query.len() * 2);
        let start = Instant::now();
        for _ in 0..iterations {
            let _columns = parser.extract_insert_columns_fast(query).unwrap();
        }
        let optimized_duration = start.elapsed();
        
        let speedup = original_duration.as_nanos() as f64 / optimized_duration.as_nanos() as f64;
        
        println!("  Query {}: {:.2}ms → {:.2}ms ({:.2}x speedup)", 
                i + 1, 
                original_duration.as_secs_f64() * 1000.0,
                optimized_duration.as_secs_f64() * 1000.0,
                speedup);
    }
    
    println!("\n=== Summary ===");
    println!("✅ Optimized parsers implemented with:");
    println!("   • SIMD vectorization for pattern matching");
    println!("   • Zero-copy string processing");
    println!("   • Pre-allocated buffers");
    println!("   • Byte-level parsing optimization");
    println!("   • Cross-platform compatibility (x86_64, ARM)");
}