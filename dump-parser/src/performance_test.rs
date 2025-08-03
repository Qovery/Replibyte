/// Simple performance tests for the optimized parsers
/// This module contains basic performance comparisons without external dependencies

#[cfg(test)]
mod tests {
    use std::time::Instant;
    use crate::postgres::optimized::OptimizedPostgresParser;
    use crate::mysql::optimized::OptimizedMySQLParser;

    #[test]
    fn test_postgres_parser_performance() {
        let query = r#"INSERT INTO "users" ("id", "name", "email") VALUES (1, 'John Doe', 'john@example.com');"#;
        let mut parser = OptimizedPostgresParser::new(1024);
        
        let start = Instant::now();
        for _ in 0..1000 {
            let _result = parser.tokenize_optimized(query).unwrap();
        }
        let duration = start.elapsed();
        
        println!("PostgreSQL parser: 1000 iterations in {:?}", duration);
        println!("Throughput: {:.2} queries/sec", 1000.0 / duration.as_secs_f64());
        
        // Test should complete in reasonable time (less than 1 second for 1000 iterations)
        assert!(duration.as_millis() < 1000, "Parser took too long: {:?}", duration);
    }

    #[test]
    fn test_mysql_parser_performance() {
        let query = r#"INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'John Doe', 'john@example.com');"#;
        let mut parser = OptimizedMySQLParser::new(1024);
        
        let start = Instant::now();
        for _ in 0..1000 {
            let _result = parser.tokenize_optimized(query).unwrap();
        }
        let duration = start.elapsed();
        
        println!("MySQL parser: 1000 iterations in {:?}", duration);
        println!("Throughput: {:.2} queries/sec", 1000.0 / duration.as_secs_f64());
        
        // Test should complete in reasonable time (less than 1 second for 1000 iterations)
        assert!(duration.as_millis() < 1000, "Parser took too long: {:?}", duration);
    }

    #[test]
    fn test_column_extraction_performance() {
        let pg_query = r#"INSERT INTO "users" ("id", "first_name", "last_name", "email") VALUES (1, 'John', 'Doe', 'john@example.com');"#;
        let mysql_query = r#"INSERT INTO `users` (`id`, `first_name`, `last_name`, `email`) VALUES (1, 'John', 'Doe', 'john@example.com');"#;
        
        let mut pg_parser = OptimizedPostgresParser::new(1024);
        let mut mysql_parser = OptimizedMySQLParser::new(1024);
        
        // PostgreSQL column extraction
        let start = Instant::now();
        for _ in 0..1000 {
            let _columns = pg_parser.extract_insert_columns_fast(pg_query).unwrap();
        }
        let pg_duration = start.elapsed();
        
        // MySQL column extraction
        let start = Instant::now();
        for _ in 0..1000 {
            let _columns = mysql_parser.extract_insert_columns_fast(mysql_query).unwrap();
        }
        let mysql_duration = start.elapsed();
        
        println!("PostgreSQL column extraction: 1000 iterations in {:?}", pg_duration);
        println!("MySQL column extraction: 1000 iterations in {:?}", mysql_duration);
        
        assert!(pg_duration.as_millis() < 500, "PostgreSQL column extraction took too long: {:?}", pg_duration);
        assert!(mysql_duration.as_millis() < 500, "MySQL column extraction took too long: {:?}", mysql_duration);
    }

    #[test]
    fn test_simd_operations() {
        use crate::simd_ops;
        
        let test_data = b"SELECT * FROM users WHERE name = 'John' AND status = 'active'";
        
        // Test pattern finding
        let start = Instant::now();
        for _ in 0..10000 {
            let _pos = simd_ops::find_pattern_case_insensitive(test_data, b"SELECT");
        }
        let find_duration = start.elapsed();
        
        // Test whitespace skipping
        let whitespace_data = b"   \t\n\r   SELECT";
        let start = Instant::now();
        for _ in 0..10000 {
            let _pos = simd_ops::skip_whitespace_simd(whitespace_data, 0);
        }
        let ws_duration = start.elapsed();
        
        println!("SIMD pattern finding: 10000 iterations in {:?}", find_duration);
        println!("SIMD whitespace skipping: 10000 iterations in {:?}", ws_duration);
        
        assert!(find_duration.as_millis() < 100, "SIMD pattern finding took too long: {:?}", find_duration);
        assert!(ws_duration.as_millis() < 50, "SIMD whitespace skipping took too long: {:?}", ws_duration);
    }

    #[test]
    fn test_memory_allocation_patterns() {
        // Test pre-allocated vs dynamic allocation
        let test_query = r#"INSERT INTO users (id, name) VALUES (1, 'test');"#;
        let iterations = 100;
        
        // Test reusing parser (pre-allocated buffers)
        let mut parser = OptimizedPostgresParser::new(1024);
        let start = Instant::now();
        for _ in 0..iterations {
            let _result = parser.tokenize_optimized(test_query).unwrap();
        }
        let reuse_duration = start.elapsed();
        
        // Test creating new parser each time (dynamic allocation)
        let start = Instant::now();
        for _ in 0..iterations {
            let mut new_parser = OptimizedPostgresParser::new(1024);
            let _result = new_parser.tokenize_optimized(test_query).unwrap();
        }
        let new_duration = start.elapsed();
        
        println!("Parser reuse: {} iterations in {:?}", iterations, reuse_duration);
        println!("New parser each time: {} iterations in {:?}", iterations, new_duration);
        
        // Reusing should be faster than creating new instances
        assert!(reuse_duration < new_duration, 
               "Parser reuse should be faster: {:?} vs {:?}", reuse_duration, new_duration);
    }
}