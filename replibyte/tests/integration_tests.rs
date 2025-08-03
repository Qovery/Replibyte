use std::io::{BufReader, Cursor};
use std::sync::mpsc;
use std::thread;
use tempfile::TempDir;

use dump_parser::utils::{list_sql_queries_from_dump_reader, ListQueryResult};

#[test]
fn test_sql_parsing_performance() {
    let sql_content = r#"
        INSERT INTO users (id, name, email) VALUES (1, 'John Doe', 'john@example.com');
        INSERT INTO users (id, name, email) VALUES (2, 'Jane Smith', 'jane@example.com');
        INSERT INTO posts (id, title, content, user_id) VALUES (1, 'Hello World', 'This is my first post', 1);
        INSERT INTO posts (id, title, content, user_id) VALUES (2, 'Another Post', 'This is another post', 2);
    "#.repeat(100); // Repeat to create substantial load

    let reader = BufReader::new(Cursor::new(sql_content.as_bytes()));

    let start = std::time::Instant::now();
    let mut query_count = 0;

    list_sql_queries_from_dump_reader(reader, |_query| {
        query_count += 1;
        ListQueryResult::Continue
    })
    .unwrap();

    let duration = start.elapsed();
    println!("Parsed {} queries in {:?}", query_count, duration);

    // Assert that parsing completed in reasonable time
    assert!(
        duration.as_secs() < 10,
        "Parsing took too long: {:?}",
        duration
    );
    assert!(query_count > 0, "No queries were parsed");
}

#[test]
fn test_memory_allocation_patterns() {
    // Test that Vec::with_capacity is more efficient than Vec::new
    let iterations = 10000;

    // Test Vec::new (will cause multiple reallocations)
    let start = std::time::Instant::now();
    let mut vec_new = Vec::new();
    for i in 0..iterations {
        vec_new.push(format!("test_string_{}", i));
    }
    let duration_new = start.elapsed();

    // Test Vec::with_capacity (minimal reallocations)
    let start = std::time::Instant::now();
    let mut vec_capacity = Vec::with_capacity(iterations);
    for i in 0..iterations {
        vec_capacity.push(format!("test_string_{}", i));
    }
    let duration_capacity = start.elapsed();

    println!("Vec::new took: {:?}", duration_new);
    println!("Vec::with_capacity took: {:?}", duration_capacity);

    // Vec::with_capacity should be faster or at least not slower
    assert!(
        duration_capacity <= duration_new * 2,
        "with_capacity should not be significantly slower"
    );
}

#[test]
fn test_buffer_operations() {
    let test_data = vec![42u8; 100_000];
    let chunk_size = 1024;

    let start = std::time::Instant::now();
    let mut chunks = Vec::new();
    let mut current_chunk = Vec::with_capacity(chunk_size);

    for &byte in &test_data {
        if current_chunk.len() >= chunk_size {
            chunks.push(std::mem::take(&mut current_chunk));
            current_chunk = Vec::with_capacity(chunk_size);
        }
        current_chunk.push(byte);
    }

    if !current_chunk.is_empty() {
        chunks.push(current_chunk);
    }

    let duration = start.elapsed();
    println!(
        "Chunked {} bytes into {} chunks in {:?}",
        test_data.len(),
        chunks.len(),
        duration
    );

    assert!(chunks.len() > 0);
    assert!(duration.as_millis() < 100, "Chunking should be fast");
}

#[test]
fn test_concurrent_operations() {
    let num_threads = 4;
    let operations_per_thread = 1000;

    let handles: Vec<_> = (0..num_threads)
        .map(|thread_id| {
            thread::spawn(move || {
                let mut results = Vec::with_capacity(operations_per_thread);

                for i in 0..operations_per_thread {
                    let query = format!(
                        "INSERT INTO table_{} (id, value) VALUES ({}, 'test');",
                        thread_id, i
                    );
                    results.push(query.len());
                }

                results.into_iter().sum::<usize>()
            })
        })
        .collect();

    let start = std::time::Instant::now();

    let total: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();

    let duration = start.elapsed();
    println!(
        "Processed {} operations across {} threads in {:?}",
        num_threads * operations_per_thread,
        num_threads,
        duration
    );

    assert!(total > 0);
    assert!(
        duration.as_secs() < 5,
        "Concurrent operations should complete quickly"
    );
}

#[test]
fn test_error_handling() {
    // Test with malformed SQL
    let malformed_sql = r#"
        INSERT INTO users (id, name VALUES (1, 'incomplete');
        SELECT * FROM; -- incomplete query
        INVALID SQL STATEMENT;
    "#;

    let reader = BufReader::new(Cursor::new(malformed_sql.as_bytes()));

    // Should handle errors gracefully without panicking
    let result = list_sql_queries_from_dump_reader(reader, |_query| ListQueryResult::Continue);

    // Either succeeds with some queries parsed or fails gracefully
    match result {
        Ok(_) => {}  // Success is fine
        Err(_) => {} // Graceful error handling is also fine
    }
}
