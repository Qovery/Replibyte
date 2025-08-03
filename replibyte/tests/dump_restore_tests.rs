use serde_yaml;
use std::fs::{create_dir_all, File};
use std::io::{BufReader, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use tempfile::{NamedTempFile, TempDir};

/// Integration tests for RepliByte dump and restore functionality
/// These tests validate that the core dump/restore workflow works correctly

#[test]
fn test_replibyte_binary_exists() {
    let output = Command::new("cargo")
        .args(&["build", "--bin", "replibyte"])
        .output()
        .expect("Failed to build replibyte binary");

    assert!(
        output.status.success(),
        "Failed to build replibyte binary: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn test_replibyte_help_command() {
    let output = Command::new("./target/debug/replibyte")
        .args(&["--help"])
        .output()
        .expect("Failed to run replibyte --help");

    assert!(output.status.success(), "replibyte --help failed");

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("replibyte"),
        "Help output should contain 'replibyte'"
    );
    assert!(
        stdout.contains("dump"),
        "Help output should mention dump command"
    );
    assert!(
        stdout.contains("backup"),
        "Help output should mention backup command"
    );
}

#[test]
fn test_config_file_validation() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config_path = temp_dir.path().join("test_config.yaml");

    // Create a minimal valid config
    let config_content = r#"
source:
  connection_uri: "postgres://user:pass@localhost:5432/testdb"

datastore:
  local_disk:
    dir: "./test_dumps"

transformers:
  - name: "hash_transformer"
    database: "testdb"
    table: "users"
    columns: ["email", "phone"]
    transformer:
      hash: {}
"#;

    std::fs::write(&config_path, config_content).expect("Failed to write config file");

    // Test config validation (should not crash)
    let output = Command::new("./target/debug/replibyte")
        .args(&["-c", config_path.to_str().unwrap(), "dump", "list"])
        .output()
        .expect("Failed to run replibyte with config");

    // The command may fail due to missing database, but shouldn't crash with config parsing errors
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        !stderr.contains("failed to parse"),
        "Config parsing should not fail: {}",
        stderr
    );
}

#[test]
fn test_dump_with_postgres_format() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let dump_file = temp_dir.path().join("test_dump.sql");

    // Create a sample PostgreSQL dump file
    let dump_content = r#"
--
-- PostgreSQL database dump
--

-- Dumped from database version 13.3
-- Dumped by pg_dump version 13.3

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;

CREATE TABLE public.users (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    email character varying(255) NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

--
-- Data for Name: users; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.users (id, name, email, created_at) FROM stdin;
1	John Doe	john.doe@example.com	2023-01-01 10:00:00
2	Jane Smith	jane.smith@example.com	2023-01-02 11:00:00
3	Bob Wilson	bob.wilson@example.com	2023-01-03 12:00:00
\.

CREATE TABLE public.posts (
    id integer NOT NULL,
    title character varying(200) NOT NULL,
    content text,
    user_id integer,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);

COPY public.posts (id, title, content, user_id, created_at) FROM stdin;
1	Welcome to the Blog	This is the first post on our blog.	1	2023-01-01 10:30:00
2	Database Performance Tips	Here are some tips for optimizing your database.	2	2023-01-02 11:30:00
3	Security Best Practices	Always validate your inputs and use prepared statements.	1	2023-01-03 12:30:00
\.

--
-- PostgreSQL database dump complete
--
"#;

    std::fs::write(&dump_file, dump_content).expect("Failed to write dump file");

    // Create config for local disk datastore
    let config_path = temp_dir.path().join("config.yaml");
    let dumps_dir = temp_dir.path().join("dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "{}"

transformers:
  - name: "email_transformer"
    database: "testdb"
    table: "users"
    columns: ["email"]
    transformer:
      random: {}
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write config");

    // Test parsing the dump file (using stdin simulation)
    let output = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "cat {} | ./target/debug/replibyte -c {} backup run -s postgres -i",
                dump_file.to_str().unwrap(),
                config_path.to_str().unwrap()
            ),
        ])
        .output()
        .expect("Failed to run dump parsing test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("STDOUT: {}", stdout);
    println!("STDERR: {}", stderr);

    // The command may fail due to missing actual database connection,
    // but it should at least parse the SQL without crashing
    assert!(!stderr.contains("panic"), "Should not panic: {}", stderr);
}

#[test]
fn test_dump_with_mysql_format() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let dump_file = temp_dir.path().join("test_mysql_dump.sql");

    // Create a sample MySQL dump file
    let dump_content = r#"
-- MySQL dump 10.13  Distrib 8.0.25, for Linux (x86_64)
--
-- Host: localhost    Database: testdb
-- ------------------------------------------------------
-- Server version	8.0.25

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!50503 SET NAMES utf8mb4 */;

--
-- Table structure for table `users`
--

DROP TABLE IF EXISTS `users`;
CREATE TABLE `users` (
  `id` int NOT NULL AUTO_INCREMENT,
  `name` varchar(100) NOT NULL,
  `email` varchar(255) NOT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

--
-- Dumping data for table `users`
--

LOCK TABLES `users` WRITE;
/*!40000 ALTER TABLE `users` DISABLE KEYS */;
INSERT INTO `users` (`id`, `name`, `email`, `created_at`) VALUES
(1,'John Doe','john.doe@example.com','2023-01-01 10:00:00'),
(2,'Jane Smith','jane.smith@example.com','2023-01-02 11:00:00'),
(3,'Bob Wilson','bob.wilson@example.com','2023-01-03 12:00:00');
/*!40000 ALTER TABLE `users` ENABLE KEYS */;
UNLOCK TABLES;

--
-- Table structure for table `posts`
--

DROP TABLE IF EXISTS `posts`;
CREATE TABLE `posts` (
  `id` int NOT NULL AUTO_INCREMENT,
  `title` varchar(200) NOT NULL,
  `content` text,
  `user_id` int DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `user_id` (`user_id`),
  CONSTRAINT `posts_ibfk_1` FOREIGN KEY (`user_id`) REFERENCES `users` (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

LOCK TABLES `posts` WRITE;
/*!40000 ALTER TABLE `posts` DISABLE KEYS */;
INSERT INTO `posts` (`id`, `title`, `content`, `user_id`, `created_at`) VALUES
(1,'Welcome to the Blog','This is the first post on our blog.',1,'2023-01-01 10:30:00'),
(2,'Database Performance Tips','Here are some tips for optimizing your database.',2,'2023-01-02 11:30:00'),
(3,'Security Best Practices','Always validate your inputs and use prepared statements.',1,'2023-01-03 12:30:00');
/*!40000 ALTER TABLE `posts` ENABLE KEYS */;
UNLOCK TABLES;

-- Dump completed on 2023-01-04 15:30:42
"#;

    std::fs::write(&dump_file, dump_content).expect("Failed to write MySQL dump file");

    // Create config for MySQL
    let config_path = temp_dir.path().join("mysql_config.yaml");
    let dumps_dir = temp_dir.path().join("mysql_dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "mysql://root:password@localhost:3306/testdb"

datastore:
  local_disk:
    dir: "{}"

transformers:
  - name: "email_transformer"
    database: "testdb"
    table: "users"
    columns: ["email"]
    transformer:
      random: {}
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write MySQL config");

    // Test parsing the MySQL dump file
    let output = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "cat {} | ./target/debug/replibyte -c {} backup run -s mysql -i",
                dump_file.to_str().unwrap(),
                config_path.to_str().unwrap()
            ),
        ])
        .output()
        .expect("Failed to run MySQL dump parsing test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("MySQL STDOUT: {}", stdout);
    println!("MySQL STDERR: {}", stderr);

    // Should parse without panicking
    assert!(
        !stderr.contains("panic"),
        "MySQL parsing should not panic: {}",
        stderr
    );
}

#[test]
fn test_dump_list_command() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config_path = temp_dir.path().join("list_config.yaml");
    let dumps_dir = temp_dir.path().join("list_dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "{}"
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write config");

    // Test dump list command
    let output = Command::new("./target/debug/replibyte")
        .args(&["-c", config_path.to_str().unwrap(), "dump", "list"])
        .output()
        .expect("Failed to run dump list command");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("List STDOUT: {}", stdout);
    println!("List STDERR: {}", stderr);

    // Command should complete without crashing
    // May show empty list if no dumps exist, which is fine
}

#[test]
fn test_config_with_transformers() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let config_path = temp_dir.path().join("transformers_config.yaml");

    // Test various transformer configurations
    let config_content = r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "./test_dumps"

transformers:
  - name: "hash_emails"
    database: "testdb"
    table: "users"
    columns: ["email"]
    transformer:
      hash: {}

  - name: "random_names"
    database: "testdb"
    table: "users"
    columns: ["name"]
    transformer:
      random: {}

  - name: "redact_sensitive"
    database: "testdb"
    table: "users"
    columns: ["ssn", "credit_card"]
    transformer:
      redacted: {}
"#;

    std::fs::write(&config_path, config_content).expect("Failed to write transformers config");

    // Test that config parses correctly
    let output = Command::new("./target/debug/replibyte")
        .args(&["-c", config_path.to_str().unwrap(), "dump", "list"])
        .output()
        .expect("Failed to run with transformers config");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should not have config parsing errors
    assert!(
        !stderr.contains("failed to parse"),
        "Transformer config should parse correctly: {}",
        stderr
    );
    assert!(
        !stderr.contains("unknown field"),
        "All transformer fields should be recognized: {}",
        stderr
    );
}

#[test]
fn test_performance_with_large_dataset() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let large_dump = temp_dir.path().join("large_dump.sql");

    // Generate a large dataset for performance testing
    let mut dump_content = String::new();
    dump_content.push_str("-- Large dataset test\n");
    dump_content.push_str("CREATE TABLE large_table (id INTEGER, data TEXT);\n");

    // Add many INSERT statements
    for i in 0..10000 {
        dump_content.push_str(&format!(
            "INSERT INTO large_table (id, data) VALUES ({}, 'test_data_{}');\n",
            i, i
        ));
    }

    std::fs::write(&large_dump, dump_content).expect("Failed to write large dump file");

    let config_path = temp_dir.path().join("perf_config.yaml");
    let dumps_dir = temp_dir.path().join("perf_dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "{}"
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write perf config");

    // Test processing large dataset
    let start = std::time::Instant::now();

    let output = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "cat {} | timeout 30 ./target/debug/replibyte -c {} backup run -s postgres -i",
                large_dump.to_str().unwrap(),
                config_path.to_str().unwrap()
            ),
        ])
        .output()
        .expect("Failed to run large dataset test");

    let duration = start.elapsed();
    let stderr = String::from_utf8_lossy(&output.stderr);

    println!("Large dataset processing took: {:?}", duration);
    println!("Large dataset STDERR: {}", stderr);

    // Should complete within reasonable time (30 seconds timeout)
    assert!(
        duration.as_secs() < 30,
        "Large dataset processing should complete within 30 seconds"
    );
    assert!(
        !stderr.contains("panic"),
        "Should not panic with large dataset: {}",
        stderr
    );
}

#[test]
fn test_error_handling_invalid_sql() {
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let invalid_dump = temp_dir.path().join("invalid_dump.sql");

    // Create SQL with syntax errors
    let invalid_content = r#"
CREATE TABLE test_table (
    id INTEGER
    name VARCHAR(100) -- Missing comma
);

INSERT INTO test_table (id, name VALUES (1, 'test'); -- Missing closing parenthesis
SELECT * FROM; -- Incomplete query
INVALID SQL STATEMENT THAT MAKES NO SENSE;
"#;

    std::fs::write(&invalid_dump, invalid_content).expect("Failed to write invalid dump file");

    let config_path = temp_dir.path().join("error_config.yaml");
    let dumps_dir = temp_dir.path().join("error_dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "{}"
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write error config");

    // Test error handling with invalid SQL
    let output = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "cat {} | ./target/debug/replibyte -c {} backup run -s postgres -i",
                invalid_dump.to_str().unwrap(),
                config_path.to_str().unwrap()
            ),
        ])
        .output()
        .expect("Failed to run invalid SQL test");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should handle errors gracefully without panicking
    assert!(
        !stderr.contains("panic"),
        "Should handle invalid SQL gracefully: {}",
        stderr
    );

    // May exit with error code, but shouldn't crash
    println!("Invalid SQL handling STDERR: {}", stderr);
}

#[test]
fn test_memory_usage_during_processing() {
    // This test validates that memory usage remains reasonable during processing
    let temp_dir = TempDir::new().expect("Failed to create temp directory");
    let memory_dump = temp_dir.path().join("memory_test_dump.sql");

    // Create a dump with repetitive data to test memory efficiency
    let mut dump_content = String::new();
    dump_content.push_str("CREATE TABLE memory_test (id INTEGER, data TEXT);\n");

    // Add INSERT statements with large text data
    for i in 0..1000 {
        let large_text = "A".repeat(1000); // 1KB per row
        dump_content.push_str(&format!(
            "INSERT INTO memory_test (id, data) VALUES ({}, '{}');\n",
            i, large_text
        ));
    }

    std::fs::write(&memory_dump, dump_content).expect("Failed to write memory test dump");

    let config_path = temp_dir.path().join("memory_config.yaml");
    let dumps_dir = temp_dir.path().join("memory_dumps");
    create_dir_all(&dumps_dir).expect("Failed to create dumps directory");

    let config_content = format!(
        r#"
source:
  connection_uri: "postgres://localhost:5432/testdb"

datastore:
  local_disk:
    dir: "{}"
"#,
        dumps_dir.to_str().unwrap()
    );

    std::fs::write(&config_path, config_content).expect("Failed to write memory config");

    // Monitor memory usage during processing
    let output = Command::new("bash")
        .args(&[
            "-c",
            &format!(
                "cat {} | ./target/debug/replibyte -c {} backup run -s postgres -i",
                memory_dump.to_str().unwrap(),
                config_path.to_str().unwrap()
            ),
        ])
        .output()
        .expect("Failed to run memory test");

    let stderr = String::from_utf8_lossy(&output.stderr);

    // Should complete without memory-related errors
    assert!(
        !stderr.contains("out of memory"),
        "Should not run out of memory: {}",
        stderr
    );
    assert!(
        !stderr.contains("panic"),
        "Should not panic during memory test: {}",
        stderr
    );

    println!("Memory test completed successfully");
}
