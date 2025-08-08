use std::borrow::Cow;
use std::io::{BufRead, BufReader, Read};
use std::str;

use crate::utils::ListQueryResult;
use crate::DumpFileError;

/// High-performance SQL parser with zero-copy string processing
pub struct OptimizedSqlParser {
    buffer: Vec<u8>,
    line_buffer: Vec<u8>,
    stack: Vec<u8>,
    capacity: usize,
}

impl OptimizedSqlParser {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            line_buffer: Vec::with_capacity(1024),
            stack: Vec::with_capacity(16),
            capacity,
        }
    }

    /// Parse SQL dump with minimal allocations and zero-copy string operations
    pub fn parse_dump<R: Read, F>(
        &mut self,
        mut reader: BufReader<R>,
        mut callback: F,
    ) -> Result<(), DumpFileError>
    where
        F: FnMut(&str) -> ListQueryResult,
    {
        self.buffer.clear();
        let mut _bytes_read = 0;

        // Read entire input in chunks to minimize system calls
        loop {
            self.line_buffer.clear();
            match reader.read_until(b'\n', &mut self.line_buffer) {
                Ok(0) => break, // EOF
                Ok(n) => {
                    _bytes_read += n;
                    self.buffer.extend_from_slice(&self.line_buffer);

                    // Process buffer when it gets large enough or at EOF
                    if self.buffer.len() >= self.capacity / 2 {
                        self.process_buffer(&mut callback)?;
                    }
                }
                Err(e) => return Err(DumpFileError::ReadError(e)),
            }
        }

        // Process remaining buffer
        if !self.buffer.is_empty() {
            self.process_buffer(&mut callback)?;
        }

        Ok(())
    }

    fn process_buffer<F>(&mut self, callback: &mut F) -> Result<(), DumpFileError>
    where
        F: FnMut(&str) -> ListQueryResult,
    {
        let mut start = 0;
        let mut in_string = false;
        let mut in_comment = false;
        let mut escape_next = false;

        // SIMD-friendly byte scanning
        for (i, &byte) in self.buffer.iter().enumerate() {
            match byte {
                b'\'' if !in_comment && !escape_next => {
                    in_string = !in_string;
                    if in_string {
                        self.stack.push(b'\'');
                    } else if let Some(&b'\'') = self.stack.last() {
                        self.stack.pop();
                    }
                }
                b'(' if !in_string && !in_comment => {
                    self.stack.push(b'(');
                }
                b')' if !in_string && !in_comment => {
                    if let Some(&b'(') = self.stack.last() {
                        self.stack.pop();
                    }
                }
                b'-' if !in_string && i + 1 < self.buffer.len() && self.buffer[i + 1] == b'-' => {
                    in_comment = true;
                }
                b'\n' => {
                    in_comment = false;
                }
                b';' if !in_string && !in_comment && self.stack.is_empty() => {
                    // Found complete statement
                    let statement_bytes = &self.buffer[start..=i];
                    if let Ok(statement_str) = str::from_utf8(statement_bytes) {
                        if callback(statement_str) == ListQueryResult::Break {
                            return Ok(());
                        }
                    }
                    start = i + 1;
                    self.stack.clear();
                }
                b'\\' if in_string => {
                    escape_next = !escape_next;
                    continue;
                }
                _ => {}
            }
            escape_next = false;
        }

        // Keep incomplete statement for next batch
        if start < self.buffer.len() {
            self.buffer.drain(0..start);
        } else {
            self.buffer.clear();
        }

        Ok(())
    }
}

/// Zero-copy string holder that avoids allocations
#[derive(Debug)]
pub struct StringRef<'a> {
    data: Cow<'a, str>,
}

impl<'a> StringRef<'a> {
    pub fn borrowed(s: &'a str) -> Self {
        Self {
            data: Cow::Borrowed(s),
        }
    }

    pub fn owned(s: String) -> Self {
        Self {
            data: Cow::Owned(s),
        }
    }

    pub fn as_str(&self) -> &str {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// SIMD-optimized byte operations for SQL parsing
pub mod simd_ops {
    #[cfg(target_arch = "x86_64")]
    use std::arch::x86_64::*;

    /// Fast search for SQL delimiters using SIMD when available
    pub fn find_sql_delimiter(haystack: &[u8], delimiter: u8) -> Option<usize> {
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return unsafe { find_delimiter_avx2(haystack, delimiter) };
            }
        }

        // Fallback to standard search
        haystack.iter().position(|&b| b == delimiter)
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn find_delimiter_avx2(haystack: &[u8], delimiter: u8) -> Option<usize> {
        if haystack.len() < 32 {
            return haystack.iter().position(|&b| b == delimiter);
        }

        let delimiter_vec = _mm256_set1_epi8(delimiter as i8);
        let mut offset = 0;

        while offset + 32 <= haystack.len() {
            let chunk = _mm256_loadu_si256(haystack.as_ptr().add(offset) as *const __m256i);
            let cmp = _mm256_cmpeq_epi8(chunk, delimiter_vec);
            let mask = _mm256_movemask_epi8(cmp);

            if mask != 0 {
                return Some(offset + mask.trailing_zeros() as usize);
            }
            offset += 32;
        }

        // Handle remaining bytes
        haystack[offset..]
            .iter()
            .position(|&b| b == delimiter)
            .map(|pos| offset + pos)
    }
}

/// Memory-mapped file reader for extremely large dumps
pub struct MmapReader {
    #[cfg(unix)]
    mmap: memmap2::Mmap,
    offset: usize,
}

#[cfg(unix)]
impl MmapReader {
    pub fn new(file: std::fs::File) -> Result<Self, std::io::Error> {
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self { mmap, offset: 0 })
    }

    pub fn read_chunk(&mut self, size: usize) -> Option<&[u8]> {
        if self.offset >= self.mmap.len() {
            return None;
        }

        let end = std::cmp::min(self.offset + size, self.mmap.len());
        let chunk = &self.mmap[self.offset..end];
        self.offset = end;

        Some(chunk)
    }

    pub fn remaining(&self) -> usize {
        self.mmap.len().saturating_sub(self.offset)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_optimized_parser() {
        let sql = "INSERT INTO users (id, name) VALUES (1, 'John'); INSERT INTO posts (id, title) VALUES (1, 'Hello');";
        let reader = BufReader::new(Cursor::new(sql.as_bytes()));

        let mut parser = OptimizedSqlParser::new(8192);
        let mut statements = Vec::new();

        parser
            .parse_dump(reader, |stmt| {
                statements.push(stmt.to_string());
                ListQueryResult::Continue
            })
            .unwrap();

        assert_eq!(statements.len(), 2);
        assert!(statements[0].contains("INSERT INTO users"));
        assert!(statements[1].contains("INSERT INTO posts"));
    }

    #[test]
    fn test_simd_delimiter_search() {
        let data = b"SELECT * FROM users WHERE id = 1; SELECT * FROM posts;";
        let pos = simd_ops::find_sql_delimiter(data, b';');
        // The first semicolon is at position 32 (0-indexed)
        assert_eq!(pos, Some(32));
    }
}
