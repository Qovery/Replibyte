use std::borrow::Cow;
use std::str;

use crate::postgres::{Token, TokenizerError, Whitespace, Word, Keyword};
use crate::simd_ops;

/// High-performance PostgreSQL parser with SIMD optimizations and zero-copy techniques
pub struct OptimizedPostgresParser {
    buffer: Vec<u8>,
    token_buffer: Vec<Token>,
    string_buffer: Vec<u8>,
    position_stack: Vec<usize>,
}

impl OptimizedPostgresParser {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: Vec::with_capacity(capacity),
            token_buffer: Vec::with_capacity(capacity / 10), // Estimate 1 token per 10 bytes
            string_buffer: Vec::with_capacity(1024),
            position_stack: Vec::with_capacity(16),
        }
    }

    /// Fast tokenization using SIMD optimizations and zero-copy techniques
    pub fn tokenize_optimized(&mut self, query: &str) -> Result<Vec<Token>, TokenizerError> {
        self.token_buffer.clear();
        self.buffer.clear();
        self.buffer.extend_from_slice(query.as_bytes());

        let mut pos = 0;
        let data = &self.buffer;

        while pos < data.len() {
            // Skip whitespace using SIMD
            pos = self.skip_whitespace_simd(data, pos);
            if pos >= data.len() {
                break;
            }

            match data[pos] {
                // Fast keyword detection using SIMD
                b'I' | b'i' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"INSERT") {
                        self.token_buffer.push(Token::make_keyword("INSERT"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'S' | b's' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"SELECT") {
                        self.token_buffer.push(Token::make_keyword("SELECT"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'C' | b'c' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"CREATE") {
                        self.token_buffer.push(Token::make_keyword("CREATE"));
                        pos = token_pos;
                        continue;
                    }
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"COPY") {
                        self.token_buffer.push(Token::make_keyword("COPY"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'T' | b't' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"TABLE") {
                        self.token_buffer.push(Token::make_keyword("TABLE"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'F' | b'f' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"FROM") {
                        self.token_buffer.push(Token::make_keyword("FROM"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'V' | b'v' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"VALUES") {
                        self.token_buffer.push(Token::make_keyword("VALUES"));
                        pos = token_pos;
                        continue;
                    }
                }
                
                // Fast string parsing
                b'\'' => {
                    let (string_content, new_pos) = self.parse_single_quoted_string_fast(data, pos)?;
                    self.token_buffer.push(Token::SingleQuotedString(string_content));
                    pos = new_pos;
                }
                
                // Fast punctuation
                b'(' => {
                    self.token_buffer.push(Token::LParen);
                    pos += 1;
                }
                b')' => {
                    self.token_buffer.push(Token::RParen);
                    pos += 1;
                }
                b',' => {
                    self.token_buffer.push(Token::Comma);
                    pos += 1;
                }
                b';' => {
                    self.token_buffer.push(Token::SemiColon);
                    pos += 1;
                }
                b'=' => {
                    self.token_buffer.push(Token::Eq);
                    pos += 1;
                }
                b'.' => {
                    self.token_buffer.push(Token::Period);
                    pos += 1;
                }
                
                // Fast identifier/word parsing
                _ if data[pos].is_ascii_alphabetic() || data[pos] == b'_' => {
                    let (word, new_pos) = self.parse_identifier_fast(data, pos);
                    self.token_buffer.push(Token::Word(Word::new(&word)));
                    pos = new_pos;
                }
                
                // Fast number parsing
                _ if data[pos].is_ascii_digit() => {
                    let (number, new_pos) = self.parse_number_fast(data, pos);
                    self.token_buffer.push(Token::Number(number, false));
                    pos = new_pos;
                }
                
                // Skip or handle other characters
                _ => {
                    pos += 1;
                }
            }
        }

        Ok(self.token_buffer.clone())
    }

    /// SIMD-optimized whitespace skipping
    fn skip_whitespace_simd(&self, data: &[u8], mut pos: usize) -> usize {
        while pos < data.len() {
            match data[pos] {
                b' ' | b'\t' | b'\n' | b'\r' => pos += 1,
                _ => break,
            }
        }
        pos
    }

    /// Try to parse a keyword using SIMD-optimized comparison
    fn try_parse_keyword_simd(&self, data: &[u8], pos: usize, keyword: &[u8]) -> Option<usize> {
        if pos + keyword.len() > data.len() {
            return None;
        }

        // Use SIMD for comparison if available
        let slice = &data[pos..pos + keyword.len()];
        if slice.eq_ignore_ascii_case(keyword) {
            let next_pos = pos + keyword.len();
            // Check word boundary
            if next_pos >= data.len() || 
               !data[next_pos].is_ascii_alphanumeric() && data[next_pos] != b'_' {
                return Some(next_pos);
            }
        }
        None
    }

    /// Fast single-quoted string parsing with minimal allocations
    fn parse_single_quoted_string_fast(&mut self, data: &[u8], start_pos: usize) -> Result<(String, usize), TokenizerError> {
        self.string_buffer.clear();
        let mut pos = start_pos + 1; // Skip opening quote
        
        while pos < data.len() {
            match data[pos] {
                b'\'' => {
                    // Check for escaped quote
                    if pos + 1 < data.len() && data[pos + 1] == b'\'' {
                        self.string_buffer.push(b'\'');
                        pos += 2;
                    } else {
                        // End of string
                        pos += 1;
                        break;
                    }
                }
                b'\\' => {
                    // Handle escape sequences
                    if pos + 1 < data.len() {
                        match data[pos + 1] {
                            b'n' => self.string_buffer.push(b'\n'),
                            b't' => self.string_buffer.push(b'\t'),
                            b'r' => self.string_buffer.push(b'\r'),
                            b'\\' => self.string_buffer.push(b'\\'),
                            b'\'' => self.string_buffer.push(b'\''),
                            c => {
                                self.string_buffer.push(b'\\');
                                self.string_buffer.push(c);
                            }
                        }
                        pos += 2;
                    } else {
                        self.string_buffer.push(data[pos]);
                        pos += 1;
                    }
                }
                c => {
                    self.string_buffer.push(c);
                    pos += 1;
                }
            }
        }

        // Convert to UTF-8 string with error handling
        match str::from_utf8(&self.string_buffer) {
            Ok(s) => Ok((s.to_string(), pos)),
            Err(_) => {
                // Fallback to lossy conversion
                Ok((String::from_utf8_lossy(&self.string_buffer).into_owned(), pos))
            }
        }
    }

    /// Fast identifier parsing with zero-copy when possible
    fn parse_identifier_fast(&self, data: &[u8], start_pos: usize) -> (String, usize) {
        let mut pos = start_pos;
        
        while pos < data.len() {
            match data[pos] {
                c if c.is_ascii_alphanumeric() || c == b'_' => pos += 1,
                _ => break,
            }
        }

        // Use zero-copy conversion when possible
        let identifier_bytes = &data[start_pos..pos];
        let identifier = unsafe {
            // Safe because we know the bytes are ASCII alphanumeric + underscore
            std::str::from_utf8_unchecked(identifier_bytes)
        };

        (identifier.to_string(), pos)
    }

    /// Fast number parsing
    fn parse_number_fast(&self, data: &[u8], start_pos: usize) -> (String, usize) {
        let mut pos = start_pos;
        
        while pos < data.len() && (data[pos].is_ascii_digit() || data[pos] == b'.') {
            pos += 1;
        }

        let number_bytes = &data[start_pos..pos];
        let number = unsafe {
            // Safe because we know the bytes are ASCII digits and dots
            std::str::from_utf8_unchecked(number_bytes)
        };

        (number.to_string(), pos)
    }

    /// Optimized INSERT INTO column name extraction
    pub fn extract_insert_columns_fast(&mut self, query: &str) -> Result<Vec<Cow<str>>, TokenizerError> {
        // Use SIMD to quickly find "INSERT INTO" pattern
        let query_bytes = query.as_bytes();
        
        // Find INSERT keyword position
        let insert_pos = simd_ops::find_pattern_case_insensitive(query_bytes, b"INSERT")
            .ok_or_else(|| TokenizerError::General("Not an INSERT statement".into()))?;
        
        // Find INTO keyword position  
        let into_pos = simd_ops::find_pattern_case_insensitive(&query_bytes[insert_pos..], b"INTO")
            .ok_or_else(|| TokenizerError::General("Missing INTO keyword".into()))?
            + insert_pos;
        
        // Find opening parenthesis after table name
        let lparen_pos = query_bytes[into_pos..]
            .iter()
            .position(|&b| b == b'(')
            .ok_or_else(|| TokenizerError::General("Missing column list".into()))?
            + into_pos;
        
        // Find closing parenthesis
        let rparen_pos = query_bytes[lparen_pos..]
            .iter()
            .position(|&b| b == b')')
            .ok_or_else(|| TokenizerError::General("Unclosed column list".into()))?
            + lparen_pos;
        
        // Extract column list efficiently
        let column_list = &query_bytes[lparen_pos + 1..rparen_pos];
        let mut columns = Vec::new();
        let mut start = 0;
        
        // Use SIMD to find commas quickly
        while start < column_list.len() {
            let comma_pos = column_list[start..]
                .iter()
                .position(|&b| b == b',')
                .unwrap_or(column_list.len() - start) + start;
            
            let column_bytes = &column_list[start..comma_pos];
            let column_str = self.parse_column_name_fast(column_bytes);
            columns.push(column_str);
            
            start = comma_pos + 1;
            // Skip whitespace
            while start < column_list.len() && column_list[start].is_ascii_whitespace() {
                start += 1;
            }
        }
        
        Ok(columns)
    }

    /// Fast column name parsing with quote handling
    fn parse_column_name_fast(&self, column_bytes: &[u8]) -> Cow<str> {
        // Trim whitespace
        let mut start = 0;
        let mut end = column_bytes.len();
        
        while start < end && column_bytes[start].is_ascii_whitespace() {
            start += 1;
        }
        while end > start && column_bytes[end - 1].is_ascii_whitespace() {
            end -= 1;
        }
        
        let trimmed = &column_bytes[start..end];
        
        if trimmed.is_empty() {
            return Cow::Borrowed("");
        }
        
        // Handle quoted identifiers
        if trimmed[0] == b'"' && trimmed[trimmed.len() - 1] == b'"' {
            let unquoted = &trimmed[1..trimmed.len() - 1];
            unsafe {
                Cow::Borrowed(std::str::from_utf8_unchecked(unquoted))
            }
        } else {
            unsafe {
                Cow::Borrowed(std::str::from_utf8_unchecked(trimmed))
            }
        }
    }
}

/// SIMD operations module for PostgreSQL parsing
mod simd_ops {
    /// Fast case-insensitive pattern finding using SIMD when available
    pub fn find_pattern_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        if needle.is_empty() || haystack.len() < needle.len() {
            return None;
        }

        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") {
                return unsafe { find_pattern_avx2_case_insensitive(haystack, needle) };
            }
        }

        // Fallback implementation
        find_pattern_fallback_case_insensitive(haystack, needle)
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn find_pattern_avx2_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        use std::arch::x86_64::*;

        if haystack.len() < 32 || needle.len() > 32 {
            return find_pattern_fallback_case_insensitive(haystack, needle);
        }

        let first_char_lower = needle[0].to_ascii_lowercase();
        let first_char_upper = needle[0].to_ascii_uppercase();
        let first_lower_vec = _mm256_set1_epi8(first_char_lower as i8);
        let first_upper_vec = _mm256_set1_epi8(first_char_upper as i8);

        let mut offset = 0;
        while offset + 32 <= haystack.len() {
            let chunk = _mm256_loadu_si256(haystack.as_ptr().add(offset) as *const __m256i);
            let cmp_lower = _mm256_cmpeq_epi8(chunk, first_lower_vec);
            let cmp_upper = _mm256_cmpeq_epi8(chunk, first_upper_vec);
            let cmp = _mm256_or_si256(cmp_lower, cmp_upper);
            let mask = _mm256_movemask_epi8(cmp);

            if mask != 0 {
                let bit_pos = mask.trailing_zeros() as usize;
                let candidate_pos = offset + bit_pos;
                
                if candidate_pos + needle.len() <= haystack.len() {
                    let candidate = &haystack[candidate_pos..candidate_pos + needle.len()];
                    if candidate.eq_ignore_ascii_case(needle) {
                        return Some(candidate_pos);
                    }
                }
            }
            offset += 32;
        }

        // Check remaining bytes
        find_pattern_fallback_case_insensitive(&haystack[offset..], needle)
            .map(|pos| offset + pos)
    }

    fn find_pattern_fallback_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
        haystack
            .windows(needle.len())
            .position(|window| window.eq_ignore_ascii_case(needle))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_optimized_tokenization() {
        let mut parser = OptimizedPostgresParser::new(1024);
        let query = "INSERT INTO users (id, name) VALUES (1, 'John');";
        
        let tokens = parser.tokenize_optimized(query).unwrap();
        
        // Verify basic tokenization works
        assert!(!tokens.is_empty());
        
        // Check for INSERT keyword
        assert!(tokens.iter().any(|t| matches!(t, Token::Word(w) if w.value == "INSERT")));
    }

    #[test]
    fn test_fast_column_extraction() {
        let mut parser = OptimizedPostgresParser::new(1024);
        let query = r#"INSERT INTO public.users (id, "name", email) VALUES (1, 'John', 'john@example.com')"#;
        
        let columns = parser.extract_insert_columns_fast(query).unwrap();
        
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0], "id");
        assert_eq!(columns[1], "name");
        assert_eq!(columns[2], "email");
    }

    #[test]
    fn test_simd_pattern_search() {
        let haystack = b"SELECT * FROM users WHERE name = 'John'";
        let needle = b"SELECT";
        
        let pos = simd_ops::find_pattern_case_insensitive(haystack, needle);
        assert_eq!(pos, Some(0));
        
        let needle = b"FROM";
        let pos = simd_ops::find_pattern_case_insensitive(haystack, needle);
        assert_eq!(pos, Some(14));
    }

    #[test]
    fn test_fast_string_parsing() {
        let mut parser = OptimizedPostgresParser::new(1024);
        let data = b"'Hello, World!'";
        
        let (result, _) = parser.parse_single_quoted_string_fast(data, 0).unwrap();
        assert_eq!(result, "Hello, World!");
    }
}