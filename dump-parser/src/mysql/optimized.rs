use std::borrow::Cow;
use std::str;

use crate::mysql::{Token, TokenizerError, Whitespace, Word, Keyword};
use crate::simd_ops;

/// High-performance MySQL parser with SIMD optimizations and zero-copy techniques
pub struct OptimizedMySQLParser {
    buffer: Vec<u8>,
    token_buffer: Vec<Token>,
    string_buffer: Vec<u8>,
    position_stack: Vec<usize>,
}

impl OptimizedMySQLParser {
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
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"INTO") {
                        self.token_buffer.push(Token::make_keyword("INTO"));
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
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"SET") {
                        self.token_buffer.push(Token::make_keyword("SET"));
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
                b'W' | b'w' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"WHERE") {
                        self.token_buffer.push(Token::make_keyword("WHERE"));
                        pos = token_pos;
                        continue;
                    }
                }
                b'U' | b'u' => {
                    if let Some(token_pos) = self.try_parse_keyword_simd(data, pos, b"UPDATE") {
                        self.token_buffer.push(Token::make_keyword("UPDATE"));
                        pos = token_pos;
                        continue;
                    }
                }
                
                // Fast string parsing (MySQL supports both single and double quotes)
                b'\'' => {
                    let (string_content, new_pos) = self.parse_single_quoted_string_fast(data, pos)?;
                    self.token_buffer.push(Token::SingleQuotedString(string_content));
                    pos = new_pos;
                }
                b'"' => {
                    let (string_content, new_pos) = self.parse_double_quoted_string_fast(data, pos)?;
                    self.token_buffer.push(Token::SingleQuotedString(string_content)); // MySQL treats both the same
                    pos = new_pos;
                }
                
                // MySQL backtick identifiers
                b'`' => {
                    let (identifier, new_pos) = self.parse_backtick_identifier_fast(data, pos)?;
                    self.token_buffer.push(Token::Word(Word::new(&identifier)));
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
                
                // MySQL comments
                b'-' if pos + 1 < data.len() && data[pos + 1] == b'-' => {
                    pos = self.skip_line_comment(data, pos);
                }
                b'/' if pos + 1 < data.len() && data[pos + 1] == b'*' => {
                    pos = self.skip_block_comment(data, pos)?;
                }
                b'#' => {
                    pos = self.skip_line_comment(data, pos);
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
        // Use SIMD for bulk whitespace detection when available
        #[cfg(target_arch = "x86_64")]
        {
            if is_x86_feature_detected!("avx2") && data.len() - pos >= 32 {
                return unsafe { self.skip_whitespace_avx2(data, pos) };
            }
        }

        // Fallback scalar implementation
        while pos < data.len() {
            match data[pos] {
                b' ' | b'\t' | b'\n' | b'\r' => pos += 1,
                _ => break,
            }
        }
        pos
    }

    #[cfg(target_arch = "x86_64")]
    unsafe fn skip_whitespace_avx2(&self, data: &[u8], mut pos: usize) -> usize {
        use std::arch::x86_64::*;

        let space_vec = _mm256_set1_epi8(b' ' as i8);
        let tab_vec = _mm256_set1_epi8(b'\t' as i8);
        let newline_vec = _mm256_set1_epi8(b'\n' as i8);
        let cr_vec = _mm256_set1_epi8(b'\r' as i8);

        while pos + 32 <= data.len() {
            let chunk = _mm256_loadu_si256(data.as_ptr().add(pos) as *const __m256i);
            
            let is_space = _mm256_cmpeq_epi8(chunk, space_vec);
            let is_tab = _mm256_cmpeq_epi8(chunk, tab_vec);
            let is_newline = _mm256_cmpeq_epi8(chunk, newline_vec);
            let is_cr = _mm256_cmpeq_epi8(chunk, cr_vec);
            
            let is_whitespace = _mm256_or_si256(
                _mm256_or_si256(is_space, is_tab),
                _mm256_or_si256(is_newline, is_cr)
            );
            
            let mask = _mm256_movemask_epi8(is_whitespace);
            
            if mask == 0xFFFFFFFF {
                // All bytes are whitespace
                pos += 32;
            } else if mask == 0 {
                // No whitespace found
                break;
            } else {
                // Mixed - find first non-whitespace
                let non_ws_pos = (!mask).trailing_zeros() as usize;
                pos += non_ws_pos;
                break;
            }
        }

        // Handle remaining bytes with scalar code
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

    /// Fast single-quoted string parsing with MySQL escape handling
    fn parse_single_quoted_string_fast(&mut self, data: &[u8], start_pos: usize) -> Result<(String, usize), TokenizerError> {
        self.string_buffer.clear();
        let mut pos = start_pos + 1; // Skip opening quote
        
        while pos < data.len() {
            match data[pos] {
                b'\'' => {
                    // Check for escaped quote (MySQL uses '' for literal quote)
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
                    // Handle MySQL escape sequences
                    if pos + 1 < data.len() {
                        match data[pos + 1] {
                            b'n' => self.string_buffer.push(b'\n'),
                            b't' => self.string_buffer.push(b'\t'),
                            b'r' => self.string_buffer.push(b'\r'),
                            b'\\' => self.string_buffer.push(b'\\'),
                            b'\'' => self.string_buffer.push(b'\''),
                            b'"' => self.string_buffer.push(b'"'),
                            b'0' => self.string_buffer.push(b'\0'),
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

        match str::from_utf8(&self.string_buffer) {
            Ok(s) => Ok((s.to_string(), pos)),
            Err(_) => Ok((String::from_utf8_lossy(&self.string_buffer).into_owned(), pos))
        }
    }

    /// Fast double-quoted string parsing
    fn parse_double_quoted_string_fast(&mut self, data: &[u8], start_pos: usize) -> Result<(String, usize), TokenizerError> {
        self.string_buffer.clear();
        let mut pos = start_pos + 1; // Skip opening quote
        
        while pos < data.len() {
            match data[pos] {
                b'"' => {
                    pos += 1;
                    break;
                }
                b'\\' => {
                    if pos + 1 < data.len() {
                        match data[pos + 1] {
                            b'n' => self.string_buffer.push(b'\n'),
                            b't' => self.string_buffer.push(b'\t'),
                            b'r' => self.string_buffer.push(b'\r'),
                            b'\\' => self.string_buffer.push(b'\\'),
                            b'"' => self.string_buffer.push(b'"'),
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

        match str::from_utf8(&self.string_buffer) {
            Ok(s) => Ok((s.to_string(), pos)),
            Err(_) => Ok((String::from_utf8_lossy(&self.string_buffer).into_owned(), pos))
        }
    }

    /// Fast backtick identifier parsing (MySQL specific)
    fn parse_backtick_identifier_fast(&mut self, data: &[u8], start_pos: usize) -> Result<(String, usize), TokenizerError> {
        self.string_buffer.clear();
        let mut pos = start_pos + 1; // Skip opening backtick
        
        while pos < data.len() {
            match data[pos] {
                b'`' => {
                    // Check for escaped backtick
                    if pos + 1 < data.len() && data[pos + 1] == b'`' {
                        self.string_buffer.push(b'`');
                        pos += 2;
                    } else {
                        // End of identifier
                        pos += 1;
                        break;
                    }
                }
                c => {
                    self.string_buffer.push(c);
                    pos += 1;
                }
            }
        }

        match str::from_utf8(&self.string_buffer) {
            Ok(s) => Ok((s.to_string(), pos)),
            Err(_) => Ok((String::from_utf8_lossy(&self.string_buffer).into_owned(), pos))
        }
    }

    /// Fast identifier parsing
    fn parse_identifier_fast(&self, data: &[u8], start_pos: usize) -> (String, usize) {
        let mut pos = start_pos;
        
        while pos < data.len() {
            match data[pos] {
                c if c.is_ascii_alphanumeric() || c == b'_' || c == b'$' => pos += 1,
                _ => break,
            }
        }

        let identifier_bytes = &data[start_pos..pos];
        let identifier = unsafe {
            std::str::from_utf8_unchecked(identifier_bytes)
        };

        (identifier.to_string(), pos)
    }

    /// Fast number parsing with MySQL decimal support
    fn parse_number_fast(&self, data: &[u8], start_pos: usize) -> (String, usize) {
        let mut pos = start_pos;
        let mut has_dot = false;
        
        while pos < data.len() {
            match data[pos] {
                c if c.is_ascii_digit() => pos += 1,
                b'.' if !has_dot => {
                    has_dot = true;
                    pos += 1;
                }
                b'e' | b'E' => {
                    pos += 1;
                    if pos < data.len() && (data[pos] == b'+' || data[pos] == b'-') {
                        pos += 1;
                    }
                    while pos < data.len() && data[pos].is_ascii_digit() {
                        pos += 1;
                    }
                    break;
                }
                _ => break,
            }
        }

        let number_bytes = &data[start_pos..pos];
        let number = unsafe {
            std::str::from_utf8_unchecked(number_bytes)
        };

        (number.to_string(), pos)
    }

    /// Skip line comment (-- or #)
    fn skip_line_comment(&self, data: &[u8], mut pos: usize) -> usize {
        while pos < data.len() && data[pos] != b'\n' {
            pos += 1;
        }
        pos
    }

    /// Skip block comment (/* ... */)
    fn skip_block_comment(&self, data: &[u8], mut pos: usize) -> Result<usize, TokenizerError> {
        pos += 2; // Skip /*
        
        while pos + 1 < data.len() {
            if data[pos] == b'*' && data[pos + 1] == b'/' {
                return Ok(pos + 2);
            }
            pos += 1;
        }
        
        Err(TokenizerError::General("Unterminated block comment".into()))
    }

    /// Optimized INSERT INTO column name extraction for MySQL
    pub fn extract_insert_columns_fast(&mut self, query: &str) -> Result<Vec<Cow<str>>, TokenizerError> {
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
        
        while start < column_list.len() {
            let comma_pos = column_list[start..]
                .iter()
                .position(|&b| b == b',')
                .unwrap_or(column_list.len() - start) + start;
            
            let column_bytes = &column_list[start..comma_pos];
            let column_str = self.parse_mysql_column_name_fast(column_bytes);
            columns.push(column_str);
            
            start = comma_pos + 1;
            // Skip whitespace
            while start < column_list.len() && column_list[start].is_ascii_whitespace() {
                start += 1;
            }
        }
        
        Ok(columns)
    }

    /// Fast MySQL column name parsing with backtick and quote handling
    fn parse_mysql_column_name_fast(&self, column_bytes: &[u8]) -> Cow<str> {
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
        
        // Handle MySQL quoted identifiers (backticks, single quotes, double quotes)
        if trimmed.len() >= 2 {
            let first = trimmed[0];
            let last = trimmed[trimmed.len() - 1];
            
            if (first == b'`' && last == b'`') ||
               (first == b'"' && last == b'"') ||
               (first == b'\'' && last == b'\'') {
                let unquoted = &trimmed[1..trimmed.len() - 1];
                return unsafe {
                    Cow::Borrowed(std::str::from_utf8_unchecked(unquoted))
                };
            }
        }
        
        unsafe {
            Cow::Borrowed(std::str::from_utf8_unchecked(trimmed))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mysql_optimized_tokenization() {
        let mut parser = OptimizedMySQLParser::new(1024);
        let query = "INSERT INTO `users` (`id`, `name`) VALUES (1, 'John');";
        
        let tokens = parser.tokenize_optimized(query).unwrap();
        
        assert!(!tokens.is_empty());
        
        // Check for INSERT keyword
        assert!(tokens.iter().any(|t| matches!(t, Token::Word(w) if w.value == "INSERT")));
    }

    #[test]
    fn test_mysql_fast_column_extraction() {
        let mut parser = OptimizedMySQLParser::new(1024);
        let query = r#"INSERT INTO `users` (`id`, `name`, `email`) VALUES (1, 'John', 'john@example.com')"#;
        
        let columns = parser.extract_insert_columns_fast(query).unwrap();
        
        assert_eq!(columns.len(), 3);
        assert_eq!(columns[0], "id");
        assert_eq!(columns[1], "name");
        assert_eq!(columns[2], "email");
    }

    #[test]
    fn test_mysql_backtick_parsing() {
        let mut parser = OptimizedMySQLParser::new(1024);
        let data = b"`column_name`";
        
        let (result, _) = parser.parse_backtick_identifier_fast(data, 0).unwrap();
        assert_eq!(result, "column_name");
    }

    #[test]
    fn test_mysql_string_escaping() {
        let mut parser = OptimizedMySQLParser::new(1024);
        let data = b"'Hello''s World'";
        
        let (result, _) = parser.parse_single_quoted_string_fast(data, 0).unwrap();
        assert_eq!(result, "Hello's World");
    }

    #[test]
    fn test_mysql_comment_handling() {
        let mut parser = OptimizedMySQLParser::new(1024);
        let query = "SELECT * FROM users -- this is a comment\nWHERE id = 1";
        
        let tokens = parser.tokenize_optimized(query).unwrap();
        
        // Should contain SELECT and WHERE but not the comment
        assert!(tokens.iter().any(|t| matches!(t, Token::Word(w) if w.value == "SELECT")));
        assert!(tokens.iter().any(|t| matches!(t, Token::Word(w) if w.value == "WHERE")));
    }
}