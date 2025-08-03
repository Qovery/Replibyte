/// SIMD operations module for high-performance parsing
/// Provides cross-platform SIMD optimizations with fallbacks

/// Fast case-insensitive pattern finding using SIMD when available
pub fn find_pattern_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }

    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && haystack.len() >= 32 && needle.len() <= 32 {
            return unsafe { find_pattern_avx2_case_insensitive(haystack, needle) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && haystack.len() >= 16 && needle.len() <= 16 {
            return unsafe { find_pattern_neon_case_insensitive(haystack, needle) };
        }
    }

    // Fallback implementation
    find_pattern_fallback_case_insensitive(haystack, needle)
}

/// Fast whitespace detection using SIMD
pub fn skip_whitespace_simd(data: &[u8], mut pos: usize) -> usize {
    #[cfg(target_arch = "x86_64")]
    {
        if is_x86_feature_detected!("avx2") && data.len() - pos >= 32 {
            return unsafe { skip_whitespace_avx2(data, pos) };
        }
    }

    #[cfg(target_arch = "aarch64")]
    {
        if std::arch::is_aarch64_feature_detected!("neon") && data.len() - pos >= 16 {
            return unsafe { skip_whitespace_neon(data, pos) };
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
unsafe fn find_pattern_avx2_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    use std::arch::x86_64::*;

    if haystack.len() < 32 || needle.is_empty() {
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
            let mut bit_mask = mask as u32;
            while bit_mask != 0 {
                let bit_pos = bit_mask.trailing_zeros() as usize;
                let candidate_pos = offset + bit_pos;
                
                if candidate_pos + needle.len() <= haystack.len() {
                    let candidate = &haystack[candidate_pos..candidate_pos + needle.len()];
                    if candidate.eq_ignore_ascii_case(needle) {
                        return Some(candidate_pos);
                    }
                }
                bit_mask &= bit_mask - 1; // Clear the lowest set bit
            }
        }
        offset += 32;
    }

    // Check remaining bytes
    find_pattern_fallback_case_insensitive(&haystack[offset..], needle)
        .map(|pos| offset + pos)
}

#[cfg(target_arch = "x86_64")]
unsafe fn skip_whitespace_avx2(data: &[u8], mut pos: usize) -> usize {
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
        
        if mask == -1 {
            // All bytes are whitespace
            pos += 32;
        } else if mask == 0 {
            // No whitespace found
            break;
        } else {
            // Mixed - find first non-whitespace
            let non_ws_pos = (!mask as u32).trailing_zeros() as usize;
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

#[cfg(target_arch = "aarch64")]
unsafe fn find_pattern_neon_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    use std::arch::aarch64::*;

    if haystack.len() < 16 || needle.is_empty() {
        return find_pattern_fallback_case_insensitive(haystack, needle);
    }

    let first_char_lower = needle[0].to_ascii_lowercase();
    let first_char_upper = needle[0].to_ascii_uppercase();
    let first_lower_vec = vdupq_n_u8(first_char_lower);
    let first_upper_vec = vdupq_n_u8(first_char_upper);

    let mut offset = 0;
    while offset + 16 <= haystack.len() {
        let chunk = vld1q_u8(haystack.as_ptr().add(offset));
        let cmp_lower = vceqq_u8(chunk, first_lower_vec);
        let cmp_upper = vceqq_u8(chunk, first_upper_vec);
        let cmp = vorrq_u8(cmp_lower, cmp_upper);
        
        // Extract mask from comparison result
        let mut mask = [0u8; 16];
        vst1q_u8(mask.as_mut_ptr(), cmp);
        
        for (i, &mask_byte) in mask.iter().enumerate() {
            if mask_byte != 0 {
                let candidate_pos = offset + i;
                if candidate_pos + needle.len() <= haystack.len() {
                    let candidate = &haystack[candidate_pos..candidate_pos + needle.len()];
                    if candidate.eq_ignore_ascii_case(needle) {
                        return Some(candidate_pos);
                    }
                }
            }
        }
        offset += 16;
    }

    // Check remaining bytes
    find_pattern_fallback_case_insensitive(&haystack[offset..], needle)
        .map(|pos| offset + pos)
}

#[cfg(target_arch = "aarch64")]
unsafe fn skip_whitespace_neon(data: &[u8], mut pos: usize) -> usize {
    use std::arch::aarch64::*;

    let space_vec = vdupq_n_u8(b' ');
    let tab_vec = vdupq_n_u8(b'\t');
    let newline_vec = vdupq_n_u8(b'\n');
    let cr_vec = vdupq_n_u8(b'\r');

    while pos + 16 <= data.len() {
        let chunk = vld1q_u8(data.as_ptr().add(pos));
        
        let is_space = vceqq_u8(chunk, space_vec);
        let is_tab = vceqq_u8(chunk, tab_vec);
        let is_newline = vceqq_u8(chunk, newline_vec);
        let is_cr = vceqq_u8(chunk, cr_vec);
        
        let is_whitespace = vorrq_u8(
            vorrq_u8(is_space, is_tab),
            vorrq_u8(is_newline, is_cr)
        );
        
        // Check if all bytes are whitespace
        let mut mask = [0u8; 16];
        vst1q_u8(mask.as_mut_ptr(), is_whitespace);
        
        let all_whitespace = mask.iter().all(|&b| b != 0);
        let no_whitespace = mask.iter().all(|&b| b == 0);
        
        if all_whitespace {
            pos += 16;
        } else if no_whitespace {
            break;
        } else {
            // Mixed - find first non-whitespace
            for (i, &mask_byte) in mask.iter().enumerate() {
                if mask_byte == 0 {
                    pos += i;
                    return pos;
                }
            }
            pos += 16;
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

fn find_pattern_fallback_case_insensitive(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window.eq_ignore_ascii_case(needle))
}

/// Fast keyword comparison with SIMD optimization
pub fn compare_keyword_case_insensitive(data: &[u8], pos: usize, keyword: &[u8]) -> bool {
    if pos + keyword.len() > data.len() {
        return false;
    }

    let slice = &data[pos..pos + keyword.len()];
    slice.eq_ignore_ascii_case(keyword)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pattern_finding() {
        let haystack = b"SELECT * FROM users WHERE name = 'John'";
        
        assert_eq!(find_pattern_case_insensitive(haystack, b"SELECT"), Some(0));
        assert_eq!(find_pattern_case_insensitive(haystack, b"select"), Some(0));
        assert_eq!(find_pattern_case_insensitive(haystack, b"FROM"), Some(9));
        assert_eq!(find_pattern_case_insensitive(haystack, b"from"), Some(9));
        assert_eq!(find_pattern_case_insensitive(haystack, b"WHERE"), Some(20));
        assert_eq!(find_pattern_case_insensitive(haystack, b"MISSING"), None);
    }

    #[test]
    fn test_whitespace_skipping() {
        let data = b"   \t\n\r   SELECT";
        let pos = skip_whitespace_simd(data, 0);
        assert_eq!(pos, 7);
        assert_eq!(&data[pos..pos + 6], b"SELECT");
    }

    #[test]
    fn test_keyword_comparison() {
        let data = b"INSERT INTO users";
        assert!(compare_keyword_case_insensitive(data, 0, b"INSERT"));
        assert!(compare_keyword_case_insensitive(data, 0, b"insert"));
        assert!(!compare_keyword_case_insensitive(data, 0, b"SELECT"));
        assert!(compare_keyword_case_insensitive(data, 7, b"INTO"));
        assert!(compare_keyword_case_insensitive(data, 7, b"into"));
    }
}