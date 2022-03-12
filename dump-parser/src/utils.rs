use crate::DumpFileError;
use crate::DumpFileError::ReadError;
use std::fs::File;
use std::io::{BufRead, BufReader, Read};
use std::str;

pub fn list_queries_from_dump_file<'a, S, F>(
    dump_file_path: S,
    query: F,
) -> Result<(), DumpFileError>
where
    S: Into<&'a str>,
    F: FnMut(&str),
{
    let file = match File::open(dump_file_path.into()) {
        Ok(file) => file,
        Err(_) => return Err(DumpFileError::DoesNotExist),
    };

    let reader = BufReader::new(file);
    list_queries_from_dump_reader(reader, query)
}

pub fn list_queries_from_dump_reader<R, F>(
    mut dump_reader: BufReader<R>,
    mut query: F,
) -> Result<(), DumpFileError>
where
    R: Read,
    F: FnMut(&str),
{
    let mut count_empty_lines = 0;
    let mut buf_bytes: Vec<u8> = Vec::new();

    loop {
        let bytes = dump_reader.read_until(b'\n', &mut buf_bytes);
        let total_bytes = match bytes {
            Ok(bytes) => bytes,
            Err(err) => return Err(ReadError(err)),
        };

        /* let mut alphanumeric_last_byte_idx: usize = 0;
        for (i, buf_byte) in buf_bytes.iter().rev().enumerate() {
            if *buf_byte == b'\n' {
                // set the index for the last char
                alphanumeric_last_byte_idx = buf_bytes.len() - i - 1;
                break;
            }
        }*/

        let idx = if buf_bytes.len() < 1 {
            0
        } else {
            buf_bytes.len() - 1
        };

        // check end of line is a ';' char - it would mean it's the end of the query
        let is_last_by_end_of_query = match buf_bytes.get(idx) {
            Some(byte) => *byte == b';',
            None => false,
        };

        if total_bytes <= 1 || is_last_by_end_of_query {
            if count_empty_lines == 0 && buf_bytes.len() > 1 {
                let query_str = str::from_utf8(buf_bytes.as_slice()).unwrap(); // FIXME remove unwrap

                // split query_str by ';' in case of multiple queries are inside the string
                let queries_str = query_str.split(";").collect::<Vec<&str>>();

                if queries_str.len() == 1 {
                    // there is a only one query inside the str
                    query(query_str);
                } else {
                    // iterate and send all queries one by one
                    for query_str in queries_str {
                        let query_str = query_str.trim();
                        if !query_str.is_empty() {
                            let query_str = format!("{};", query_str);
                            query(query_str.as_str());
                        }
                    }
                }
            }

            let _ = buf_bytes.clear();
            count_empty_lines += 1;
        } else {
            count_empty_lines = 0;
        }

        // 49 is an empirical number -
        // not too large to avoid looping too much time, and not too small to avoid wrong end of query
        if count_empty_lines > 49 {
            // EOF?
            break;
        }
    }

    Ok(())
}
