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

        if total_bytes <= 1 {
            if count_empty_lines == 0 && buf_bytes.len() > 1 {
                let query_str = str::from_utf8(buf_bytes.as_slice()).unwrap(); // FIXME remove unwrap
                query(query_str);
            }

            let _ = buf_bytes.clear();
            count_empty_lines += 1;
        } else {
            count_empty_lines = 0;
        }

        if count_empty_lines > 49 {
            // EOF?
            break;
        }
    }

    Ok(())
}
