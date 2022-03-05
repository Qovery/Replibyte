use crate::DumpFileError;
use std::fs::File;
use std::io::BufReader;

pub fn read_dump<'a, S: Into<&'a str>>(
    dump_file_path: S,
) -> Result<BufReader<File>, DumpFileError> {
    let file = match File::open(dump_file_path.into()) {
        Ok(file) => file,
        Err(_) => return Err(DumpFileError::DoesNotExist),
    };

    Ok(BufReader::new(file))
}
