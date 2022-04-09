use std::io::ErrorKind;

#[derive(Debug)]
pub enum Error {
    DumpFile(DumpFileError),
}

#[derive(Debug)]
pub enum DumpFileError {
    DoesNotExist,
    ReadError(std::io::Error),
    MalFormatted,
}

impl From<DumpFileError> for std::io::Error {
    fn from(err: DumpFileError) -> Self {
        std::io::Error::new(ErrorKind::Other, format!("{:?}", err))
    }
}
