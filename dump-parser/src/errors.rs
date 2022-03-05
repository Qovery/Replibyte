#[derive(Debug)]
pub enum Error {
    DumpFile(DumpFileError),
}

#[derive(Debug)]
pub enum DumpFileError {
    DoesNotExist,
    MalFormatted,
}
