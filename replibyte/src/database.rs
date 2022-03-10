use crate::types::Row;
use std::io::Error;

pub trait Database {
    fn stream_rows<F: FnMut(Row)>(&self, row: F) -> Result<(), Error>;
}
