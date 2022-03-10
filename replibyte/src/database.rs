use crate::transformer::Transformer;
use crate::types::Row;
use std::io::Error;

pub trait Database {
    fn stream_rows<T: Transformer, F: FnMut(Row)>(
        &self,
        transformer: &T,
        row: F,
    ) -> Result<(), Error>;
}
