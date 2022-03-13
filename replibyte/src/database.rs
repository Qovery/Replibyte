use std::io::Error;

use crate::transformer::Transformer;
use crate::types::{OriginalRow, Row};

pub trait Database {
    fn stream_rows<F: FnMut(OriginalRow, Row)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        row: F,
    ) -> Result<(), Error>;
}
