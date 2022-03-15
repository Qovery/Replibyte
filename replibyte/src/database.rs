use std::io::Error;

use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};

pub trait Database {
    fn stream_dump_queries<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error>;
}
