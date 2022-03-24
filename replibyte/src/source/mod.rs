use std::io::Error;

use crate::connector::Connector;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};

pub mod mongo;
pub mod mongo_stdin;
pub mod postgres;
pub mod postgres_stdin;

pub trait Source: Connector {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error>;
}
