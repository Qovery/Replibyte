use std::io::Error;

use crate::connector::Connector;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};

pub mod postgres;

pub trait Source: Connector {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error>;
}
