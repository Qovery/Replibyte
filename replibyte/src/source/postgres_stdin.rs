use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::postgres::read_and_transform;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};
use crate::Source;

/// Source Postgres dump from STDIN
pub struct PostgresStdin {}

impl PostgresStdin {
    pub fn new() -> Self {
        PostgresStdin {}
    }
}

impl Default for PostgresStdin {
    fn default() -> Self {
        PostgresStdin {}
    }
}

impl Connector for PostgresStdin {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Source for PostgresStdin {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error> {
        let reader = BufReader::new(stdin());
        read_and_transform(reader, transformers, query_callback);
        Ok(())
    }
}
