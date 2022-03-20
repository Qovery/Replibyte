use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::postgres::read_and_transform;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};
use crate::Source;

pub struct PostgresStdin {}

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
        mut query_callback: F,
    ) -> Result<(), Error> {
        let reader = BufReader::new(stdin());
        read_and_transform(reader, transformers, query_callback);
        Ok(())
    }
}
