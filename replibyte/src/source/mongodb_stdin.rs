use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::mongodb::read_and_transform;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};
use crate::Source;

pub struct MongoDBStdin {}

impl Default for MongoDBStdin {
    fn default() -> Self {
        MongoDBStdin {}
    }
}

impl Connector for MongoDBStdin {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Source for MongoDBStdin {
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
