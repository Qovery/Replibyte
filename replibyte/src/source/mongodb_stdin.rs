use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::mongodb::read_and_transform;
use crate::types::{OriginalQuery, Query};
use crate::Source;
use crate::SourceOptions;

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
        source_options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error> {
        let reader = BufReader::new(stdin());
        read_and_transform(reader, source_options, query_callback);
        Ok(())
    }
}
