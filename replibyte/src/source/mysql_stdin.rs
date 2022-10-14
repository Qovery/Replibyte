use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::mysql::read_and_transform;
use crate::types::{OriginalQuery, Query};
use crate::Source;
use crate::SourceOptions;

/// Source MySQL dump from STDIN
pub struct MysqlStdin {}

impl Default for MysqlStdin {
    fn default() -> Self {
        Self {}
    }
}

impl Connector for MysqlStdin {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Source for MysqlStdin {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error> {
        let reader = BufReader::new(stdin());
        read_and_transform(reader, options, query_callback);

        Ok(())
    }
}
