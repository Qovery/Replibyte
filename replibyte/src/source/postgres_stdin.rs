use std::io::{stdin, BufReader, Error};

use crate::connector::Connector;
use crate::source::postgres::{read_and_transform, subset};
use crate::types::{OriginalQuery, Query};
use crate::Source;
use crate::SourceOptions;

/// Source Postgres dump from STDIN
#[derive(Default)]
pub struct PostgresStdin {}



impl Connector for PostgresStdin {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl Source for PostgresStdin {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error> {
        match &options.database_subset {
            None => {
                let reader = BufReader::new(stdin());
                read_and_transform(reader, options, query_callback);
            }
            Some(subset_config) => {
                let dump_reader = BufReader::new(stdin());
                let reader = subset(dump_reader, subset_config)?;
                read_and_transform(reader, options, query_callback);
            }
        };

        Ok(())
    }
}
