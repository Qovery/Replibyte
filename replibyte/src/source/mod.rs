use std::io::{Error, ErrorKind, Read};
use std::process::Child;

use crate::config::{DatabaseSubsetConfig, SkipConfig};
use crate::connector::Connector;
use crate::transformer::Transformer;
use crate::types::{OriginalQuery, Query};

pub mod mongodb;
pub mod mongodb_stdin;
pub mod mysql;
pub mod mysql_stdin;
pub mod postgres;
pub mod postgres_stdin;

pub trait Source: Connector {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error>;
}

pub struct SourceOptions<'a> {
    pub transformers: &'a Vec<Box<dyn Transformer>>,
    pub skip_config: &'a Vec<SkipConfig>,
    pub database_subset: &'a Option<DatabaseSubsetConfig>,
}
