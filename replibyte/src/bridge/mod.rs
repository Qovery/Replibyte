use crate::connector::Connector;
use crate::types::{Queries, Query};
use serde::{Deserialize, Serialize};
use std::io::Error;

pub mod s3;

pub trait Bridge: Connector + Send + Sync {
    /// Getting Index file with all the backups information
    fn index_file(&self) -> Result<IndexFile, Error>;
    fn save(&self, index_file: &IndexFile) -> Result<(), Error>;
    fn upload(&self, file_part: u16, queries: &Queries) -> Result<(), Error>;
    fn download<F>(&self, query_callback: F) -> Result<(), Error>
    where
        F: FnMut(Query);
}

#[derive(Serialize, Deserialize)]
pub struct IndexFile {
    backups: Vec<Backup>,
}

#[derive(Serialize, Deserialize)]
pub struct Backup {
    directory_name: String,
    size: usize,
    created_at: u128,
}
