use crate::connector::Connector;
use crate::types::Bytes;
use serde::{Deserialize, Serialize};
use std::io::Error;

pub mod s3;

pub trait Bridge: Connector + Send + Sync {
    /// Getting Index file with all the backups information
    fn index_file(&self) -> Result<IndexFile, Error>;
    fn save(&self, index_file: &IndexFile) -> Result<(), Error>;
    fn upload(&self, file_part: u16, data: Bytes) -> Result<(), Error>;
    fn download<F>(&self, options: &DownloadOptions, data_callback: F) -> Result<(), Error>
    where
        F: FnMut(Bytes);
}

#[derive(Serialize, Deserialize)]
pub struct IndexFile {
    pub backups: Vec<Backup>,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Backup {
    pub directory_name: String,
    pub size: usize,
    pub created_at: u128,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum DownloadOptions {
    Latest,
    Backup { name: String },
}
