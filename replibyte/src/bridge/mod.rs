use crate::connector::Connector;
use crate::types::Bytes;
use serde::{Deserialize, Serialize};
use std::io::{Error, ErrorKind};

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

impl IndexFile {
    pub fn find_backup(&mut self, options: &DownloadOptions) -> Result<&Backup, Error> {
        match options {
            DownloadOptions::Latest => {
                self.backups.sort_by(|a, b| a.created_at.cmp(&b.created_at));

                match self.backups.last() {
                    Some(backup) => Ok(backup),
                    None => return Err(Error::new(ErrorKind::Other, "No backups available.")),
                }
            }
            DownloadOptions::Backup { name } => {
                match self
                    .backups
                    .iter()
                    .find(|backup| backup.directory_name.as_str() == name.as_str())
                {
                    Some(backup) => Ok(backup),
                    None => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("Can't find backup with name '{}'", name),
                        ));
                    }
                }
            }
        }
    }
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
