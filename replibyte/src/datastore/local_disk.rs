use std::fs::{read, read_dir, write, File, OpenOptions};
use std::io::{BufReader, Error, Read, Write};

use log::{debug, info};

use crate::cli::DumpDeleteArgs;
use crate::connector::Connector;
use crate::types;
use crate::utils::epoch_millis;

use super::{
    compress, decompress, decrypt, encrypt, Backup, Datastore, IndexFile, INDEX_FILE_NAME,
};

pub struct LocalDisk {
    dir: String,
    dump_name: String,
    enable_compression: bool,
    encryption_key: Option<String>,
}

impl LocalDisk {
    pub fn new<S: Into<String>>(dir: S) -> Self {
        Self {
            dir: dir.into(),
            enable_compression: true,
            encryption_key: None,
            dump_name: format!("dump-{}", epoch_millis()),
        }
    }

    fn create_index_file(&self) -> Result<IndexFile, Error> {
        // TODO: creating dir if not exists
        match self.index_file() {
            Ok(index_file) => Ok(index_file),
            Err(_) => {
                let index_file = IndexFile { backups: vec![] };
                let _ = self.write_index_file(&index_file)?;
                Ok(index_file)
            }
        }
    }
}

impl Connector for LocalDisk {
    fn init(&mut self) -> Result<(), Error> {
        debug!("initializing local_disk datastore");
        self.create_index_file().map(|_| ())
    }
}

impl Datastore for LocalDisk {
    fn index_file(&self) -> Result<IndexFile, Error> {
        info!("reading index_file from datastore");

        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}", self.dir, INDEX_FILE_NAME))?;
        let reader = BufReader::new(file);

        let index_file: IndexFile =
            serde_json::from_reader(reader).map_err(|err| Error::from(err))?;

        Ok(index_file)
    }

    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error> {
        info!("writing index_file to datastore");

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(format!("{}/{}", self.dir, INDEX_FILE_NAME))?;

        serde_json::to_writer(file, index_file).map_err(|err| Error::from(err))
    }

    fn write(&self, file_part: u16, data: types::Bytes) -> Result<(), Error> {
        // compress data?
        let data = if self.compression_enabled() {
            compress(data)?
        } else {
            data
        };

        // encrypt data?
        let data = match self.encryption_key() {
            Some(key) => encrypt(data, key.as_str())?,
            None => data,
        };

        let data_size = data.len();
        let key = format!("{}/{}.dump", self.dump_name, file_part);
        let dir = &self.dir;
        let destination_path = format!("{}/{}", dir, key);

        let _ = write(destination_path, data)?;

        // update index file
        let mut index_file = self.index_file()?;

        let mut new_backup = Backup {
            directory_name: dir.to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: self.compression_enabled(),
            encrypted: self.encryption_key().is_some(),
        };

        // find or create Backup
        let mut backup = index_file
            .backups
            .iter_mut()
            .find(|b| b.directory_name.as_str() == self.dump_name)
            .unwrap_or(&mut new_backup);

        if backup.size == 0 {
            // it means it's a new backup.
            // We need to add it into the index_file.backups
            new_backup.size = data_size;
            index_file.backups.push(new_backup);
        } else {
            // update total backup size
            backup.size = backup.size + data_size;
        }

        // save index file
        self.write_index_file(&index_file)
    }

    fn read(
        &self,
        options: &super::ReadOptions,
        data_callback: &mut dyn FnMut(types::Bytes),
    ) -> Result<(), Error> {
        let mut index_file = self.index_file()?;
        let backup = index_file.find_backup(options)?;
        let entries = read_dir(format!("{}/{}", self.dir, backup.directory_name))?;

        for entry in entries {
            let entry = entry?;
            let data = read(entry.path())?;

            // decrypt data?
            let data = if backup.encrypted {
                // It should be safe to unwrap here because the backup is marked as encrypted in the backup manifest
                // so if there is no encryption key set at the datastore level we want to panic.
                let encryption_key = self.encryption_key.as_ref().unwrap();
                decrypt(data, encryption_key.as_str())?
            } else {
                data
            };

            // decompress data?
            let data = if backup.compressed {
                decompress(data)?
            } else {
                data
            };

            data_callback(data);
        }

        Ok(())
    }

    fn compression_enabled(&self) -> bool {
        self.enable_compression
    }

    fn set_compression(&mut self, enable: bool) {
        if !enable {
            info!("disable datastore compression");
        }

        self.enable_compression = enable;
    }

    fn encryption_key(&self) -> &Option<String> {
        &self.encryption_key
    }

    fn set_encryption_key(&mut self, key: String) {
        info!("set datastore encryption_key");

        self.encryption_key = Some(key)
    }

    fn set_dump_name(&mut self, name: String) {
        self.dump_name = name
    }

    fn delete(&self, args: &DumpDeleteArgs) -> Result<(), Error> {
        todo!()
    }
}
