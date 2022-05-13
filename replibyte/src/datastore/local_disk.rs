use std::fs::{read, read_dir, write, DirBuilder, OpenOptions};
use std::io::{BufReader, Error, Read, Write};

use log::{debug, error, info};

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
        info!(
            "reading index_file from local_disk datastore at: {}",
            &self.dir
        );

        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}", self.dir, INDEX_FILE_NAME))?;

        let reader = BufReader::new(file);

        let index_file: IndexFile =
            serde_json::from_reader(reader).map_err(|err| Error::from(err))?;

        Ok(index_file)
    }

    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error> {
        info!("writing index_file to local_disk datastore");
        let index_file_path = format!("{}/{}", self.dir, INDEX_FILE_NAME);

        debug!("opening index_file at {}", index_file_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_file_path)?;

        debug!("writing index_file at {}", index_file_path.as_str());
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
        let dump_dir_path = format!("{}/{}", self.dir, self.dump_name);
        let dump_file_path = format!("{}/{}.dump", dump_dir_path, file_part);

        // create the dump directory if needed
        DirBuilder::new()
            .recursive(true)
            .create(&dump_dir_path)
            .map_err(|err| {
                error!("error while creating the dump directory: {}", dump_dir_path);
                err
            })?;

        debug!("writing dump at: {}", dump_file_path);
        let _ = write(&dump_file_path, data).map_err(|err| {
            error!("error while writing dumpt at: {}", dump_file_path);
            err
        })?;

        // update index file
        let mut index_file = self.index_file()?;

        let mut new_backup = Backup {
            directory_name: self.dump_name.to_string(),
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

    fn delete_by_name(&self, name: String) -> Result<(), Error> {
        todo!()
    }

    fn delete_older_than(&self, days: i64) -> Result<(), Error> {
        todo!()
    }

    fn delete_keep_last(&self, keep_last: usize) -> Result<(), Error> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use tempfile::tempdir;

    use crate::{
        connector::Connector,
        datastore::{Backup, Datastore, ReadOptions},
        utils::epoch_millis,
    };

    use super::LocalDisk;

    #[test]
    fn init_local_disk() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        // executed twice to check that there is no error at the second call
        assert!(local_disk.init().is_ok());
        assert!(local_disk.init().is_ok());
    }

    #[test]
    fn test_write_and_read() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());
        let _ = local_disk.init().expect("local_disk init failed");

        let bytes: Vec<u8> = b"hello world".to_vec();

        assert!(local_disk.write(1, bytes).is_ok());

        // index_file should contain 1 dump
        let mut index_file = local_disk.index_file().unwrap();
        assert_eq!(index_file.backups.len(), 1);

        let dump = index_file.find_backup(&ReadOptions::Latest).unwrap();

        // part 1 of dump should exists
        assert!(Path::new(&format!(
            "{}/{}/1.dump",
            dir.path().to_str().unwrap(),
            dump.directory_name
        ))
        .exists());

        let mut dump_content: Vec<u8> = vec![];
        assert!(local_disk
            .read(&ReadOptions::Latest, &mut |bytes| {
                let mut b = bytes;
                dump_content.append(&mut b);
            })
            .is_ok());
        assert_eq!(dump_content, b"hello world".to_vec())
    }

    #[test]
    fn test_index_file() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());
        let _ = local_disk.init().expect("local_disk init failed");

        assert!(local_disk.index_file().is_ok());

        let mut index_file = local_disk.index_file().unwrap();

        assert!(index_file.backups.is_empty());

        index_file.backups.push(Backup {
            directory_name: "backup-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(local_disk.write_index_file(&index_file).is_ok());

        assert_eq!(local_disk.index_file().unwrap().backups.len(), 1);
    }

    #[test]
    fn test_backup_name() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        local_disk.set_dump_name("custom-backup-name".to_string());

        assert_eq!(local_disk.dump_name, "custom-backup-name".to_string())
    }
}
