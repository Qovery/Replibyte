use std::fs::{read, read_dir, remove_dir_all, write, DirBuilder, OpenOptions};
use std::io::{BufReader, Error, Read, Write};
use std::path::Path;

use log::{debug, error, info};
use serde_json::Value;

use crate::connector::Connector;
use crate::types;
use crate::utils::epoch_millis;

use super::{compress, decompress, decrypt, encrypt, Datastore, Dump, IndexFile, INDEX_FILE_NAME};

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
                info!("creating a new index_file");
                let index_file = IndexFile::new();
                let _ = self.write_index_file(&index_file)?;
                Ok(index_file)
            }
        }
    }
}

impl Connector for LocalDisk {
    fn init(&mut self) -> Result<(), Error> {
        debug!("initializing datastore");
        self.create_index_file().map(|_| ())
    }
}

impl Datastore for LocalDisk {
    fn index_file(&self) -> Result<IndexFile, Error> {
        debug!("reading index_file at: {}", &self.dir);

        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}", self.dir, INDEX_FILE_NAME))?;

        let reader = BufReader::new(file);

        let index_file: IndexFile =
            serde_json::from_reader(reader).map_err(|err| Error::from(err))?;

        Ok(index_file)
    }

    fn raw_index_file(&self) -> Result<Value, Error> {
        info!("reading raw index_file at: {}", &self.dir);

        let file = OpenOptions::new()
            .read(true)
            .open(format!("{}/{}", self.dir, INDEX_FILE_NAME))?;

        let reader = BufReader::new(file);

        let raw_index_file = serde_json::from_reader(reader).map_err(|err| Error::from(err))?;

        Ok(raw_index_file)
    }

    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error> {
        info!("writing index_file");
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

    fn write_raw_index_file(&self, raw_index_file: &Value) -> Result<(), Error> {
        info!("writing raw index_file");
        let index_file_path = format!("{}/{}", self.dir, INDEX_FILE_NAME);

        debug!("opening index_file at {}", index_file_path);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&index_file_path)?;

        debug!("writing raw index_file at {}", index_file_path.as_str());
        serde_json::to_writer(file, raw_index_file).map_err(|err| Error::from(err))
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

        let mut new_dump = Dump {
            directory_name: self.dump_name.to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: self.compression_enabled(),
            encrypted: self.encryption_key().is_some(),
        };

        // find or create Dump
        let mut dump = index_file
            .dumps
            .iter_mut()
            .find(|b| b.directory_name.as_str() == self.dump_name)
            .unwrap_or(&mut new_dump);

        if dump.size == 0 {
            // it means it's a new dump.
            // We need to add it into the index_file.dumps
            new_dump.size = data_size;
            index_file.dumps.push(new_dump);
        } else {
            // update total dump size
            dump.size = dump.size + data_size;
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
        let dump = index_file.find_dump(options)?;
        let entries = read_dir(format!("{}/{}", self.dir, dump.directory_name))?;

        let mut paths: Vec<_> = read_dir(format!("{}/{}", self.dir, dump.directory_name)).unwrap()
            .map(|r| r.unwrap())
            .collect();
        paths.sort_by(|a, b| {
            let a_int = a.path().file_stem().unwrap().to_os_string().to_str().unwrap().parse::<i32>().unwrap();
            let b_int = b.path().file_stem().unwrap().to_os_string().to_str().unwrap().parse::<i32>().unwrap();
            return a_int.cmp(&b_int)
        });
        for entry in paths {
            let data = read(entry.path())?;

            // decrypt data?
            let data = if dump.encrypted {
                // It should be safe to unwrap here because the dump is marked as encrypted in the dump manifest
                // so if there is no encryption key set at the datastore level we want to panic.
                let encryption_key = self.encryption_key.as_ref().unwrap();
                decrypt(data, encryption_key.as_str())?
            } else {
                data
            };

            // decompress data?
            let data = if dump.compressed {
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
        let mut index_file = self.index_file()?;

        let dump_dir_path = format!("{}/{}", self.dir, name);
        remove_dir_all(&dump_dir_path).map_err(|err| {
            error!("error while removing the dump directory: {}", dump_dir_path);
            err
        })?;

        // update the index_file.
        index_file.dumps.retain(|b| b.directory_name != name);

        self.write_index_file(&index_file)
    }
}

#[cfg(test)]
mod tests {
    use std::{fs::OpenOptions};
    use std::path::Path;

    use chrono::{Duration, Utc};
    use serde_json::json;
    use tempfile::tempdir;

    use crate::{
        cli::DumpDeleteArgs,
        connector::Connector,
        datastore::{Datastore, Dump, ReadOptions, INDEX_FILE_NAME},
        migration::{
            rename_backups_to_dumps::RenameBackupsToDump,
            update_version_number::UpdateVersionNumber, Migrator,
        },
        utils::epoch_millis,
    };

    use super::LocalDisk;

    // update_dump_date is a helper function that updates the date of a dump inside the index file.
    fn update_dump_date(local_disk: &LocalDisk, dump_name: String, days_before_now: i64) {
        let mut index_file = local_disk.index_file().unwrap();

        let mut dump = index_file
            .dumps
            .iter_mut()
            .find(|b| b.directory_name == dump_name)
            .unwrap();
        dump.created_at = (Utc::now() - Duration::days(days_before_now)).timestamp_millis() as u128;

        let _ = local_disk.write_index_file(&index_file);
    }

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
        assert_eq!(index_file.dumps.len(), 1);

        let dump = index_file.find_dump(&ReadOptions::Latest).unwrap();

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

        assert!(index_file.dumps.is_empty());

        index_file.dumps.push(Dump {
            directory_name: "dump-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(local_disk.write_index_file(&index_file).is_ok());

        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
    }

    #[test]
    fn test_dump_name() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        local_disk.set_dump_name("custom-dump-name".to_string());

        assert_eq!(local_disk.dump_name, "custom-dump-name".to_string())
    }

    #[test]
    fn test_delete_by_name() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        // init local_disk
        let _ = local_disk.init().expect("local_disk init failed");
        assert!(local_disk.index_file().is_ok());
        let index_file = local_disk.index_file().unwrap();
        assert!(index_file.dumps.is_empty());

        // create dump 1
        local_disk.set_dump_name("dump-1".to_string());
        let bytes: Vec<u8> = b"hello world from dump-1".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        assert!(Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());

        // create dump 2
        local_disk.set_dump_name("dump-2".to_string());
        let bytes: Vec<u8> = b"hello world from dump-2".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());

        // remove dump 1
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: Some("dump-1".to_string()),
                older_than: None,
                keep_last: None
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        assert!(!Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());

        // remove dump 2
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: Some("dump-2".to_string()),
                older_than: None,
                keep_last: None
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 0);
        assert!(!Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
    }

    #[test]
    fn test_delete_keep_last() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        // init local_disk
        let _ = local_disk.init().expect("local_disk init failed");
        assert!(local_disk.index_file().is_ok());
        let index_file = local_disk.index_file().unwrap();
        assert!(index_file.dumps.is_empty());

        // create dump 1
        local_disk.set_dump_name("dump-1".to_string());
        let bytes: Vec<u8> = b"hello world from dump-1".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        assert!(Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        update_dump_date(&local_disk, "dump-1".to_string(), 3);

        // create dump 2
        local_disk.set_dump_name("dump-2".to_string());
        let bytes: Vec<u8> = b"hello world from dump-2".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        update_dump_date(&local_disk, "dump-2".to_string(), 2);

        // create dump 3
        local_disk.set_dump_name("dump-3".to_string());
        let bytes: Vec<u8> = b"hello world from dump-3".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 3);
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());
        update_dump_date(&local_disk, "dump-3".to_string(), 1);

        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: None,
                keep_last: Some(2),
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        // only dump-1 must be deleted
        assert!(!Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());

        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: None,
                keep_last: Some(1),
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        // only dump-3 must exists
        assert!(!Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        assert!(!Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());
    }

    #[test]
    fn test_delete_older_than() {
        let dir = tempdir().expect("cannot create tempdir");
        let mut local_disk = LocalDisk::new(dir.path().to_str().unwrap().to_string());

        // init local_disk
        let _ = local_disk.init().expect("local_disk init failed");
        assert!(local_disk.index_file().is_ok());
        let index_file = local_disk.index_file().unwrap();
        assert!(index_file.dumps.is_empty());

        // create dump 1
        local_disk.set_dump_name("dump-1".to_string());
        let bytes: Vec<u8> = b"hello world from dump-1".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        assert!(Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        update_dump_date(&local_disk, "dump-1".to_string(), 5);

        // create dump 2
        local_disk.set_dump_name("dump-2".to_string());
        let bytes: Vec<u8> = b"hello world from dump-2".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        update_dump_date(&local_disk, "dump-2".to_string(), 3);

        // create dump 3
        local_disk.set_dump_name("dump-3".to_string());
        let bytes: Vec<u8> = b"hello world from dump-3".to_vec();
        assert!(local_disk.write(1, bytes).is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 3);
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());

        // delete dump older than 6 days doesn't remove any dump
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("6d".to_string()),
                keep_last: None,
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 3);
        assert!(Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());

        // delete dump older than 4 days must remove dump-1
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("4d".to_string()),
                keep_last: None,
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        assert!(!Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());

        // delete dump older than 1 day must remove dump-2
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("1d".to_string()),
                keep_last: None,
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 1);
        assert!(!Path::new(&format!("{}/dump-1", dir.path().to_str().unwrap())).exists());
        assert!(!Path::new(&format!("{}/dump-2", dir.path().to_str().unwrap())).exists());
        assert!(Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());

        // delete dump older than 0 day must remove dump-3
        assert!(local_disk
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("0d".to_string()),
                keep_last: None,
            })
            .is_ok());
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 0);
        assert!(!Path::new(&format!("{}/dump-3", dir.path().to_str().unwrap())).exists());
    }

    #[test]
    fn test_migrate_update_index_file_version_and_rename_backups_to_dumps() {
        // arrange
        let dir = tempdir().expect("cannot create tempdir");
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&format!(
                "{}/{}",
                dir.path().to_str().unwrap(),
                INDEX_FILE_NAME
            ))
            .expect("cannot create test metadata.json");

        // overwrite the index file
        let value = json!({
            "backups": [
                {
                    "directory_name": "dump-1653170039392",
                    "size": 62279,
                    "created_at": 1234,
                    "compressed": true,
                    "encrypted": false
                },
                {
                    "directory_name": "dump-1653170570014",
                    "size": 62283,
                    "created_at": 5678,
                    "compressed": true,
                    "encrypted": false
                }
            ]
        });
        serde_json::to_writer(file, &value).expect("cannot write test metadata.json");

        // create,init and migrate datastore
        let mut local_disk: Box<dyn Datastore> =
            Box::new(LocalDisk::new(dir.path().to_str().unwrap().to_string()));

        let migrator = Migrator::new(
            "0.7.3",
            &local_disk,
            vec![
                Box::new(UpdateVersionNumber::new("0.7.3")),
                Box::new(RenameBackupsToDump::default()),
            ],
        );
        assert!(migrator.migrate().is_ok());

        let _ = local_disk.init().expect("local_disk init failed");

        // assert
        assert!(local_disk.index_file().is_ok());
        assert_eq!(
            local_disk.index_file().unwrap().v,
            Some("0.7.3".to_string())
        );
        assert_eq!(local_disk.index_file().unwrap().dumps.len(), 2);
        assert_eq!(
            local_disk.index_file().unwrap().dumps.get(0),
            Some(&Dump {
                directory_name: "dump-1653170039392".to_string(),
                size: 62279,
                created_at: 1234,
                compressed: true,
                encrypted: false
            })
        );
        assert_eq!(
            local_disk.index_file().unwrap().dumps.get(1),
            Some(&Dump {
                directory_name: "dump-1653170570014".to_string(),
                size: 62283,
                created_at: 5678,
                compressed: true,
                encrypted: false
            })
        );
    }
}
