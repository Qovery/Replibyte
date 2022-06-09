use std::io::{Error, ErrorKind};
use std::str::FromStr;

use log::info;

use crate::datastore::Datastore;
use crate::migration::rename_backups_to_dumps::RenameBackupsToDump;
use crate::migration::update_version_number::UpdateVersionNumber;
use crate::utils::get_replibyte_version;

pub mod rename_backups_to_dumps;
pub mod update_version_number;

#[derive(Debug, PartialEq, PartialOrd)]
pub struct Version {
    major: u8,
    minor: u8,
    patch: u8,
}

impl FromStr for Version {
    type Err = Error;

    fn from_str(v: &str) -> Result<Self, Self::Err> {
        let numbers = v.split_terminator('.').collect::<Vec<&str>>();

        match numbers.len() {
            3 => {
                // unwrap is safe here as we know we have 3 items in vec.
                let major = parse_str_to_u8(numbers.get(0).unwrap())?;
                let minor = parse_str_to_u8(numbers.get(1).unwrap())?;
                let patch = parse_str_to_u8(numbers.get(2).unwrap())?;

                Ok(Self {
                    major,
                    minor,
                    patch,
                })
            }
            _ => Err(Error::new(
                ErrorKind::Other,
                format!("migration: version number '{}' is invalid, must have 'major.minor.patch' format", v),
            )),
        }
    }
}

pub trait Migration {
    /// minimal version for which the migration needs to be triggered.
    fn minimal_version(&self) -> Version;
    /// run the migration.
    fn run(&self, datastore: &Box<dyn Datastore>) -> Result<(), Error>;
}

// All registered migrations
pub fn migrations() -> Vec<Box<dyn Migration>> {
    vec![
        Box::new(UpdateVersionNumber::new(get_replibyte_version())),
        Box::new(RenameBackupsToDump::default()),
    ]
}

pub struct Migrator<'a> {
    current_replibyte_version: &'a str,
    datastore: &'a Box<dyn Datastore>,
    migrations: Vec<Box<dyn Migration>>,
}

impl<'a> Migrator<'a> {
    pub fn new(
        version: &'a str,
        datastore: &'a Box<dyn Datastore>,
        migrations: Vec<Box<dyn Migration>>,
    ) -> Self {
        Self {
            current_replibyte_version: version,
            datastore,
            migrations,
        }
    }

    /// run all registered migrations when the minimal version is matched.
    pub fn migrate(&self) -> Result<(), Error> {
        match self.datastore.raw_index_file() {
            Ok(_) => {
                for migration in &self.migrations {
                    if self.should_run_migration(migration) {
                        let _ = migration.run(self.datastore)?;
                    }
                }
                Ok(())
            },
            Err(err) => {
                // raw_index_file returns an error when we don't have a metadata.json file, in this case we don't need to run migrations.
                info!("migrate: skip migrate '{}'", err.to_string());
                Ok(())
            },
        }
    }

    fn should_run_migration(&self, migration: &Box<dyn Migration>) -> bool {
        let current_version = Version::from_str(self.current_replibyte_version).unwrap();

        current_version >= migration.minimal_version()
    }
}

fn parse_str_to_u8(s: &str) -> Result<u8, Error> {
    s.parse::<u8>()
        .map_err(|err| Error::new(ErrorKind::Other, err.to_string()))
}

#[cfg(test)]
mod tests {
    use std::{
        io::{Error, ErrorKind},
        str::FromStr,
    };

    use serde_json::json;

    use crate::connector::Connector;
    use crate::datastore::{Datastore, IndexFile, ReadOptions};

    use super::{Migration, Migrator, Version};

    struct FakeMigration {}
    impl Migration for FakeMigration {
        fn minimal_version(&self) -> Version {
            Version::from_str("0.7.2").unwrap()
        }

        fn run(&self, _datastore: &Box<dyn Datastore>) -> Result<(), std::io::Error> {
            // trigger an error so we can assert against it
            Err(Error::new(ErrorKind::Other, "should not run"))
        }
    }

    // an in memory datastore to test the migrator struct logic.
    struct InMemoryDatastore {
        index_file: IndexFile,
    }

    impl Connector for InMemoryDatastore {
        fn init(&mut self) -> Result<(), Error> {
            Ok(())
        }
    }

    impl Datastore for InMemoryDatastore {
        fn index_file(&self) -> Result<IndexFile, Error> {
            Ok(IndexFile {
                v: None,
                dumps: vec![],
            })
        }

        fn raw_index_file(&self) -> Result<serde_json::Value, Error> {
            Ok(json!(self.index_file))
        }

        fn write_index_file(&self, _index_file: &IndexFile) -> Result<(), Error> {
            unimplemented!()
        }

        fn write_raw_index_file(&self, _raw_index_file: &serde_json::Value) -> Result<(), Error> {
            unimplemented!()
        }

        fn write(&self, _file_part: u16, _data: crate::types::Bytes) -> Result<(), Error> {
            unimplemented!()
        }

        fn read(
            &self,
            _options: &ReadOptions,
            _data_callback: &mut dyn FnMut(crate::types::Bytes),
        ) -> Result<(), Error> {
            unimplemented!()
        }

        fn compression_enabled(&self) -> bool {
            true
        }

        fn set_compression(&mut self, _enable: bool) {
            unimplemented!()
        }

        fn encryption_key(&self) -> &Option<String> {
            unimplemented!()
        }

        fn set_encryption_key(&mut self, _key: String) {
            unimplemented!()
        }

        fn set_dump_name(&mut self, _name: String) {
            unimplemented!()
        }

        fn delete_by_name(&self, _name: String) -> Result<(), Error> {
            unimplemented!()
        }
    }

    #[test]
    fn str_to_version() {
        let version = Version::from_str("0.7.2").unwrap();
        assert_eq!(version.major, 0);
        assert_eq!(version.minor, 7);
        assert_eq!(version.patch, 2);

        assert!(Version::from_str("0.7").is_err());
    }

    #[test]
    fn compare_version() {
        let old_version = Version::from_str("0.7.2").unwrap();
        let new_version = Version::from_str("0.7.3").unwrap();
        assert!(old_version < new_version);

        let old_version = Version::from_str("1.7.0").unwrap();
        let new_version = Version::from_str("1.7.1").unwrap();
        assert!(old_version < new_version);

        let old_version = Version::from_str("0.7.0").unwrap();
        let new_version = Version::from_str("1.0.0").unwrap();
        assert!(old_version < new_version);
    }

    #[test]
    fn test_migrator() {
        let store: Box<dyn Datastore> = Box::new(InMemoryDatastore {
            index_file: IndexFile {
                v: None,
                dumps: vec![],
            },
        });

        let m = Migrator::new("0.7.3", &store, vec![Box::new(FakeMigration {})]);
        // migrate returns an error as FakeMigration is run
        assert!(m.migrate().is_err());

        let store: Box<dyn Datastore> = Box::new(InMemoryDatastore {
            index_file: IndexFile {
                v: None,
                dumps: vec![],
            },
        });

        let m = Migrator::new("0.7.0", &store, vec![Box::new(FakeMigration {})]);
        // migrate returns Ok as FakeMigration doesn't run
        assert!(m.migrate().is_ok());
    }
}
