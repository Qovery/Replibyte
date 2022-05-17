use std::io::{Error, ErrorKind};

use log::info;
use serde_json::{json, Value};

use super::Datastore;

pub struct Migration<'a> {
    datastore: &'a Box<dyn Datastore>,
    current_version: &'a str,
}

impl<'a> Migration<'a> {
    pub fn new(datastore: &'a Box<dyn Datastore>, current_version: &'a str) -> Self {
        Self {
            datastore,
            current_version,
        }
    }

    pub fn run(&self) -> Result<(), Error> {
        // Early return if there is no index file because we don't need to migrate
        if !self.datastore.index_file_exists() {
            return Ok(());
        }

        let mut index_file = self.datastore.raw_index_file()?;

        let index_file_version = match &index_file.get("v") {
            Some(v) if v.is_string() => v.as_str(),
            _ => None,
        };

        // In all case when we doesn't have a version in the index_file, we should rename backups to dumps
        // as the dump rename occurs prior to adding version in the index file.
        if index_file_version.is_none() {
            let _ = add_version(&mut index_file, self.current_version)?;
            let _ = rename_backups_to_dumps(&mut index_file)?;
        }

        self.datastore.write_raw_index_file(&index_file)
    }
}

fn add_version(metadata_json: &mut Value, current_replibyte_version: &str) -> Result<(), Error> {
    info!("migrate: add version number");

    match metadata_json.as_object_mut() {
        Some(metadata) => {
            metadata.insert("v".to_string(), json!(current_replibyte_version));
            Ok(())
        }
        None => Err(Error::new(
            ErrorKind::Other,
            "migrate: metadata.json is not an object",
        )),
    }
}

fn rename_backups_to_dumps(metadata_json: &mut Value) -> Result<(), Error> {
    info!("migrate: rename backups to dumps");

    match metadata_json.as_object_mut() {
        Some(metadata) => {
            // we rename the `backups` key to `dumps`
            let backups = metadata.get("backups").unwrap_or(&json!([])).clone();
            metadata.insert("dumps".to_string(), backups);
            metadata.remove("backups");
            Ok(())
        }
        None => Err(Error::new(
            ErrorKind::Other,
            "migrate: metadata.json is not an object",
        )),
    }
}

#[cfg(test)]
mod tests {

    use serde_json::json;

    use crate::datastore::migration::{add_version, rename_backups_to_dumps};

    #[test]
    fn test_add_version() {
        let mut metadata_json = json!({"backups": []});
        assert!(add_version(&mut metadata_json, "0.1.0").is_ok());
        assert!(metadata_json.get("v").is_some());
        assert_eq!(metadata_json.get("v").unwrap(), "0.1.0");

        let mut metadata_json = json!({
            "backups": [
                {
                    "directory_name":"dump-1653170039392",
                    "size":62279,
                    "created_at":1234,
                    "compressed":true,
                    "encrypted":false
                }
            ]
        });
        assert!(add_version(&mut metadata_json, "0.2.0").is_ok());
        assert!(metadata_json.get("v").is_some());
        assert_eq!(metadata_json.get("v").unwrap(), "0.2.0");
    }

    #[test]
    fn test_rename_backup_to_dumps() {
        let mut metadata_json = json!({"backups": []});
        assert!(rename_backups_to_dumps(&mut metadata_json).is_ok());
        assert!(metadata_json.get("backups").is_none());
        assert!(metadata_json.get("dumps").is_some());
        assert!(metadata_json.get("dumps").unwrap().is_array());

        let mut metadata_json = json!({
            "backups": [
                {
                    "directory_name":"dump-1653170039392",
                    "size":62279,
                    "created_at":1234,
                    "compressed":true,
                    "encrypted":false
                }
            ]
        });
        assert!(rename_backups_to_dumps(&mut metadata_json).is_ok());
        assert!(metadata_json.get("backups").is_none());
        assert!(metadata_json.get("dumps").is_some());
        assert!(metadata_json.get("dumps").unwrap().is_array());
        assert!(metadata_json
            .get("dumps")
            .unwrap()
            .as_array()
            .unwrap()
            .contains(&json!({
                "directory_name":"dump-1653170039392",
                "size":62279,
                "created_at":1234,
                "compressed":true,
                "encrypted":false
            })));
    }
}
