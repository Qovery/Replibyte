use std::{
    io::{Error, ErrorKind},
    str::FromStr,
};

use log::info;
use serde_json::{json, Value};

use crate::datastore::Datastore;

use super::{Migration, Version};

pub struct RenameBackupsToDump {}

impl RenameBackupsToDump {
    pub fn default() -> Self {
        Self {}
    }
}

impl Migration for RenameBackupsToDump {
    fn minimal_version(&self) -> Version {
        Version::from_str("0.7.3").unwrap()
    }

    fn run(&self, datastore: &Box<dyn Datastore>) -> Result<(), Error> {
        info!("migrate: rename backups to dumps");

        let mut raw_index_file = datastore.raw_index_file()?;
        let _ = rename_backups_to_dumps(&mut raw_index_file)?;
        datastore.write_raw_index_file(&raw_index_file)
    }
}

fn rename_backups_to_dumps(metadata_json: &mut Value) -> Result<(), Error> {
    match metadata_json.as_object_mut() {
        Some(metadata) => {
            // we rename the `backups` key to `dumps`
            if metadata.contains_key("backups") {
                let backups = metadata.get("backups").unwrap_or(&json!([])).clone();
                metadata.insert("dumps".to_string(), backups);
                metadata.remove("backups");
            }
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

    use crate::migration::rename_backups_to_dumps::rename_backups_to_dumps;

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
