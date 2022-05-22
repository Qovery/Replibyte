use std::{
    io::{Error, ErrorKind},
    str::FromStr,
};

use log::info;
use serde_json::{json, Value};

use crate::datastore::Datastore;

use super::{Migration, Version};

pub struct UpdateVersionNumber<'a> {
    version: &'a str,
}

impl<'a> UpdateVersionNumber<'a> {
    pub fn new(version: &'a str) -> Self {
        Self { version }
    }
}

impl<'a> Migration for UpdateVersionNumber<'a> {
    fn minimal_version(&self) -> Version {
        Version::from_str("0.7.3").unwrap()
    }

    fn run(&self, datastore: &Box<dyn Datastore>) -> Result<(), Error> {
        info!("migrate: update version number");

        let mut raw_index_file = datastore.raw_index_file()?;
        let _ = update_version(&mut raw_index_file, self.version)?;
        datastore.write_raw_index_file(&raw_index_file)
    }
}

fn update_version(metadata_json: &mut Value, version: &str) -> Result<(), Error> {
    match metadata_json.as_object_mut() {
        Some(metadata) => {
            metadata.insert("v".to_string(), json!(version));
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

    use crate::migration::update_version_number::update_version;

    #[test]
    fn test_update_version() {
        let mut metadata_json = json!({"backups": []});

        assert!(update_version(&mut metadata_json, "0.1.0").is_ok());
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
        assert!(update_version(&mut metadata_json, "0.2.0").is_ok());
        assert!(metadata_json.get("v").is_some());
        assert_eq!(metadata_json.get("v").unwrap(), "0.2.0");

        let mut metadata_json = json!({"v": "0.7.3", "backups": []});
        assert!(update_version(&mut metadata_json, "0.7.4").is_ok());
        assert!(metadata_json.get("v").is_some());
        assert_eq!(metadata_json.get("v").unwrap(), "0.7.4");
    }
}
