use std::io::{Error, ErrorKind};
use std::str::FromStr;

use aws_config::provider_config::ProviderConfig;
use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, Object, ObjectIdentifier,
};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint as SdkEndpoint};
use aws_types::os_shim_internal::Env;
use chrono::{Duration, Utc};
use log::{error, info};

use crate::bridge::s3::S3Error::FailedObjectUpload;
use crate::bridge::{
    compress, decompress, decrypt, encrypt, Backup, Bridge, IndexFile, ReadOptions,
};
use crate::cli::BackupDeleteArgs;
use crate::config::Endpoint;
use crate::connector::Connector;
use crate::runtime::block_on;
use crate::types::Bytes;
use crate::utils::epoch_millis;

const INDEX_FILE_NAME: &str = "metadata.json";

pub struct S3 {
    bucket: String,
    root_key: String,
    region: String,
    client: Client,
    enable_compression: bool,
    encryption_key: Option<String>,
}

impl S3 {
    pub fn new<S: Into<String>>(
        bucket: S,
        region: S,
        access_key_id: S,
        secret_access_key: S,
        endpoint: Endpoint,
    ) -> Self {
        let access_key_id = access_key_id.into();
        let secret_access_key = secret_access_key.into();
        let region = region.into();

        let sdk_config = block_on(
            aws_config::from_env()
                .configure(ProviderConfig::default().with_env(Env::from_slice(&[
                    ("AWS_ACCESS_KEY_ID", access_key_id.as_str()),
                    ("AWS_SECRET_ACCESS_KEY", secret_access_key.as_str()),
                    ("AWS_REGION", region.as_str()),
                ])))
                .load(),
        );

        let s3_config_builder = aws_sdk_s3::config::Builder::from(&sdk_config);

        let s3_config = match endpoint {
            Endpoint::Default => s3_config_builder.build(),
            Endpoint::Custom(url) => match http::Uri::from_str(url.as_str()) {
                Ok(uri) => s3_config_builder
                    .endpoint_resolver(SdkEndpoint::immutable(uri))
                    .build(),
                Err(_) => s3_config_builder.build(),
            },
        };

        S3 {
            bucket: bucket.into().to_string(),
            root_key: format!("backup-{}", epoch_millis()),
            region,
            client: Client::from_conf(s3_config),
            enable_compression: true,
            encryption_key: None,
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

impl Connector for S3 {
    fn init(&mut self) -> Result<(), Error> {
        let _ = create_bucket(&self.client, self.bucket.as_str(), self.region.as_str())?;
        self.create_index_file().map(|_| ())
    }
}

impl Bridge for S3 {
    fn index_file(&self) -> Result<IndexFile, Error> {
        let object = get_object(&self.client, self.bucket.as_str(), INDEX_FILE_NAME)?;
        let index_file: IndexFile = serde_json::from_slice(object.as_slice())?;
        Ok(index_file)
    }

    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error> {
        let index_file_json = serde_json::to_vec(index_file)?;

        create_object(
            &self.client,
            self.bucket.as_str(),
            INDEX_FILE_NAME,
            index_file_json,
        )
        .map_err(|err| Error::from(err))
    }

    fn write(&self, file_part: u16, data: Bytes) -> Result<(), Error> {
        // compress data?
        let data = if self.enable_compression {
            compress(data)?
        } else {
            data
        };

        // encrypt data?
        let data = match &self.encryption_key {
            Some(key) => encrypt(data, key.as_str())?,
            None => data,
        };

        let data_size = data.len();
        let key = format!("{}/{}.dump", self.root_key.as_str(), file_part);

        info!("upload object '{}' part {} on", key.as_str(), file_part);

        let _ = create_object(&self.client, self.bucket.as_str(), key.as_str(), data)?;

        // update index file
        let mut index_file = self.index_file()?;

        let mut new_backup = Backup {
            directory_name: self.root_key.clone(),
            size: 0,
            created_at: epoch_millis(),
            compressed: self.enable_compression,
            encrypted: self.encryption_key.is_some(),
        };

        // find or create Backup
        let mut backup = index_file
            .backups
            .iter_mut()
            .find(|b| b.directory_name.as_str() == self.root_key.as_str())
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

    fn read<'a, F>(&self, options: &ReadOptions, mut data_callback: F) -> Result<(), Error>
    where
        F: FnMut(Bytes),
    {
        let mut index_file = self.index_file()?;
        let backup = index_file.find_backup(options)?;

        for object in list_objects(
            &self.client,
            self.bucket.as_str(),
            Some(backup.directory_name.as_str()),
        )? {
            let data = get_object(&self.client, self.bucket.as_str(), object.key().unwrap())?;

            // decrypt data?
            let data = if backup.encrypted {
                // It should be safe to unwrap here because the backup is marked as encrypted in the backup manifest
                // so if there is no encryption key set at the bridge level we want to panic.
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

    fn set_encryption_key(&mut self, key: String) {
        self.encryption_key = Some(key);
    }

    fn set_compression(&mut self, enable: bool) {
        self.enable_compression = enable;
    }

    fn set_backup_name(&mut self, name: String) {
        self.root_key = name;
    }

    fn delete(&self, args: &BackupDeleteArgs) -> Result<(), Error> {
        if let Some(backup_name) = &args.backup {
            return delete_by_name(&self, backup_name.as_str());
        }

        if let Some(older_than) = &args.older_than {
            let days = match older_than.chars().nth_back(0) {
                Some('d') => {
                    // remove the last character which corresponds to the unit
                    let mut older_than = older_than.to_string();
                    older_than.pop();

                    match older_than.parse::<i64>() {
                        Ok(days) => days,
                        Err(err) => return Err(Error::new(
                            ErrorKind::Other,
                            format!("command error: {} - invalid `--older-than` format. Use `--older-than=14d`", err),
                        )),
                    }
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "command error: invalid `--older-than` format. Use `--older-than=14d`",
                    ))
                }
            };

            return delete_older_than(&self, days);
        }

        if let Some(keep_last) = args.keep_last {
            return delete_keep_last(&self, keep_last);
        }

        Err(Error::new(
            ErrorKind::Other,
            "command error: parameters or options required",
        ))
    }
}

fn delete_older_than(bridge: &S3, days: i64) -> Result<(), Error> {
    let index_file = bridge.index_file()?;

    let threshold_date = Utc::now() - Duration::days(days);
    let threshold_date = threshold_date.timestamp_millis() as u128;

    let backups_to_delete: Vec<Backup> = index_file
        .backups
        .into_iter()
        .filter(|b| b.created_at.lt(&threshold_date))
        .collect();

    for backup in backups_to_delete {
        delete_by_name(&bridge, backup.directory_name.as_str())?
    }

    Ok(())
}

fn delete_keep_last(bridge: &S3, keep_last: usize) -> Result<(), Error> {
    let mut index_file = bridge.index_file()?;

    index_file
        .backups
        .sort_by(|a, b| b.created_at.cmp(&a.created_at));

    if let Some(backups) = index_file.backups.get(keep_last..) {
        for backup in backups {
            delete_by_name(&bridge, backup.directory_name.as_str())?;
        }
    }

    Ok(())
}

fn delete_by_name(bridge: &S3, backup_name: &str) -> Result<(), Error> {
    let mut index_file = bridge.index_file()?;

    let bucket = &bridge.bucket;

    let _ =
        delete_directory(&bridge.client, bucket, backup_name).map_err(|err| Error::from(err))?;

    index_file
        .backups
        .retain(|b| b.directory_name != backup_name);

    bridge.write_index_file(&index_file)
}

#[derive(Debug, Eq, PartialEq)]
enum S3Error<'a> {
    FailedToCreateBucket { bucket: &'a str },
    FailedToDeleteBucket { bucket: &'a str },
    FailedToListObjects { bucket: &'a str },
    ObjectDoesNotExist { bucket: &'a str, key: &'a str },
    FailedObjectDownload { bucket: &'a str, key: &'a str },
    FailedObjectUpload { bucket: &'a str, key: &'a str },
    FailedToDeleteObject { bucket: &'a str, key: &'a str },
    FailedToDeleteDirectory { bucket: &'a str, directory: &'a str },
}

impl<'a> From<S3Error<'a>> for Error {
    fn from(err: S3Error<'a>) -> Self {
        match err {
            S3Error::FailedToCreateBucket { bucket } => Error::new(
                ErrorKind::Other,
                format!("failed to create bucket '{}'", bucket),
            ),
            S3Error::FailedToDeleteBucket { bucket } => Error::new(
                ErrorKind::Other,
                format!("failed to delete bucket '{}'", bucket),
            ),
            S3Error::FailedToListObjects { bucket } => Error::new(
                ErrorKind::Other,
                format!("failed to list objects from bucket '{}'", bucket),
            ),
            S3Error::ObjectDoesNotExist {
                bucket,
                key: object,
            } => Error::new(
                ErrorKind::Other,
                format!("object '{}/{}' does not exist", bucket, object),
            ),
            S3Error::FailedObjectDownload {
                bucket,
                key: object,
            } => Error::new(
                ErrorKind::Other,
                format!("failed to download object '{}/{}'", bucket, object),
            ),
            FailedObjectUpload {
                bucket,
                key: object,
            } => Error::new(
                ErrorKind::Other,
                format!("failed to upload object '{}/{}'", bucket, object),
            ),
            S3Error::FailedToDeleteObject {
                bucket,
                key: object,
            } => Error::new(
                ErrorKind::Other,
                format!("failed to delete object '{}/{}'", bucket, object),
            ),
            S3Error::FailedToDeleteDirectory { bucket, directory } => Error::new(
                ErrorKind::Other,
                format!("failed to delete directory '{}/{}'", bucket, directory),
            ),
        }
    }
}

fn create_bucket<'a>(client: &Client, bucket: &'a str, region: &str) -> Result<(), S3Error<'a>> {
    let constraint = BucketLocationConstraint::from(region);
    let cfg = CreateBucketConfiguration::builder()
        .location_constraint(constraint)
        .build();

    if let Ok(_) = block_on(
        client
            .get_bucket_accelerate_configuration()
            .bucket(bucket)
            .send(),
    ) {
        info!("bucket {} exists", bucket);
        return Ok(());
    }

    let result = block_on(
        client
            .create_bucket()
            .create_bucket_configuration(cfg)
            .bucket(bucket)
            .send(),
    );

    match result {
        Ok(_) => {}
        Err(err) => {
            error!("{}", err);
            return Err(S3Error::FailedToCreateBucket { bucket });
        }
    }

    info!("bucket {} created", bucket);

    Ok(())
}

fn delete_bucket<'a>(client: &Client, bucket: &'a str, force: bool) -> Result<(), S3Error<'a>> {
    if force {
        for object in list_objects(client, bucket, None)? {
            let _ = delete_object(client, bucket, object.key().unwrap_or(""));
        }
    }

    let result = block_on(client.delete_bucket().bucket(bucket).send());
    match result {
        Ok(_) => {}
        Err(err) => {
            error!("{}", err);
            return Err(S3Error::FailedToDeleteBucket { bucket });
        }
    }

    info!("bucket {} created", bucket);

    Ok(())
}

fn create_object<'a>(
    client: &Client,
    bucket: &'a str,
    key: &'a str,
    object: Vec<u8>,
) -> Result<(), S3Error<'a>> {
    let result = block_on(
        client
            .put_object()
            .bucket(bucket)
            .key(key)
            .body(ByteStream::from(object))
            // TODO: set metadata etag to validate upload on the S3 side
            .send(),
    );

    if let Err(_) = result {
        return Err(S3Error::FailedObjectUpload { bucket, key });
    }

    Ok(())
}

fn get_object<'a>(client: &Client, bucket: &'a str, key: &'a str) -> Result<Vec<u8>, S3Error<'a>> {
    let result = block_on(client.get_object().bucket(bucket).key(key).send());

    match result {
        Ok(file) => match block_on(file.body.collect()) {
            Ok(data) => Ok(data.into_bytes().to_vec()),
            Err(_) => Err(S3Error::FailedObjectDownload { bucket, key }),
        },
        Err(_) => Err(S3Error::ObjectDoesNotExist { bucket, key }),
    }
}

fn list_objects<'a>(
    client: &Client,
    bucket: &'a str,
    path: Option<&'a str>,
) -> Result<Vec<Object>, S3Error<'a>> {
    let objects = block_on(client.list_objects_v2().bucket(bucket).send());
    let objects = match objects {
        Ok(objects) => objects,
        Err(err) => {
            error!("{}", err);
            return Err(S3Error::FailedToListObjects { bucket });
        }
    };

    // FIXME max objects listed is 1000 -> pagination?

    let objects = objects.contents.unwrap_or(Vec::new());
    if path.is_none() {
        return Ok(objects);
    }

    let path = path.unwrap();
    let mut objects = objects
        .into_iter()
        .filter(|object| match object.key() {
            Some(key) => key.starts_with(path),
            None => false,
        })
        .collect::<Vec<_>>();

    // sort by key
    objects.sort_by(|a, b| a.key.cmp(&b.key));

    Ok(objects)
}

fn delete_object<'a>(client: &Client, bucket: &'a str, key: &'a str) -> Result<(), S3Error<'a>> {
    let _ = get_object(client, bucket, key)?;

    let result = block_on(client.delete_object().bucket(bucket).key(key).send());

    match result {
        Ok(_) => Ok(()),
        Err(_) => Err(S3Error::FailedToDeleteObject { bucket, key }),
    }
}

fn delete_directory<'a>(
    client: &Client,
    bucket: &'a str,
    directory: &'a str,
) -> Result<(), S3Error<'a>> {
    if let Ok(objects) = block_on(
        client
            .list_objects_v2()
            .bucket(bucket)
            .prefix(directory)
            .send(),
    ) {
        let mut delete_objects: Vec<ObjectIdentifier> = vec![];
        for obj in objects.contents().unwrap_or_default() {
            let obj_id = ObjectIdentifier::builder()
                .set_key(Some(obj.key().unwrap().to_string()))
                .build();
            delete_objects.push(obj_id);
        }

        match block_on(
            client
                .delete_objects()
                .bucket(bucket)
                .delete(Delete::builder().set_objects(Some(delete_objects)).build())
                .send(),
        ) {
            Ok(_) => Ok(()),
            Err(err) => {
                eprintln!("{}", err);
                Err(S3Error::FailedToDeleteDirectory { bucket, directory })
            }
        }
    } else {
        Err(S3Error::FailedToListObjects { bucket })
    }
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, Utc};
    use fake::{Fake, Faker};

    use crate::bridge::s3::{create_object, delete_bucket, delete_object, get_object, S3Error};
    use crate::bridge::{Backup, Bridge};
    use crate::cli::BackupDeleteArgs;
    use crate::config::Endpoint;
    use crate::connector::Connector;
    use crate::utils::epoch_millis;
    use crate::S3;

    const BUCKET_NAME: &str = "replibyte-test";
    const REGION: &str = "us-east-2";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";
    const MINIO_CREDENTIALS: &str = "minioadmin";

    fn bucket() -> String {
        format!("replibyte-test-{}", Faker.fake::<String>().to_lowercase())
    }

    fn credentials() -> (String, String) {
        (
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or(MINIO_CREDENTIALS.to_string()),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or(MINIO_CREDENTIALS.to_string()),
        )
    }

    fn s3(bucket: &str) -> S3 {
        let (access_key_id, secret_access_key) = credentials();

        S3::new(
            bucket.to_string(),
            "us-east-2".to_string(),
            access_key_id,
            secret_access_key,
            Endpoint::Custom(MINIO_ENDPOINT.to_string()),
        )
    }

    #[test]
    fn init_s3() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());
        // executed twice to check that there is no error at the second call
        assert!(s3.init().is_ok());
        assert!(s3.init().is_ok());

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }

    #[test]
    fn create_and_get_and_delete_object() {
        let bucket = bucket();

        let mut s3 = s3(bucket.as_str());
        let _ = s3.init().expect("s3 init failed");

        let key = format!("testing-object-{}", Faker.fake::<String>());

        assert_eq!(
            get_object(&s3.client, bucket.as_str(), key.as_str())
                .err()
                .unwrap(),
            S3Error::ObjectDoesNotExist {
                bucket: bucket.as_str(),
                key: key.as_str(),
            }
        );

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            key.as_str(),
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert_eq!(
            get_object(&s3.client, bucket.as_str(), key.as_str()).unwrap(),
            b"hello w0rld"
        );

        // check that the object is updated
        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            key.as_str(),
            b"hello w0rld updated".to_vec(),
        )
        .is_ok());

        assert_eq!(
            get_object(&s3.client, bucket.as_str(), key.as_str()).unwrap(),
            b"hello w0rld updated"
        );

        assert!(delete_object(&s3.client, bucket.as_str(), key.as_str()).is_ok());

        assert_eq!(
            delete_object(&s3.client, bucket.as_str(), key.as_str())
                .err()
                .unwrap(),
            S3Error::ObjectDoesNotExist {
                bucket: bucket.as_str(),
                key: key.as_str(),
            }
        );

        assert_eq!(
            get_object(&s3.client, bucket.as_str(), key.as_str())
                .err()
                .unwrap(),
            S3Error::ObjectDoesNotExist {
                bucket: bucket.as_str(),
                key: key.as_str(),
            }
        );

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }

    #[test]
    fn test_s3_index_file() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.backups.is_empty());

        index_file.backups.push(Backup {
            directory_name: "backup-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }

    #[test]
    fn test_backup_name() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());

        s3.set_backup_name("custom-backup-name".to_string());

        assert_eq!(s3.root_key, "custom-backup-name".to_string())
    }

    #[test]
    fn test_s3_backup_delete_by_name() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.backups.is_empty());

        // Add 2 backups in the manifest
        index_file.backups.push(Backup {
            directory_name: "backup-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        index_file.backups.push(Backup {
            directory_name: "backup-2".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().backups.len(), 2);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-2/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: Some("backup-1".to_string()),
                older_than: None,
                keep_last: None
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: Some("backup-2".to_string()),
                older_than: None,
                keep_last: None
            })
            .is_ok());
        assert!(s3.index_file().unwrap().backups.is_empty());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
    }

    #[test]
    fn test_s3_backup_delete_older_than() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.backups.is_empty());

        // Add a backup from 5 days ago
        index_file.backups.push(Backup {
            directory_name: "backup-1".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(5)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        // Add a backup from now
        index_file.backups.push(Backup {
            directory_name: "backup-2".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().backups.len(), 2);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-2/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: None,
                older_than: Some("6d".to_string()),
                keep_last: None
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: None,
                older_than: Some("5d".to_string()),
                keep_last: None
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());
    }

    #[test]
    fn test_s3_backup_keep_last() {
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.backups.is_empty());

        index_file.backups.push(Backup {
            directory_name: "backup-1".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(3)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        index_file.backups.push(Backup {
            directory_name: "backup-2".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(5)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        index_file.backups.push(Backup {
            directory_name: "backup-3".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().backups.len(), 3);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-2/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "backup-3/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: None,
                older_than: None,
                keep_last: Some(2)
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-3/testing-key.dump").is_ok());

        assert!(s3
            .delete(&BackupDeleteArgs {
                backup: None,
                older_than: None,
                keep_last: Some(1)
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-3/testing-key.dump").is_ok());
    }
}
