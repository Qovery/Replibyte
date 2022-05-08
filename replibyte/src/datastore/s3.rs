use std::io::SeekFrom::End;
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

use crate::cli::DumpDeleteArgs;
use crate::config::Endpoint;
use crate::connector::Connector;
use crate::datastore::s3::S3Error::FailedObjectUpload;
use crate::datastore::{
    compress, decompress, decrypt, encrypt, Backup, Datastore, IndexFile, ReadOptions,
};
use crate::runtime::block_on;
use crate::types::Bytes;
use crate::utils::epoch_millis;

const INDEX_FILE_NAME: &str = "metadata.json";
const GOOGLE_CLOUD_STORAGE_URL: &str = "https://storage.googleapis.com";

pub struct S3 {
    bucket: String,
    root_key: String,
    region: String,
    endpoint: Endpoint,
    client: Client,
    enable_compression: bool,
    encryption_key: Option<String>,
}

impl S3 {
    pub fn aws<S: Into<String>>(
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

        let s3_config = match &endpoint {
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
            endpoint,
            client: Client::from_conf(s3_config),
            enable_compression: true,
            encryption_key: None,
        }
    }

    pub fn gcp<S: Into<String>>(
        bucket: S,
        region: S,
        access_key: S,
        secret: S,
        endpoint: Endpoint,
    ) -> Self {
        let endpoint = match endpoint {
            // change for default GCP Cloud Storage endpoint
            Endpoint::Default => Endpoint::Custom(GOOGLE_CLOUD_STORAGE_URL.to_string()),
            // passthrough
            Endpoint::Custom(url) => Endpoint::Custom(url),
        };

        S3::aws(bucket, region, access_key, secret, endpoint)
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
        match &self.endpoint {
            Endpoint::Custom(url) if url.as_str() == GOOGLE_CLOUD_STORAGE_URL => {
                // Do not try to create bucket - the current S3 client does not supports well GCP Cloud Storage
            }
            _ => {
                let _ = create_bucket(&self.client, self.bucket.as_str(), self.region.as_str())?;
            }
        }

        self.create_index_file().map(|_| ())
    }
}

impl Datastore for S3 {
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
        write_objects(
            self,
            file_part,
            data,
            self.bucket.as_str(),
            self.root_key.as_str(),
            &self.client,
        )
    }

    fn read(
        &self,
        options: &ReadOptions,
        mut data_callback: &mut dyn FnMut(Bytes),
    ) -> Result<(), Error> {
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

    fn set_encryption_key(&mut self, key: String) {
        self.encryption_key = Some(key);
    }

    fn set_compression(&mut self, enable: bool) {
        self.enable_compression = enable;
    }

    fn set_backup_name(&mut self, name: String) {
        self.root_key = name;
    }

    fn delete(&self, args: &DumpDeleteArgs) -> Result<(), Error> {
        if let Some(backup_name) = &args.dump {
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
                    ));
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

    fn compression_enabled(&self) -> bool {
        self.enable_compression
    }

    fn encryption_key(&self) -> &Option<String> {
        &self.encryption_key
    }
}

fn write_objects<B: Datastore>(
    datastore: &B,
    file_part: u16,
    data: Bytes,
    bucket: &str,
    root_key: &str,
    client: &Client,
) -> Result<(), Error> {
    // compress data?
    let data = if datastore.compression_enabled() {
        compress(data)?
    } else {
        data
    };

    // encrypt data?
    let data = match datastore.encryption_key() {
        Some(key) => encrypt(data, key.as_str())?,
        None => data,
    };

    let data_size = data.len();
    let key = format!("{}/{}.dump", root_key, file_part);

    info!("upload object '{}' part {} on", key.as_str(), file_part);

    let _ = create_object(client, bucket, key.as_str(), data)?;

    // update index file
    let mut index_file = datastore.index_file()?;

    let mut new_backup = Backup {
        directory_name: root_key.to_string(),
        size: 0,
        created_at: epoch_millis(),
        compressed: datastore.compression_enabled(),
        encrypted: datastore.encryption_key().is_some(),
    };

    // find or create Backup
    let mut backup = index_file
        .backups
        .iter_mut()
        .find(|b| b.directory_name.as_str() == root_key)
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
    datastore.write_index_file(&index_file)
}

fn delete_older_than(datastore: &S3, days: i64) -> Result<(), Error> {
    let index_file = datastore.index_file()?;

    let threshold_date = Utc::now() - Duration::days(days);
    let threshold_date = threshold_date.timestamp_millis() as u128;

    let backups_to_delete: Vec<Backup> = index_file
        .backups
        .into_iter()
        .filter(|b| b.created_at.lt(&threshold_date))
        .collect();

    for backup in backups_to_delete {
        delete_by_name(&datastore, backup.directory_name.as_str())?
    }

    Ok(())
}

fn delete_keep_last(datastore: &S3, keep_last: usize) -> Result<(), Error> {
    let mut index_file = datastore.index_file()?;

    index_file
        .backups
        .sort_by(|a, b| b.created_at.cmp(&a.created_at));

    if let Some(backups) = index_file.backups.get(keep_last..) {
        for backup in backups {
            delete_by_name(&datastore, backup.directory_name.as_str())?;
        }
    }

    Ok(())
}

fn delete_by_name(datastore: &S3, backup_name: &str) -> Result<(), Error> {
    let mut index_file = datastore.index_file()?;

    let bucket = &datastore.bucket;

    let _ =
        delete_directory(&datastore.client, bucket, backup_name).map_err(|err| Error::from(err))?;

    index_file
        .backups
        .retain(|b| b.directory_name != backup_name);

    datastore.write_index_file(&index_file)
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
    use std::future::pending;

    use crate::cli::DumpDeleteArgs;
    use crate::config::Endpoint;
    use crate::connector::Connector;
    use crate::datastore::s3::{create_object, delete_bucket, delete_object, get_object, S3Error};
    use crate::datastore::{Backup, Datastore};
    use crate::utils::epoch_millis;
    use crate::S3;

    const BUCKET_NAME: &str = "replibyte-test";
    const REGION: &str = "us-east-2";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";
    const MINIO_CREDENTIALS: &str = "minioadmin";

    fn aws_bucket() -> String {
        format!("replibyte-test-{}", Faker.fake::<String>().to_lowercase())
    }

    fn gcp_bucket() -> String {
        "replibyte-test-us".to_string()
    }

    fn aws_credentials() -> (String, String) {
        (
            std::env::var("AWS_ACCESS_KEY_ID").unwrap_or(MINIO_CREDENTIALS.to_string()),
            std::env::var("AWS_SECRET_ACCESS_KEY").unwrap_or(MINIO_CREDENTIALS.to_string()),
        )
    }

    fn aws_s3(bucket: &str) -> S3 {
        let (access_key_id, secret_access_key) = aws_credentials();

        S3::aws(
            bucket.to_string(),
            "us-east-2".to_string(),
            access_key_id,
            secret_access_key,
            Endpoint::Custom(MINIO_ENDPOINT.to_string()),
        )
    }

    fn gcp_credentials() -> (String, String, Endpoint) {
        let endpoint = if std::env::var("GS_ACCESS_KEY").is_err() {
            Endpoint::Custom(MINIO_ENDPOINT.to_string())
        } else {
            Endpoint::Default
        };

        (
            std::env::var("GS_ACCESS_KEY").unwrap_or(MINIO_CREDENTIALS.to_string()),
            std::env::var("GS_SECRET").unwrap_or(MINIO_CREDENTIALS.to_string()),
            endpoint,
        )
    }

    fn gcp_s3(bucket: &str) -> S3 {
        let (access_key, secret, endpoint) = gcp_credentials();

        S3::gcp(
            bucket.to_string(),
            "us-central1".to_string(),
            access_key,
            secret,
            endpoint,
        )
    }

    #[test]
    fn init_s3() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());
        // executed twice to check that there is no error at the second call
        assert!(s3.init().is_ok());
        assert!(s3.init().is_ok());

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }

    #[test]
    fn create_and_get_and_delete_object_for_aws_s3() {
        let bucket = aws_bucket();

        let mut s3 = aws_s3(bucket.as_str());
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
    fn create_and_get_and_delete_object_for_gcp_s3() {
        let bucket = gcp_bucket();
        let mut s3 = gcp_s3(bucket.as_str());
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
    }

    #[test]
    fn test_s3_index_file() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

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
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

        s3.set_backup_name("custom-backup-name".to_string());

        assert_eq!(s3.root_key, "custom-backup-name".to_string())
    }

    #[test]
    fn test_s3_backup_delete_by_name() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

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
            .delete(&DumpDeleteArgs {
                dump: Some("backup-1".to_string()),
                older_than: None,
                keep_last: None,
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: Some("backup-2".to_string()),
                older_than: None,
                keep_last: None,
            })
            .is_ok());
        assert!(s3.index_file().unwrap().backups.is_empty());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
    }

    #[test]
    fn test_s3_backup_delete_older_than() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

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
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("6d".to_string()),
                keep_last: None,
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("5d".to_string()),
                keep_last: None,
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_ok());
    }

    #[test]
    fn test_s3_backup_keep_last() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

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
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: None,
                keep_last: Some(2),
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-3/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: None,
                keep_last: Some(1),
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "backup-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "backup-3/testing-key.dump").is_ok());
    }
}
