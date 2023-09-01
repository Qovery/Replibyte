use std::borrow::Cow;
use std::io::{Error, ErrorKind};
use std::str::FromStr;

use aws_config::profile::retry_config::ProfileFileRetryConfigProvider;
use aws_config::profile::{ProfileFileCredentialsProvider, ProfileFileRegionProvider};
use aws_sdk_s3::model::{
    BucketLocationConstraint, CreateBucketConfiguration, Delete, Object, ObjectIdentifier,
};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::{Client, Endpoint as SdkEndpoint};
use aws_types::region::Region;
use aws_types::Credentials;
use log::{error, info};
use serde_json::Value;

use crate::config::{AwsCredentials, Endpoint};
use crate::connector::Connector;
use crate::datastore::s3::S3Error::FailedObjectUpload;
use crate::datastore::{
    compress, decompress, decrypt, encrypt, Datastore, Dump, IndexFile, ReadOptions,
};
use crate::runtime::block_on;
use crate::types::Bytes;
use crate::utils::epoch_millis;

use super::INDEX_FILE_NAME;

const GOOGLE_CLOUD_STORAGE_URL: &str = "https://storage.googleapis.com";

pub struct S3 {
    bucket: String,
    root_key: String,
    region: Option<String>,
    endpoint: Endpoint,
    client: Client,
    enable_compression: bool,
    encryption_key: Option<String>,
}

impl S3 {
    pub fn aws<S>(
        bucket: S,
        region: Option<S>,
        profile: Option<S>,
        credentials: Option<AwsCredentials>,
        endpoint: Endpoint,
    ) -> anyhow::Result<Self>
    where
        S: 'static + AsRef<str> + Into<Cow<'static, str>> + Clone,
    {
        let mut config_loader = aws_config::from_env();

        if let Some(profile) = profile {
            config_loader = config_loader
                .region(
                    ProfileFileRegionProvider::builder()
                        .profile_name(profile.as_ref())
                        .build(),
                )
                .credentials_provider(
                    ProfileFileCredentialsProvider::builder()
                        .profile_name(profile.as_ref())
                        .build(),
                )
                .retry_config(
                    block_on(
                        ProfileFileRetryConfigProvider::builder()
                            .profile_name(profile.as_ref())
                            .build()
                            .retry_config_builder(),
                    )?
                    .build(),
                )
        }

        if let Some(region) = region.clone() {
            let region: Cow<str> = region.into();

            config_loader = config_loader.region(Region::new(region))
        }

        if let Some(credentials) = credentials {
            config_loader = config_loader.credentials_provider(Credentials::new(
                credentials.access_key_id,
                credentials.secret_access_key,
                credentials.session_token,
                None,
                "replibyte-config",
            ))
        }

        let sdk_config = block_on(config_loader.load());

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

        Ok(S3 {
            bucket: bucket.as_ref().into(),
            root_key: format!("dump-{}", epoch_millis()),
            region: region.map(|region| region.as_ref().into()),
            endpoint,
            client: Client::from_conf(s3_config),
            enable_compression: true,
            encryption_key: None,
        })
    }

    pub fn gcp<S>(
        bucket: S,
        region: S,
        access_key: S,
        secret: S,
        endpoint: Endpoint,
    ) -> anyhow::Result<Self>
    where
        S: 'static + AsRef<str> + Into<Cow<'static, str>> + Clone,
    {
        let endpoint = match endpoint {
            // change for default GCP Cloud Storage endpoint
            Endpoint::Default => Endpoint::Custom(GOOGLE_CLOUD_STORAGE_URL.to_string()),
            // passthrough
            Endpoint::Custom(url) => Endpoint::Custom(url),
        };

        S3::aws(
            bucket,
            Some(region),
            None,
            Some(AwsCredentials {
                access_key_id: access_key.as_ref().into(),
                secret_access_key: secret.as_ref().into(),
                session_token: None,
            }),
            endpoint,
        )
    }

    fn create_index_file(&self) -> Result<IndexFile, Error> {
        match self.index_file() {
            Ok(index_file) => Ok(index_file),
            Err(_) => {
                let index_file = IndexFile::new();
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
                let _ = create_bucket(&self.client, self.bucket.as_str(), self.region.as_ref())?;
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

    fn raw_index_file(&self) -> Result<Value, Error> {
        let object = get_object(&self.client, self.bucket.as_str(), INDEX_FILE_NAME)?;
        let index_file = serde_json::from_slice(object.as_slice())?;

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

    fn write_raw_index_file(&self, raw_index_file: &Value) -> Result<(), Error> {
        let index_file_json = serde_json::to_vec(raw_index_file)?;

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
        let dump = index_file.find_dump(options)?;

        for object in list_objects(
            &self.client,
            self.bucket.as_str(),
            Some(dump.directory_name.as_str()),
        )? {
            let data = get_object(&self.client, self.bucket.as_str(), object.key().unwrap())?;

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

    fn set_encryption_key(&mut self, key: String) {
        self.encryption_key = Some(key);
    }

    fn set_compression(&mut self, enable: bool) {
        self.enable_compression = enable;
    }

    fn set_dump_name(&mut self, name: String) {
        self.root_key = name;
    }

    fn compression_enabled(&self) -> bool {
        self.enable_compression
    }

    fn encryption_key(&self) -> &Option<String> {
        &self.encryption_key
    }

    fn delete_by_name(&self, name: String) -> Result<(), Error> {
        let mut index_file = self.index_file()?;

        let bucket = &self.bucket;

        let _ = delete_directory(&self.client, bucket, &name).map_err(|err| Error::from(err))?;

        index_file.dumps.retain(|b| b.directory_name != name);

        self.write_index_file(&index_file)
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

    let mut new_dump = Dump {
        directory_name: root_key.to_string(),
        size: 0,
        created_at: epoch_millis(),
        compressed: datastore.compression_enabled(),
        encrypted: datastore.encryption_key().is_some(),
    };

    // find or create dump
    let mut dump = index_file
        .dumps
        .iter_mut()
        .find(|b| b.directory_name.as_str() == root_key)
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

fn create_bucket<'a, S: AsRef<str>>(
    client: &Client,
    bucket: &'a str,
    region: Option<S>,
) -> Result<(), S3Error<'a>> {
    let mut cfg = CreateBucketConfiguration::builder();
    if let Some(region) = region {
        let constraint = BucketLocationConstraint::from(region.as_ref());
        cfg = cfg.location_constraint(constraint);
    }

    let cfg = cfg.build();

    if let Ok(_) = block_on(
        client
            .get_bucket_location()
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
            error!("{}", err.to_string());
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

    if let Err(err) = result {
        error!("{}", err.to_string());
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
            error!("{}", err.to_string());
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
                error!("{}", err.to_string());
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
    use serde_json::json;

    use crate::cli::DumpDeleteArgs;
    use crate::config::{AwsCredentials, Endpoint};
    use crate::connector::Connector;
    use crate::datastore::s3::{
        create_bucket, create_object, delete_bucket, delete_object, get_object, S3Error,
    };
    use crate::datastore::{Datastore, Dump, INDEX_FILE_NAME};
    use crate::migration::rename_backups_to_dumps::RenameBackupsToDump;
    use crate::migration::update_version_number::UpdateVersionNumber;
    use crate::migration::Migrator;
    use crate::utils::epoch_millis;
    use crate::S3;

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
            Some(REGION.to_string()),
            None,
            Some(AwsCredentials {
                access_key_id,
                secret_access_key,
                session_token: None,
            }),
            Endpoint::Custom(MINIO_ENDPOINT.to_string()),
        )
        .unwrap()
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
        .unwrap()
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

        assert!(index_file.dumps.is_empty());

        index_file.dumps.push(Dump {
            directory_name: "dump-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());

        assert_eq!(s3.index_file().unwrap().dumps.len(), 1);

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }

    #[test]
    fn test_dump_name() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

        s3.set_dump_name("custom-dump-name".to_string());

        assert_eq!(s3.root_key, "custom-dump-name".to_string())
    }

    #[test]
    fn test_s3_dump_delete_by_name() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.dumps.is_empty());

        // Add 2 dumps in the manifest
        index_file.dumps.push(Dump {
            directory_name: "dump-1".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        index_file.dumps.push(Dump {
            directory_name: "dump-2".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().dumps.len(), 2);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-2/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: Some("dump-1".to_string()),
                older_than: None,
                keep_last: None,
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().dumps.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "dump-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: Some("dump-2".to_string()),
                older_than: None,
                keep_last: None,
            })
            .is_ok());
        assert!(s3.index_file().unwrap().dumps.is_empty());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_err());
    }

    #[test]
    fn test_s3_dump_delete_older_than() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.dumps.is_empty());

        // Add a dump from 5 days ago
        index_file.dumps.push(Dump {
            directory_name: "dump-1".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(5)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        // Add a dump from now
        index_file.dumps.push(Dump {
            directory_name: "dump-2".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().dumps.len(), 2);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-2/testing-key.dump",
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

        assert_eq!(s3.index_file().unwrap().dumps.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "dump-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: Some("5d".to_string()),
                keep_last: None,
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().dumps.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "dump-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_ok());
    }

    #[test]
    fn test_s3_dump_keep_last() {
        let bucket = aws_bucket();
        let mut s3 = aws_s3(bucket.as_str());

        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let mut index_file = s3.index_file().unwrap();

        assert!(index_file.dumps.is_empty());

        index_file.dumps.push(Dump {
            directory_name: "dump-1".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(3)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        index_file.dumps.push(Dump {
            directory_name: "dump-2".to_string(),
            size: 0,
            created_at: (Utc::now() - Duration::days(5)).timestamp_millis() as u128,
            compressed: true,
            encrypted: false,
        });

        index_file.dumps.push(Dump {
            directory_name: "dump-3".to_string(),
            size: 0,
            created_at: epoch_millis(),
            compressed: true,
            encrypted: false,
        });

        assert!(s3.write_index_file(&index_file).is_ok());
        assert_eq!(s3.index_file().unwrap().dumps.len(), 3);

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-1/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-2/testing-key.dump",
            b"hello w0rld".to_vec(),
        )
        .is_ok());

        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            "dump-3/testing-key.dump",
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

        assert_eq!(s3.index_file().unwrap().dumps.len(), 2);
        assert!(get_object(&s3.client, bucket.as_str(), "dump-1/testing-key.dump").is_ok());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-3/testing-key.dump").is_ok());

        assert!(s3
            .delete(&DumpDeleteArgs {
                dump: None,
                older_than: None,
                keep_last: Some(1),
            })
            .is_ok());

        assert_eq!(s3.index_file().unwrap().dumps.len(), 1);
        assert!(get_object(&s3.client, bucket.as_str(), "dump-1/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-2/testing-key.dump").is_err());
        assert!(get_object(&s3.client, bucket.as_str(), "dump-3/testing-key.dump").is_ok());
    }

    #[test]
    fn test_migrate_add_index_file_version_and_rename_backups_to_dumps() {
        let bucket = aws_bucket();
        let s3 = aws_s3(bucket.as_str());

        // create a fake index file
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

        // create a test bucket
        assert!(create_bucket(&s3.client, bucket.as_str(), Some(REGION.to_string())).is_ok());

        // create a test metadata.json file
        assert!(create_object(
            &s3.client,
            bucket.as_str(),
            INDEX_FILE_NAME,
            value.to_string().into_bytes()
        )
        .is_ok());

        let mut s3: Box<dyn Datastore> = Box::new(s3);
        let migrator = Migrator::new(
            "0.7.3",
            &s3,
            vec![
                Box::new(UpdateVersionNumber::new("0.7.3")),
                Box::new(RenameBackupsToDump::default()),
            ],
        );
        assert!(migrator.migrate().is_ok());

        let _ = s3.init().expect("s3 init failed");

        // assert
        assert!(s3.index_file().is_ok());
        assert_eq!(s3.index_file().unwrap().v, Some("0.7.3".to_string()));
        assert_eq!(s3.index_file().unwrap().dumps.len(), 2);
        assert_eq!(
            s3.index_file().unwrap().dumps.get(0),
            Some(&Dump {
                directory_name: "dump-1653170039392".to_string(),
                size: 62279,
                created_at: 1234,
                compressed: true,
                encrypted: false
            })
        );
        assert_eq!(
            s3.index_file().unwrap().dumps.get(1),
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
