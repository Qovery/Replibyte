use std::io::{Error, ErrorKind};

use aws_config::provider_config::ProviderConfig;
use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration, Object};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use aws_types::os_shim_internal::Env;
use log::{error, info};

use crate::bridge::s3::S3Error::FailedObjectUpload;
use crate::bridge::{Backup, Bridge, DownloadOptions, IndexFile};
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
}

impl S3 {
    pub fn new<S: Into<String>>(
        bucket: S,
        region: S,
        access_key_id: S,
        secret_access_key: S,
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

        S3 {
            bucket: bucket.into().to_string(),
            root_key: format!("backup-{}", epoch_millis()),
            region,
            client: Client::new(&sdk_config),
        }
    }

    fn create_index_file(&self) -> Result<IndexFile, Error> {
        match self.index_file() {
            Ok(index_file) => Ok(index_file),
            Err(_) => {
                let index_file = IndexFile { backups: vec![] };
                let _ = self.save(&index_file)?;
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

    fn save(&self, index_file: &IndexFile) -> Result<(), Error> {
        let index_file_json = serde_json::to_vec(index_file)?;

        create_object(
            &self.client,
            self.bucket.as_str(),
            INDEX_FILE_NAME,
            index_file_json,
        )
        .map_err(|err| Error::from(err))
    }

    fn upload(&self, file_part: u16, data: Bytes) -> Result<(), Error> {
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
        self.save(&index_file)
    }

    fn download<'a, F>(&self, options: &DownloadOptions, mut data_callback: F) -> Result<(), Error>
    where
        F: FnMut(Bytes),
    {
        let mut index_file = self.index_file()?;

        let backup = match options {
            DownloadOptions::Latest => {
                index_file
                    .backups
                    .sort_by(|a, b| a.created_at.cmp(&b.created_at));

                match index_file.backups.last() {
                    Some(backup) => backup,
                    None => return Err(Error::new(ErrorKind::Other, "No backups available.")),
                }
            }
            DownloadOptions::Backup { name } => {
                match index_file
                    .backups
                    .iter()
                    .find(|backup| backup.directory_name.as_str() == name.as_str())
                {
                    Some(backup) => backup,
                    None => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("Can't find backup with name '{}'", name),
                        ))
                    }
                }
            }
        };

        for object in list_objects(
            &self.client,
            self.bucket.as_str(),
            Some(backup.directory_name.as_str()),
        )? {
            let data = get_object(&self.client, self.bucket.as_str(), object.key().unwrap())?;
            data_callback(data);
        }

        Ok(())
    }
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
    let objects = objects
        .into_iter()
        .filter(|object| match object.key() {
            Some(key) => key.starts_with(path),
            None => false,
        })
        .collect::<Vec<_>>();

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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use fake::{Fake, Faker};

    use crate::bridge::s3::{create_object, delete_bucket, delete_object, get_object, S3Error};
    use crate::bridge::{Backup, Bridge};
    use crate::connector::Connector;
    use crate::utils::epoch_millis;
    use crate::S3;

    const BUCKET_NAME: &str = "replibyte-test";
    const REGION: &str = "us-east-2";

    fn bucket() -> String {
        format!("replibyte-test-{}", Faker.fake::<String>().to_lowercase())
    }

    fn credentials() -> (String, String) {
        (
            std::env::var("AWS_ACCESS_KEY_ID").expect("missing AWS_ACCESS_KEY_ID env var"),
            std::env::var("AWS_SECRET_ACCESS_KEY").expect("missing AWS_SECRET_ACCESS_KEY env var"),
        )
    }

    fn s3(bucket: &str) -> S3 {
        let (access_key_id, secret_access_key) = credentials();

        S3::new(
            bucket,
            "us-east-2",
            access_key_id.as_str(),
            secret_access_key.as_str(),
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
        });

        assert!(s3.save(&index_file).is_ok());

        assert_eq!(s3.index_file().unwrap().backups.len(), 1);

        assert!(delete_bucket(&s3.client, bucket.as_str(), true).is_ok());
    }
}
