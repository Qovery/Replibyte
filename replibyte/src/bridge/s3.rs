use std::io::{Error, ErrorKind};

use aws_config::provider_config::ProviderConfig;
use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::types::ByteStream;
use aws_sdk_s3::Client;
use aws_types::os_shim_internal::Env;
use log::{error, info};

use crate::bridge::s3::S3Error::FailedObjectUpload;
use crate::bridge::{Bridge, IndexFile};
use crate::connector::Connector;
use crate::runtime::block_on;
use crate::types::{Queries, Query};

const INDEX_FILE_NAME: &str = "metadata.json";

pub struct S3 {
    bucket: String,
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
            region,
            client: Client::new(&sdk_config),
        }
    }

    fn create_bucket(&self) -> Result<(), Error> {
        let constraint = BucketLocationConstraint::from(self.region.as_str());
        let cfg = CreateBucketConfiguration::builder()
            .location_constraint(constraint)
            .build();

        if let Ok(result) = block_on(
            self.client
                .get_bucket_accelerate_configuration()
                .bucket(self.bucket.as_str())
                .send(),
        ) {
            info!("bucket {} exists", self.bucket.as_str());
            return Ok(());
        }

        let result = block_on(
            self.client
                .create_bucket()
                .create_bucket_configuration(cfg)
                .bucket(self.bucket.as_str())
                .send(),
        );

        match result {
            Ok(_) => {}
            Err(err) => {
                error!("{}", err);
                return Err(Error::new(ErrorKind::Other, err.to_string()));
            }
        }

        info!("bucket {} created", self.bucket.as_str());

        Ok(())
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
        let _ = self.create_bucket()?;
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

    fn upload(&self, file_part: u16, queries: &Queries) -> Result<(), Error> {
        // TODO: Implement upload logic
        Ok(())
    }

    fn download<F>(&self, query_callback: F) -> Result<(), Error>
    where
        F: FnMut(Query),
    {
        Ok(())
    }
}

#[derive(Debug, Eq, PartialEq)]
enum S3Error<'a> {
    ObjectDoesNotExist { bucket: &'a str, key: &'a str },
    FailedObjectDownload { bucket: &'a str, key: &'a str },
    FailedObjectUpload { bucket: &'a str, key: &'a str },
    FailedToDeleteObject { bucket: &'a str, key: &'a str },
}

impl<'a> From<S3Error<'a>> for Error {
    fn from(err: S3Error<'a>) -> Self {
        match err {
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
    use std::fmt::format;

    use fake::{Fake, Faker};

    use crate::bridge::s3::{create_object, delete_object, get_object, S3Error};
    use crate::bridge::Bridge;
    use crate::connector::Connector;
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
            get_object(&s3.client, BUCKET_NAME, key.as_str()).unwrap(),
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
        let bucket = bucket();
        let mut s3 = s3(bucket.as_str());
        let _ = s3.init().expect("s3 init failed");

        assert!(s3.index_file().is_ok());

        let index_file = s3.index_file().unwrap();

        assert!(index_file.backups.is_empty());
    }
}
