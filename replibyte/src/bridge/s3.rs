use std::io::{Error, ErrorKind};

use aws_config::provider_config::ProviderConfig;
use aws_sdk_s3::error::CreateBucketError;
use aws_sdk_s3::model::{BucketLocationConstraint, CreateBucketConfiguration};
use aws_sdk_s3::output::CreateBucketOutput;
use aws_sdk_s3::types::SdkError;
use aws_sdk_s3::Client;
use aws_types::os_shim_internal::Env;
use dump_parser::postgres::Keyword::Into;
use log::{error, info};

use crate::bridge::{Bridge, IndexFile};
use crate::connector::Connector;
use crate::runtime::block_on;
use crate::types::{Queries, Query};

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

    fn create_index_file(&self) -> Result<(), Error> {
        // TODO: Implement index creation logic
        Ok(())
    }
}

impl Connector for S3 {
    fn init(&mut self) -> Result<(), Error> {
        self.create_bucket()
    }
}

impl Bridge for S3 {
    fn index_file(&self) -> Result<IndexFile, Error> {
        // ? TODO: Implement index file search
        Ok(IndexFile { backups: vec![] })
    }

    fn save(&self, index_file: IndexFile) -> Result<(), Error> {
        Ok(())
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

#[cfg(test)]
mod tests {
    use crate::connector::Connector;
    use crate::S3;

    fn credentials() -> (String, String) {
        (
            std::env::var("AWS_ACCESS_KEY_ID").expect("missing AWS_ACCESS_KEY_ID env var"),
            std::env::var("AWS_SECRET_ACCESS_KEY").expect("missing AWS_SECRET_ACCESS_KEY env var"),
        )
    }

    fn s3() -> S3 {
        let (access_key_id, secret_access_key) = credentials();

        S3::new(
            "replibyte-test",
            "us-east-2",
            access_key_id.as_str(),
            secret_access_key.as_str(),
        )
    }

    #[test]
    fn init_s3() {
        let mut s3 = s3();
        assert!(s3.init().is_ok());
    }
}
