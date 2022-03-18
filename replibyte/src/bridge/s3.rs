use crate::bridge::{Bridge, IndexFile};
use crate::connector::Connector;
use crate::types::{Queries, Query};
use std::io::Error;

pub struct S3 {}

impl S3 {
    // TODO manage s3 credentials and bucket to use
    pub fn new() -> Self {
        S3 {}
    }

    fn create_bucket(&self) -> Result<(), Error> {
        // TODO
        Ok(())
    }

    fn create_index_file(&self) -> Result<(), Error> {
        // TODO
        Ok(())
    }
}

impl Connector for S3 {
    fn init(&mut self) -> Result<(), Error> {
        // TODO create S3 bucket
        todo!()
    }
}

impl Bridge for S3 {
    fn index_file(&self) -> IndexFile {
        todo!()
    }

    fn upload(&self, file_part: u16, queries: &Queries) -> Result<(), Error> {
        Ok(())
    }

    fn download<F>(&self, query_callback: F) -> Result<(), Error>
    where
        F: FnMut(Query),
    {
        todo!()
    }
}
