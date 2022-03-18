use crate::bridge::{Bridge, IndexFile};
use crate::connector::Connector;
use crate::types::{Queries, Query};
use std::io::Error;

pub struct S3 {}

impl S3 {
    pub fn new() -> Self {
        // TODO: Manage s3 credentials and bucket to use
        S3 {}
    }

    fn create_bucket(&self) -> Result<(), Error> {
        // TODO: Implement bucket creation logic
        todo!()
    }

    fn create_index_file(&self) -> Result<(), Error> {
        // TODO: Implement index creation logic
        todo!()
    }
}

impl Connector for S3 {
    fn init(&mut self) -> Result<(), Error> {
        // TODO: Implement S3 bucket creation/connection logic
        todo!()
    }
}

impl Bridge for S3 {
    fn index_file(&self) -> IndexFile {
        // ? TODO: Implement index file search
        todo!()
    }

    fn upload(&self, file_part: u16, queries: &Queries) -> Result<(), Error> {
        // TODO: Implement upload logic
        todo!()
    }

    fn download<F>(&self, query_callback: F) -> Result<(), Error>
    where
        F: FnMut(Query),
    {
        todo!()
    }
}
