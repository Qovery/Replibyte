use crate::bridge::Bridge;
use crate::connector::Connector;
use std::io::Error;

pub struct S3 {}

impl S3 {
    pub fn new() -> Self {
        S3 {}
    }
}

impl Connector for S3 {
    fn init(&mut self) -> Result<(), Error> {
        todo!()
    }
}

impl Bridge for S3 {}
