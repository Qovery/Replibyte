use std::io::{stdout, Error, Write};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;

/// Stream MongoDB dump output on stdout
pub struct MongoDBStdout {}

impl MongoDBStdout {
    pub fn new() -> Self {
        MongoDBStdout {}
    }
}

impl Default for MongoDBStdout {
    fn default() -> Self {
        MongoDBStdout {}
    }
}

impl Connector for MongoDBStdout {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Destination for MongoDBStdout {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let mut stdout = stdout();
        let _ = stdout.write_all(data.as_slice());
        Ok(())
    }
}
