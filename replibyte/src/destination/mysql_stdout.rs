use std::io::{stdout, Error, Write};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;

/// Stream Mysql dump output on stdout
pub struct MysqlStdout {}

impl MysqlStdout {
    pub fn new() -> Self {
        MysqlStdout {}
    }
}

impl Default for MysqlStdout {
    fn default() -> Self {
        MysqlStdout {}
    }
}

impl Connector for MysqlStdout {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Destination for MysqlStdout {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let mut stdout = stdout();
        let _ = stdout.write_all(data.as_slice());
        Ok(())
    }
}
