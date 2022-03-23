use std::io::{stdout, Error, ErrorKind, Write};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;

/// Stream Postgres dump output on stdout
pub struct PostgresStdout {}

impl PostgresStdout {
    pub fn new() -> Self {
        PostgresStdout {}
    }
}

impl Default for PostgresStdout {
    fn default() -> Self {
        PostgresStdout {}
    }
}

impl Connector for PostgresStdout {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Destination for PostgresStdout {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let mut stdout = stdout();
        let _ = stdout.write_all(data.as_slice());
        Ok(())
    }
}
