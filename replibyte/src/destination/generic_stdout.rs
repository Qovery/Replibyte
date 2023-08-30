use std::io::{stdout, Error, Write};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;

/// Stream dump output on stdout
#[derive(Default)]
pub struct GenericStdout {}

impl GenericStdout {
    pub fn new() -> Self {
        GenericStdout {}
    }
}



impl Connector for GenericStdout {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Destination for GenericStdout {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let mut stdout = stdout();
        let _ = stdout.write_all(data.as_slice());
        Ok(())
    }
}
