use std::io::Error;

use crate::connector::Connector;
use crate::types::Bytes;

pub mod mongodb;
pub mod mongodb_stdout;
pub mod postgres;
pub mod postgres_stdout;

pub trait Destination: Connector {
    fn write(&self, data: Bytes) -> Result<(), Error>;
}
