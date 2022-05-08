use std::io::Error;

use crate::connector::Connector;
use crate::types::Bytes;

mod docker;
pub mod generic_stdout;
pub mod mongodb;
pub mod mongodb_docker;
pub mod mysql;
pub mod mysql_docker;
pub mod postgres;
pub mod postgres_docker;

pub trait Destination: Connector {
    fn write(&self, data: Bytes) -> Result<(), Error>;
}
