use std::io::Error;

use crate::connector::Connector;
use crate::types::Queries;

pub mod postgres;

pub trait Destination: Connector {
    fn insert(&self, data: Vec<u8>) -> Result<(), Error>;
}
