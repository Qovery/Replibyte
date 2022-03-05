use crate::connector::Connector;
use crate::database::Database;

pub mod postgres;

pub trait Destination: Connector + Database {}
