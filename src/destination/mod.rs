use crate::connector::Connector;
use crate::database::Database;

pub trait Destination: Connector + Database {}
