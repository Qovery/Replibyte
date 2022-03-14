use crate::connector::Connector;
use crate::database::Database;

pub mod postgres;
pub mod mysql;

pub trait Source: Connector + Database {}
