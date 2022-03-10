use crate::connector::Connector;
use crate::database::Database;
use crate::source::transformer::Transformer;

pub mod postgres;
pub mod transformer;

pub trait Source: Connector + Database {
    fn transformer(&self) -> &Transformer;
    fn set_transformer(&mut self, transformer: Transformer);
}
