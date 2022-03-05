use crate::connector::Connector;
use crate::database::Database;
use crate::source::transformer::Transformer;

pub mod postgres;
pub mod transformer;

pub trait Source: Connector + Database + Iterator {
    fn transformer(&self) -> &Transformer;
    fn current_row(&self) -> u64;
    fn set_current_row(&mut self, current_row: u64);
}
