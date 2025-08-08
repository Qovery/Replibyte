#![allow(clippy::manual_is_ascii_check)]
#![allow(clippy::redundant_closure)]
#![allow(clippy::needless_borrow)]
#![allow(clippy::match_like_matches_macro)]
#![allow(clippy::comparison_chain)]
#![allow(clippy::let_unit_value)]
#![allow(clippy::get_first)]
#![allow(clippy::if_same_then_else)]
#![allow(clippy::eq_op)]
#![allow(clippy::partialeq_to_none)]
#![allow(clippy::ptr_arg)]
#![allow(unused_variables)]
#![allow(dead_code)]

use std::io::{BufReader, Read};

use crate::errors::DumpFileError;

pub mod errors;
// pub mod mongodb; // Temporarily disabled due to crc dependency issue
pub mod mysql;
pub mod optimized_parser;
pub mod performance_test;
pub mod postgres;
pub mod simd_ops;
pub mod utils;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum Type {
    Postgres,
    Mysql,
}

pub trait LogicalDatabase<'a, T>
where
    T: Table,
{
    fn name(&self) -> &str;
    fn tables(&self) -> Result<Vec<T>, DumpFileError>;
}

pub trait Table {
    fn rows(&self) -> &'static Vec<Row>;
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Row {
    columns: Vec<Column>,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Column {
    name: String,
    value: Vec<u8>,
}

pub trait Database<'a, LD, T>
where
    LD: LogicalDatabase<'a, T>,
    T: Table,
{
    fn database_type(&self) -> Type;
    /// list logical databases available
    fn databases<R: Read>(&self, dump_reader: BufReader<R>) -> Result<Vec<LD>, DumpFileError>;
    /// find a logical database by name
    fn get_database<S: Into<&'a str>, R: Read>(
        &self,
        name: S,
        dump_reader: BufReader<R>,
    ) -> Result<Option<LD>, DumpFileError> {
        let databases = self.databases(dump_reader)?;

        let db_name = name.into();
        for db in databases {
            if db.name() == db_name {
                return Ok(Some(db));
            }
        }

        Ok(None)
    }
}
