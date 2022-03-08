use std::collections::HashSet;

use crate::errors::DumpFileError;

pub mod errors;
pub mod postgres;
mod utils;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum Type {
    Postgres,
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
    fn databases(&self) -> Result<HashSet<LD>, DumpFileError>;
    /// find a logical database by name
    fn get_database<S: Into<&'a str>>(&self, name: S) -> Result<Option<LD>, DumpFileError> {
        let databases = self.databases()?;

        let db_name = name.into();
        for db in databases {
            if db.name() == db_name {
                return Ok(Some(db));
            }
        }

        Ok(None)
    }
}

pub trait FromDumpFile {
    fn dump_file_path(&self) -> &str;
}

#[cfg(test)]
mod tests {
    use crate::postgres::Postgres;
    use crate::{Database, LogicalDatabase, Type};

    #[test]
    fn parse_postgres() {
        let db = Postgres::new("../db/postgres/fulldump-with-inserts.sql");

        assert_eq!(db.database_type(), Type::Postgres);

        assert!(db.get_database("do not exists").unwrap().is_none());
    }

    #[test]
    fn list_postgres_databases() {
        let db = Postgres::new("../db/postgres/fulldump-with-inserts.sql");

        assert_eq!(db.databases().unwrap().len(), 1);
        assert_eq!(db.get_database("public").unwrap().unwrap().name(), "public");
    }

    #[test]
    fn list_postgres_tables() {
        let db = Postgres::new("../db/postgres/fulldump-with-inserts.sql");

        let db = db.get_database("public").unwrap().unwrap();
        let tables = db.tables().unwrap();

        assert_eq!(tables.len(), 14);
    }
}
