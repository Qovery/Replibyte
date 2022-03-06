use crate::errors::{DumpFileError, Error};
use std::collections::HashSet;

pub mod errors;
pub mod postgres;
mod utils;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum Type {
    Postgres,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct LogicalDatabase<'a, DB: ?Sized>
where
    DB: Database,
{
    name: String,
    database: &'a DB,
    tables: Vec<Table<'a>>,
}

impl<'a, DB> LogicalDatabase<'a, DB>
where
    DB: Database,
{
    pub fn new(name: String, database: &'a DB) -> Self {
        LogicalDatabase {
            name,
            database,
            tables: vec![],
        }
    }

    pub fn tables(&self) -> &Vec<Table<'a>> {
        &self.tables
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Table<'a> {
    rows: Vec<Row<'a>>,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Row<'a> {
    columns: Vec<Column<'a>>,
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Column<'a> {
    name: &'a str,
    value: Vec<u8>,
}

pub trait Database {
    fn database_type(&self) -> Type;
    fn dump_file_path(&self) -> &str;
    /// list logical databases available
    fn databases(&self) -> Result<HashSet<LogicalDatabase<Self>>, DumpFileError>;
    /// find a logical database by name
    fn get_database<'a, S: Into<&'a str>>(
        &self,
        name: S,
    ) -> Result<Option<LogicalDatabase<Self>>, DumpFileError> {
        let databases = self.databases()?;

        let db_name = name.into();
        for db in databases {
            if &db.name == &db_name {
                return Ok(Some(db));
            }
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::postgres::Postgres;
    use crate::{Database, Type};

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
        assert_eq!(db.get_database("public").unwrap().unwrap().name, "public");
    }
}
