use std::io::{BufReader, Read};

use crate::errors::DumpFileError;

pub mod errors;
pub mod postgres;
pub mod utils;

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

#[cfg(test)]
mod tests {
    use crate::postgres::Postgres;
    use crate::{Database, LogicalDatabase, Type};
    use std::fs::File;
    use std::io::BufReader;

    fn get_buffer_reader() -> BufReader<File> {
        let file = match File::open("../db/postgres/fulldump-with-inserts.sql") {
            Ok(file) => file,
            Err(err) => panic!("{:?}", err),
        };

        BufReader::new(file)
    }

    #[test]
    fn parse_postgres() {
        let dump_reader = get_buffer_reader();
        let db = Postgres::new();

        assert_eq!(db.database_type(), Type::Postgres);

        assert!(db
            .get_database("do not exists", dump_reader)
            .unwrap()
            .is_none());
    }

    #[test]
    fn list_postgres_databases() {
        let dump_reader = get_buffer_reader();
        let db = Postgres::new();

        let databases = db.databases(dump_reader).unwrap();
        assert_eq!(databases, 1);
        assert_eq!(databases.first().unwrap().name(), "public");
    }

    #[test]
    fn list_postgres_tables() {
        let dump_reader = get_buffer_reader();
        let db = Postgres::new();

        let db = db.get_database("public", dump_reader).unwrap().unwrap();

        let dump_reader = get_buffer_reader();
        let tables = db.tables().unwrap();

        assert_eq!(tables.len(), 14);
    }
}
