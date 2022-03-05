use crate::errors::{DumpFileError, Error};

pub mod errors;
pub mod postgres;
mod utils;

#[derive(Debug, PartialOrd, PartialEq, Ord, Eq)]
pub enum Type {
    Postgres,
}

pub struct LogicalDatabase<'a> {
    name: Vec<u8>,
    tables: Vec<Table<'a>>,
}

pub struct Table<'a> {
    rows: Vec<Row<'a>>,
}

pub struct Row<'a> {
    columns: Vec<Column<'a>>,
}

pub struct Column<'a> {
    name: &'a str,
    value: Vec<u8>,
}

pub trait Database {
    fn database_type(&self) -> Type;
    fn dump_file_path(&self) -> &str;
    fn databases(&self) -> Result<Vec<LogicalDatabase>, DumpFileError>;
    fn get_database<S: Into<Vec<u8>>>(
        &self,
        name: S,
    ) -> Result<Option<LogicalDatabase>, DumpFileError> {
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
        let db = Postgres::new("../db/postgres/fulldump.sql");

        assert_eq!(db.database_type(), Type::Postgres);

        assert!(db.get_database("do not exists").unwrap().is_none());
    }

    #[test]
    fn list_postgres_databases() {
        let db = Postgres::new("../db/postgres/fulldump.sql");

        assert_eq!(db.databases().unwrap().len(), 1);
        assert_eq!(db.get_database("public").unwrap().unwrap().name, b"public");
    }
}
