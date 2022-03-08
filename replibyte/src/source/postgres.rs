use std::io::Error;

use crate::connector::Connector;
use crate::database::Database;
use crate::source::transformer::Transformer;
use crate::source::Source;

pub struct Postgres<'a> {
    current_row: u64,
    next_row: u64,
    connection_uri: &'a str,
    enable_tls: bool,
    transformer: Transformer,
}

impl<'a> Postgres<'a> {
    pub fn new(connection_uri: &'a str, enable_tls: bool) -> Self {
        Postgres {
            current_row: 0,
            next_row: 1,
            connection_uri,
            enable_tls,
            transformer: Transformer::None,
        }
    }

    pub fn set_transformer(&mut self, transformer: Transformer) {
        self.transformer = transformer
    }
}

impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        self.connect()
    }
}

impl<'a> Iterator for Postgres<'a> {
    type Item = crate::types::Row;

    fn next(&mut self) -> Option<Self::Item> {
        None
    }
}

impl<'a> Source for Postgres<'a> {
    fn transformer(&self) -> &Transformer {
        &self.transformer
    }

    fn current_row(&self) -> u64 {
        self.current_row
    }

    fn set_current_row(&mut self, current_row: u64) {
        self.current_row = current_row;
    }
}

impl<'a> Database for Postgres<'a> {
    fn connect(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::{Database, Postgres};

    fn get_postgres() -> Postgres<'static> {
        Postgres::new("postgres://root:password@localhost:5432", false)
    }

    fn get_invalid_postgres() -> Postgres<'static> {
        Postgres::new("postgres://root:wrongpassword@localhost:5432", false)
    }

    #[test]
    fn connect() {
        let mut p = get_postgres();
        assert!(p.connect().is_ok());

        let mut p = get_invalid_postgres();
        assert!(p.connect().is_err());
    }

    #[test]
    fn list_rows() {
        let mut p = get_postgres();
        assert!(p.connect().is_ok());

        for row in p {
            println!("{:?}", row.len())
        }
    }
}
