mod tokenizer;

use crate::utils::read_dump;
use crate::{Database, DumpFileError, LogicalDatabase, Table, Type};
use std::io::BufRead;

pub struct Postgres<'a> {
    dump_file_path: &'a str,
}

impl<'a> Postgres<'a> {
    pub fn new<S: Into<&'a str>>(dump_file_path: S) -> Self {
        Postgres {
            dump_file_path: dump_file_path.into(),
        }
    }
}

impl<'a> Database for Postgres<'a> {
    fn database_type(&self) -> Type {
        Type::Postgres
    }

    fn dump_file_path(&self) -> &str {
        self.dump_file_path
    }

    fn databases(&self) -> Result<Vec<LogicalDatabase>, DumpFileError> {
        let buf = read_dump(self.dump_file_path)?;

        Ok(vec![])
    }
}
