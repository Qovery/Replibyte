use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, InsertIntoQuery, OriginalQuery, Query};

pub struct Mongo<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Mongo<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Mongo {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for Mongo<'a> {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Source for Mongo<'a> {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error> {
        let s_port = self.port.to_string();

        let mut process = Command::new("mongodump")
            .args([
                "-h",
                self.host,
                "--port",
                s_port.as_str(),
                "--authenticationDatabase",
                "admin",
                "--db",
                self.database,
                "-u",
                self.username,
                "-p",
                self.password,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

        let reader = BufReader::new(stdout);

        read_and_transform(reader, transformers, query_callback);

        match process.wait() {
            Ok(exit_status) => {
                if !exit_status.success() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("command error: {:?}", exit_status.to_string()),
                    ));
                }
            }
            Err(err) => return Err(err),
        }

        Ok(())
    }
}

/// consume reader and apply transformation on INSERT INTO queries if needed
pub fn read_and_transform<R: Read, F: FnMut(OriginalQuery, Query)>(
    reader: BufReader<R>,
    transformers: &Vec<Box<dyn Transformer + '_>>,
    mut query_callback: F,
) {
    // create a map variable with Transformer by column_name
    let mut transformer_by_db_and_table_and_column_name: HashMap<String, &Box<dyn Transformer>> =
        HashMap::with_capacity(transformers.len());

    for transformer in transformers {
        let _ = transformer_by_db_and_table_and_column_name.insert(
            transformer.database_and_table_and_column_name(),
            transformer,
        );
    }

    // TODO // DUMP-PARSER
    // {
    //     Ok(_) => {}
    //     Err(err) => panic!("{:?}", err),
    // }
}

#[cfg(test)]
mod tests {
    use crate::Source;
    use std::vec;

    use crate::source::mongo::Mongo;
    use crate::transformer::transient::TransientTransformer;
    use crate::transformer::Transformer;

    fn get_mongo() -> Mongo<'static> {
        Mongo::new("localhost", 27017, "admin", "root", "password")
    }

    fn get_invalid_mongo() -> Mongo<'static> {
        Mongo::new("localhost", 27017, "admin", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_mongo();

        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_ok());

        let p = get_invalid_mongo();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_mongo();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        p.read(&transformers, |original_query, query| {
            assert!(original_query.data().len() > 0);
            assert!(query.data().len() > 0);
        })
        .unwrap();
    }
}
