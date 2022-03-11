use std::io::{BufReader, Error, ErrorKind};
use std::process::{Command, Stdio};

use dump_parser::postgres::{
    get_column_names_from_insert_into_query, get_column_values_from_insert_into_query,
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::list_queries_from_dump_reader;

use crate::connector::Connector;
use crate::database::Database;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, Row};

pub struct Postgres<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Postgres<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Postgres {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Source for Postgres<'a> {}

impl<'a> Database for Postgres<'a> {
    fn stream_rows<T: Transformer, F: FnMut(Row)>(
        &self,
        transformer: &T,
        mut row: F,
    ) -> Result<(), Error> {
        let s_port = self.port.to_string();

        let mut process = Command::new("pg_dump")
            .env("PGPASSWORD", self.password)
            .args([
                "--column-inserts",
                "-h",
                self.host,
                "-p",
                s_port.as_str(),
                "-d",
                self.database,
                "-U",
                self.username,
            ])
            //.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

        let reader = BufReader::new(stdout);

        match list_queries_from_dump_reader(reader, |query| {
            let tokens = get_tokens_from_query_str(query);

            if match_keyword_at_position(Keyword::Insert, &tokens, 0)
                && match_keyword_at_position(Keyword::Into, &tokens, 2)
            {
                if let Some(table_name) = get_word_value_at_position(&tokens, 6) {
                    // find database name by filtering out all queries starting with
                    // INSERT INTO <database>.<table> (...)
                    // INSERT       -> position 0
                    // INTO         -> position 2
                    // <table>      -> position 6
                    // L Paren      -> position X?
                    // R Paren      -> position X?

                    let column_names = get_column_names_from_insert_into_query(&tokens);
                    let column_values = get_column_values_from_insert_into_query(&tokens);

                    let mut columns = vec![];
                    for (i, column_name) in column_names.iter().enumerate() {
                        let value_token = column_values.get(i).unwrap();

                        // TODO transform column value by column name

                        let column = match value_token {
                            Token::Number(column_value, signed) => {
                                if *signed {
                                    Column::IntValue(
                                        column_name.to_string(),
                                        column_value.parse::<i64>().unwrap(),
                                    )
                                } else {
                                    Column::UIntValue(
                                        column_name.to_string(),
                                        column_value.parse::<u64>().unwrap(),
                                    )
                                }
                            }
                            Token::Char(column_value) => {
                                Column::CharValue(column_name.to_string(), column_value.clone())
                            }
                            Token::SingleQuotedString(column_value) => {
                                Column::StringValue(column_name.to_string(), column_value.clone())
                            }
                            Token::NationalStringLiteral(column_value) => {
                                Column::StringValue(column_name.to_string(), column_value.clone())
                            }
                            Token::HexStringLiteral(column_value) => {
                                Column::StringValue(column_name.to_string(), column_value.clone())
                            }
                            _ => Column::None,
                        };

                        columns.push(column);
                    }

                    row(transformer.transform(Row {
                        table_name: table_name.to_string(),
                        columns,
                    }))
                }
            }
        }) {
            Ok(_) => {}
            Err(err) => panic!("{:?}", err),
        }

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

#[cfg(test)]
mod tests {
    use crate::database::Database;
    use crate::transformer::NoTransformer;
    use crate::Postgres;

    fn get_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5432, "root", "root", "password")
    }

    fn get_invalid_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5432, "root", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_postgres();
        let t = NoTransformer::default();
        assert!(p.stream_rows(&t, |_| {}).is_ok());

        let p = get_invalid_postgres();
        let t = NoTransformer::default();
        assert!(p.stream_rows(&t, |_| {}).is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_postgres();
        let t = NoTransformer::default();
        p.stream_rows(&t, |row| {
            assert!(row.table_name.len() > 0);
            assert!(row.columns.len() > 0);
        });
    }
}
