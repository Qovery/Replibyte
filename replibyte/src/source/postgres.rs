use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::process::{Command, Stdio};

use dump_parser::postgres::{
    get_column_names_from_insert_into_query, get_column_values_from_insert_into_query,
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::list_queries_from_dump_reader;

use crate::connector::Connector;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, InsertIntoQuery, OriginalQuery, Query};

pub const COMMENT_CHARS: &str = "--";

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
        // TODO check pg_dump binary available
        Ok(())
    }
}

impl<'a> Source for Postgres<'a> {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        mut query_callback: F,
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

    match list_queries_from_dump_reader(reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);

        if match_keyword_at_position(Keyword::Insert, &tokens, 0)
            && match_keyword_at_position(Keyword::Into, &tokens, 2)
        {
            if let Some(database_name) = get_word_value_at_position(&tokens, 4) {
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

                    let mut original_columns = vec![];
                    let mut columns = vec![];

                    for (i, column_name) in column_names.iter().enumerate() {
                        let value_token = column_values.get(i).unwrap();

                        let column = match value_token {
                            Token::Number(column_value, _) => {
                                if column_value.contains(".") {
                                    Column::FloatNumberValue(
                                        column_name.to_string(),
                                        column_value.parse::<f64>().unwrap(),
                                    )
                                } else {
                                    Column::NumberValue(
                                        column_name.to_string(),
                                        column_value.parse::<i128>().unwrap(),
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
                            _ => Column::None(column_name.to_string()),
                        };

                        // get the right transformer for the right column name
                        let original_column = column.clone();

                        let db_and_table_and_column_name =
                            format!("{}.{}.{}", database_name, table_name, *column_name);
                        let column = match transformer_by_db_and_table_and_column_name
                            .get(db_and_table_and_column_name.as_str())
                        {
                            Some(transformer) => transformer.transform(column), // apply transformation on the column
                            None => column,
                        };

                        original_columns.push(original_column);
                        columns.push(column);
                    }

                    query_callback(
                        to_query(
                            Some(database_name),
                            InsertIntoQuery {
                                table_name: table_name.to_string(),
                                columns: original_columns,
                            },
                        ),
                        to_query(
                            Some(database_name),
                            InsertIntoQuery {
                                table_name: table_name.to_string(),
                                columns,
                            },
                        ),
                    )
                }
            }
        } else {
            // other rows than `INSERT INTO ...`
            query_callback(
                // there is no diff between the original and the modified one
                Query(query.as_bytes().to_vec()),
                Query(query.as_bytes().to_vec()),
            )
        }
    }) {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

fn to_query(database: Option<&str>, query: InsertIntoQuery) -> Query {
    let mut column_names = Vec::with_capacity(query.columns.len());
    let mut values = Vec::with_capacity(query.columns.len());

    for column in query.columns {
        match column {
            Column::NumberValue(column_name, value) => {
                column_names.push(column_name);
                values.push(value.to_string());
            }
            Column::FloatNumberValue(column_name, value) => {
                column_names.push(column_name);
                values.push(value.to_string());
            }
            Column::StringValue(column_name, value) => {
                column_names.push(column_name);
                values.push(format!("'{}'", value.replace("'", "''")));
            }
            Column::CharValue(column_name, value) => {
                column_names.push(column_name);
                values.push(format!("'{}'", value));
            }
            Column::None(column_name) => {
                column_names.push(column_name);
                values.push("NULL".to_string());
            }
        }
    }

    let query_prefix = match database {
        Some(database) => format!("INSERT INTO {}.", database),
        None => "INSERT INTO ".to_string(),
    };

    let query_string = format!(
        "{}{} ({}) VALUES ({});",
        query_prefix,
        query.table_name.as_str(),
        column_names.join(", "),
        values.join(", "),
    );

    Query(query_string.into_bytes())
}

#[cfg(test)]
mod tests {
    use crate::Source;
    use std::str;
    use std::vec;

    use crate::source::postgres::{to_query, Postgres};
    use crate::transformer::random::RandomTransformer;
    use crate::transformer::transient::TransientTransformer;
    use crate::transformer::Transformer;
    use crate::types::{Column, InsertIntoQuery};

    fn get_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5432, "root", "root", "password")
    }

    fn get_invalid_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5432, "root", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_postgres();

        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_ok());

        let p = get_invalid_postgres();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_postgres();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        p.read(&transformers, |original_query, query| {
            assert!(original_query.data().len() > 0);
            assert!(query.data().len() > 0);
        });
    }

    #[test]
    fn test_to_row() {
        let query = to_query(
            None,
            InsertIntoQuery {
                table_name: "test".to_string(),
                columns: vec![Column::StringValue(
                    "first_name".to_string(),
                    "romaric".to_string(),
                )],
            },
        );

        assert_eq!(
            query.data(),
            b"INSERT INTO test (first_name) VALUES ('romaric');"
        );

        let query = to_query(
            Some("public"),
            InsertIntoQuery {
                table_name: "test".to_string(),
                columns: vec![
                    Column::StringValue("first_name".to_string(), "romaric".to_string()),
                    Column::FloatNumberValue("height_in_meters".to_string(), 1.78),
                ],
            },
        );

        assert_eq!(
            query.data(),
            b"INSERT INTO public.test (first_name, height_in_meters) VALUES ('romaric', 1.78);"
        );

        let query = to_query(
            Some("public"),
            InsertIntoQuery {
                table_name: "test".to_string(),
                columns: vec![
                    Column::None("first_name".to_string()),
                    Column::FloatNumberValue("height_in_meters".to_string(), 1.78),
                ],
            },
        );

        assert_eq!(
            query.data(),
            b"INSERT INTO public.test (first_name, height_in_meters) VALUES (NULL, 1.78);"
        );

        let query = to_query(
            Some("public"),
            InsertIntoQuery {
                table_name: "test".to_string(),
                columns: vec![
                    Column::StringValue("first_name".to_string(), "romaric".to_string()),
                    Column::FloatNumberValue("height_in_meters".to_string(), 1.78),
                    Column::StringValue(
                        "description".to_string(),
                        "I'd like to say... I don't know.".to_string(),
                    ),
                ],
            },
        );

        assert_eq!(
            query.data(),
            b"INSERT INTO public.test (first_name, height_in_meters, description) \
            VALUES ('romaric', 1.78, 'I''d like to say... I don''t know.');"
        );
    }

    #[test]
    fn list_rows_and_hide_last_name() {
        let p = get_postgres();

        let database_name = "public";
        let table_name = "employees";
        let column_name_to_obfuscate = "last_name";

        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let t2: Box<dyn Transformer> = Box::new(RandomTransformer::new(
            database_name,
            table_name,
            column_name_to_obfuscate,
        ));

        let transformers = vec![t1, t2];

        p.read(&transformers, |original_query, query| {
            assert!(query.data().len() > 0);
            assert!(query.data().len() > 0);

            let query_str = str::from_utf8(query.data()).unwrap();

            if query_str.contains(database_name)
                && query_str.contains(table_name)
                && query_str.starts_with("INSERT INTO")
            {
                assert_ne!(query.data(), original_query.data());
                // TODO to complete to better check the column change only
            } else {
                assert_eq!(query.data(), original_query.data());
            }
        });
    }
}
