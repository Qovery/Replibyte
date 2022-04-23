use std::borrow::BorrowMut;
use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::process::{Command, Stdio};

use dump_parser::mysql::{
    get_column_names_from_insert_into_query, get_column_values_from_insert_into_query,
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::{list_queries_from_dump_reader, ListQueryResult};

use crate::connector::Connector;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, InsertIntoQuery, OriginalQuery, Query};
use crate::utils::binary_exists;

use super::SourceOptions;

pub const COMMENT_CHARS: &str = "--";

enum RowType {
    InsertInto {
        database_name: String,
        table_name: String,
    },
    CreateTable {
        database_name: String,
        table_name: String,
    },
    AlterTable {
        database_name: String,
        table_name: String,
    },
    Others,
}

pub struct Mysql<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Mysql<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Self {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for Mysql<'a> {
    fn init(&mut self) -> Result<(), Error> {
        binary_exists("mysqldump")
    }
}

impl<'a> Source for Mysql<'a> {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        options: SourceOptions,
        query_callback: F,
    ) -> Result<(), Error> {
        let s_port = self.port.to_string();

        // use pg_dumpall instead of pg_dump to get all the users, roles and permissions
        let mut process = Command::new("mysqldump")
            .args([
                "-h",
                self.host,
                "-P",
                s_port.as_str(),
                "-u",
                self.username,
                &format!("-p{}", self.password),
                "--add-drop-database", // add DROP DATABASE statement before each CREATE DATABASE statement
                "--add-drop-table", // add DROP TABLE statement before each CREATE TABLE statement
                "--skip-extended-insert", // have a row by INSERT INTO statement
                "--complete-insert", // have column names in INSERT INTO rows
                "--single-transaction", // https://dev.mysql.com/doc/refman/8.0/en/mysqldump.html#option_mysqldump_single-transaction
                "--quick", // reads out large tables in a way that doesn't require having enough RAM to fit the full table in memory
                "--databases",
                self.database,
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

        let reader = BufReader::new(stdout);

        read_and_transform(reader, options, query_callback);

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

fn read_and_transform<R: Read, F: FnMut(OriginalQuery, Query)>(
    reader: BufReader<R>,
    options: SourceOptions,
    mut query_callback: F,
) {
    // create a map variable with Transformer by column_name
    let mut transformer_by_db_and_table_and_column_name: HashMap<String, &Box<dyn Transformer>> =
        HashMap::with_capacity(options.transformers.len());

    for transformer in options.transformers {
        let _ = transformer_by_db_and_table_and_column_name.insert(
            transformer.database_and_table_and_column_name(),
            transformer,
        );
    }

    let mut skip_tables_map: HashMap<String, bool> =
        HashMap::with_capacity(options.skip_config.len());
    for skip in options.skip_config {
        let _ = skip_tables_map.insert(format!("{}.{}", skip.database, skip.table), true);
    }

    match list_queries_from_dump_reader(reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);

        match get_row_type(&tokens) {
            RowType::InsertInto {
                database_name,
                table_name,
            } => {
                if !skip_tables_map.contains_key(&format!("{}.{}", database_name, table_name)) {
                    let (original_columns, columns) = transform_columns(
                        database_name.as_str(),
                        table_name.as_str(),
                        &tokens,
                        &transformer_by_db_and_table_and_column_name,
                    );

                    query_callback(
                        to_query(
                            Some(database_name.as_str()),
                            InsertIntoQuery {
                                table_name: table_name.to_string(),
                                columns: original_columns,
                            },
                        ),
                        to_query(
                            Some(database_name.as_str()),
                            InsertIntoQuery {
                                table_name: table_name.to_string(),
                                columns,
                            },
                        ),
                    )
                }
            }
            RowType::CreateTable {
                database_name,
                table_name,
            } => {
                if !skip_tables_map.contains_key(&format!("{}.{}", database_name, table_name)) {
                    no_change_query_callback(query_callback.borrow_mut(), query);
                }
            }
            RowType::AlterTable {
                database_name,
                table_name,
            } => {
                if !skip_tables_map.contains_key(&format!("{}.{}", database_name, table_name)) {
                    no_change_query_callback(query_callback.borrow_mut(), query);
                }
            }
            RowType::Others => {
                // other rows than `INSERT INTO ...` and `CREATE TABLE ...`
                no_change_query_callback(query_callback.borrow_mut(), query);
            }
        }

        ListQueryResult::Continue
    }) {
        Ok(_) => {}
        Err(err) => panic!("{:?}", err),
    }
}

fn no_change_query_callback<F: FnMut(OriginalQuery, Query)>(query_callback: &mut F, query: &str) {
    query_callback(
        // there is no diff between the original and the modified one
        Query(query.as_bytes().to_vec()),
        Query(query.as_bytes().to_vec()),
    );
}

fn transform_columns(
    database_name: &str,
    table_name: &str,
    tokens: &Vec<Token>,
    transformer_by_db_and_table_and_column_name: &HashMap<String, &Box<dyn Transformer>>,
) -> (Vec<Column>, Vec<Column>) {
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

    (original_columns, columns)
}

fn is_insert_into_statement(tokens: &Vec<Token>) -> bool {
    match_keyword_at_position(Keyword::Insert, &tokens, 0)
        && match_keyword_at_position(Keyword::Into, &tokens, 2)
}

fn is_create_table_statement(tokens: &Vec<Token>) -> bool {
    match_keyword_at_position(Keyword::Create, &tokens, 0)
        && match_keyword_at_position(Keyword::Table, &tokens, 2)
}

fn is_alter_table_statement(tokens: &Vec<Token>) -> bool {
    match_keyword_at_position(Keyword::Alter, &tokens, 0)
        && match_keyword_at_position(Keyword::Table, &tokens, 2)
}

fn get_row_type(tokens: &Vec<Token>) -> RowType {
    let mut row_type = RowType::Others;

    if is_insert_into_statement(&tokens) {
        if let Some(database_name) = get_word_value_at_position(&tokens, 4) {
            if let Some(table_name) = get_word_value_at_position(&tokens, 6) {
                row_type = RowType::InsertInto {
                    database_name: database_name.to_string(),
                    table_name: table_name.to_string(),
                };
            }
        }
    }

    if is_create_table_statement(&tokens) {
        if let Some(database_name) = get_word_value_at_position(&tokens, 4) {
            if let Some(table_name) = get_word_value_at_position(&tokens, 6) {
                row_type = RowType::CreateTable {
                    database_name: database_name.to_string(),
                    table_name: table_name.to_string(),
                };
            }
        }
    }

    if is_alter_table_statement(&tokens) {
        let database_name_pos = match get_word_value_at_position(&tokens, 4) {
            Some(word) if word == "ONLY" => 6,
            _ => 4,
        };

        let table_name_pos = match get_word_value_at_position(&tokens, 4) {
            Some(word) if word == "ONLY" => 8,
            _ => 6,
        };

        if let Some(database_name) = get_word_value_at_position(&tokens, database_name_pos) {
            if let Some(table_name) = get_word_value_at_position(&tokens, table_name_pos) {
                row_type = RowType::AlterTable {
                    database_name: database_name.to_string(),
                    table_name: table_name.to_string(),
                };
            }
        }
    }

    row_type
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
    use crate::source::SourceOptions;
    use crate::transformer::{transient::TransientTransformer, Transformer};
    use crate::Source;

    use super::Mysql;

    fn get_mysql() -> Mysql<'static> {
        Mysql::new("localhost", 3306, "db", "root", "password")
    }

    fn get_invalid_mysql() -> Mysql<'static> {
        Mysql::new("localhost", 3306, "db", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_mysql();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        let source_options = SourceOptions {
            transformers: &transformers,
            skip_config: &vec![],
            database_subset: &None,
        };
        assert!(p.read(source_options, |_original_query, _query| {}).is_ok());

        let p = get_invalid_mysql();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        let source_options = SourceOptions {
            transformers: &transformers,
            skip_config: &vec![],
            database_subset: &None,
        };
        assert!(p
            .read(source_options, |_original_query, _query| {})
            .is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_mysql();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        let source_options = SourceOptions {
            transformers: &transformers,
            skip_config: &vec![],
            database_subset: &None,
        };
        let _ = p.read(source_options, |original_query, query| {
            assert!(original_query.data().len() > 0);
            assert!(query.data().len() > 0);
        });
    }
}
