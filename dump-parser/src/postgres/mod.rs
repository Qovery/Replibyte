use std::collections::HashSet;

use crate::postgres::tokenizer::{
    get_word_value_at_position, match_keyword_at_position, Keyword, Token, Tokenizer,
};
use crate::utils::list_queries_from_dump;
use crate::{Database, DumpFileError, FromDumpFile, LogicalDatabase, Row, Table, Type};

mod tokenizer;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct Postgres {
    dump_file_path: String,
}

impl<'a> Postgres {
    pub fn new<S: Into<String>>(dump_file_path: S) -> Self {
        Postgres {
            dump_file_path: dump_file_path.into(),
        }
    }
}

impl<'a> Database<'a, PostgresLogicalDatabase, PostgresTable> for Postgres {
    fn database_type(&self) -> Type {
        Type::Postgres
    }

    fn databases(&self) -> Result<HashSet<PostgresLogicalDatabase>, DumpFileError> {
        let mut results = HashSet::new();

        let _ = list_queries_from_dump(self.dump_file_path.as_str(), |query| {
            let tokens = get_sanitized_tokens_from_query(query);

            if match_keyword_at_position(Keyword::Create, &tokens, 0)
                && match_keyword_at_position(Keyword::Table, &tokens, 2)
            {
                if let Some(value) = get_word_value_at_position(&tokens, 4) {
                    // find database name by filtering out all queries starting with
                    // CREATE TABLE <database>.<table>
                    // CREATE       -> position 0
                    // TABLE        -> position 2
                    // <database>   -> position 4
                    results.insert(PostgresLogicalDatabase::new(
                        value.to_string(),
                        self.dump_file_path.clone(),
                    ));
                }
            }
        })?;

        Ok(results)
    }
}

impl FromDumpFile for Postgres {
    fn dump_file_path(&self) -> &str {
        self.dump_file_path.as_str()
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PostgresLogicalDatabase {
    name: String,
    dump_file_path: String,
}

impl<'a> PostgresLogicalDatabase {
    pub fn new(name: String, dump_file_path: String) -> Self {
        PostgresLogicalDatabase {
            name,
            dump_file_path,
        }
    }
}

impl<'a> LogicalDatabase<'a, PostgresTable> for PostgresLogicalDatabase {
    fn name(&self) -> &str {
        self.name.as_str()
    }

    fn tables(&self) -> Result<Vec<PostgresTable>, DumpFileError> {
        let mut results = vec![];

        let _ = list_queries_from_dump(self.dump_file_path.as_str(), |query| {
            let tokens = get_sanitized_tokens_from_query(query);

            if match_keyword_at_position(Keyword::Create, &tokens, 0)
                && match_keyword_at_position(Keyword::Table, &tokens, 2)
            {
                if let Some(value) = get_word_value_at_position(&tokens, 6) {
                    // find database name by filtering out all queries starting with
                    // CREATE TABLE <database>.<table>
                    // CREATE       -> position 0
                    // TABLE        -> position 2
                    // <table>      -> position 6
                    results.push(PostgresTable::new(
                        value.to_string(),
                        self.dump_file_path.clone(),
                    ));
                }
            }
        })?;

        Ok(results)
    }
}

impl FromDumpFile for PostgresLogicalDatabase {
    fn dump_file_path(&self) -> &str {
        self.dump_file_path.as_str()
    }
}

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct PostgresTable {
    name: String,
    dump_file_path: String,
}

impl PostgresTable {
    pub fn new(name: String, dump_file_path: String) -> Self {
        PostgresTable {
            name,
            dump_file_path,
        }
    }
}

impl Table for PostgresTable {
    fn rows(&self) -> &'static Vec<Row> {
        todo!()
    }
}

impl FromDumpFile for PostgresTable {
    fn dump_file_path(&self) -> &str {
        self.dump_file_path.as_str()
    }
}

fn get_sanitized_tokens_from_query(query: &str) -> Vec<Token> {
    // query by query
    let mut tokenizer = Tokenizer::new(query);

    let tokens = match tokenizer.tokenize() {
        Ok(tokens) => tokens,
        Err(err) => panic!("{:?}", err),
    };

    let tokens = tokens
        .into_iter()
        .skip_while(|token| match token {
            // remove whitespaces (and comments) at the beginning of a vec of tokens
            Token::Whitespace(_) => true,
            _ => false,
        })
        .collect::<Vec<_>>();

    tokens
}
