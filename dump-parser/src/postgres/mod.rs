use crate::postgres::tokenizer::{
    match_keyword_at_position, Keyword, Token, Tokenizer, TokenizerError, Word,
};
use crate::utils::list_queries_from_dump;
use crate::{Database, DumpFileError, LogicalDatabase, Type};
use std::collections::HashSet;

mod tokenizer;

#[derive(Debug, Hash, Eq, PartialEq)]
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

    fn databases(&self) -> Result<HashSet<LogicalDatabase<Postgres<'a>>>, DumpFileError> {
        let mut results = HashSet::new();
        let _ = list_queries_from_dump(self.dump_file_path, |query| {
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

            if match_keyword_at_position(Keyword::Create, &tokens, 0)
                && match_keyword_at_position(Keyword::Table, &tokens, 2)
            {
                if let Some(fifth_token) = tokens.get(4) {
                    match fifth_token {
                        Token::Word(word) => {
                            results.insert(LogicalDatabase::new(word.value.clone(), self));
                        }
                        _ => {}
                    }
                }
            }

            tokens;
        })?;

        Ok(results)
    }
}
