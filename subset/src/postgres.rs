use crate::postgres::SubsetQuery::RandomPercent;
use crate::{Subset, SubsetTable, SubsetTableRelation};
use dump_parser::postgres::{
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::list_queries_from_dump_reader;
use petgraph::{algo, Graph};
use std::collections::HashMap;
use std::io::{BufReader, Read};

type Database = String;
type Table = String;

#[derive(Debug)]
struct ForeignKey {
    from_database: String,
    from_table: String,
    from_property: String,
    to_database: String,
    to_table: String,
    to_property: String,
}

pub enum SubsetQuery<'a> {
    RandomPercent {
        database: &'a str,
        table: &'a str,
        percent: u8,
    },
}

impl<'a> SubsetQuery<'a> {
    pub fn random(database: &'a str, table: &'a str, percent: u8) -> Self {
        RandomPercent {
            database,
            table,
            percent,
        }
    }
}

struct Postgres<R: Read> {
    subset_table_by_database_and_table_name: HashMap<(Database, Table), SubsetTable>,
    dump_reader: BufReader<R>,
}

impl<R: Read> Postgres<R> {
    pub fn new(schema_reader: BufReader<R>, dump_reader: BufReader<R>) -> Self {
        let mut subset_table_by_database_and_table_name =
            HashMap::<(Database, Table), SubsetTable>::new();

        list_queries_from_dump_reader(schema_reader, "--", |query| {
            let tokens = get_tokens_from_query_str(query);

            if let Some((database, table)) = get_create_table(&tokens) {
                // add table into index
                let _ = subset_table_by_database_and_table_name.insert(
                    (database.clone(), table.clone()),
                    SubsetTable::new(database, table, vec![]),
                );
            }

            if let Some(fk) = get_alter_table_foreign_key(&tokens) {
                let _ = match subset_table_by_database_and_table_name
                    .get_mut(&(fk.from_database, fk.from_table))
                {
                    Some(subset_table) => {
                        subset_table.relations.push(SubsetTableRelation::new(
                            fk.to_database,
                            fk.to_table,
                            fk.from_property,
                            fk.to_property,
                        ));
                    }
                    None => {} // FIXME
                };
            }
        });

        Postgres {
            subset_table_by_database_and_table_name,
            dump_reader,
        }
    }
}

impl<R: Read> Subset for Postgres<R> {
    fn ordered_tables(&self) -> Vec<SubsetTable> {
        let mut g = Graph::<SubsetTable, ()>::new();

        let tarjan_scc_graph = algo::tarjan_scc(&g);

        for x in tarjan_scc_graph {
            for y in x {}
        }

        vec![]
    }

    fn rows(&self) {
        todo!()
    }
}

fn get_create_table(tokens: &Vec<Token>) -> Option<(Database, Table)> {
    let tokens = tokens
        .iter()
        .skip_while(|token| match *token {
            Token::Word(word) if word.keyword == Keyword::Create => false,
            _ => true,
        })
        .map(|token| token.clone())
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return None;
    }

    if match_keyword_at_position(Keyword::Create, &tokens, 0)
        && match_keyword_at_position(Keyword::Table, &tokens, 2)
    {
        if let Some(database) = get_word_value_at_position(&tokens, 4) {
            if let Some(table) = get_word_value_at_position(&tokens, 6) {
                return Some((database.to_string(), table.to_string()));
            }
        }
    }

    None
}

fn get_alter_table_foreign_key(tokens: &Vec<Token>) -> Option<ForeignKey> {
    let tokens = tokens
        .iter()
        .skip_while(|token| match *token {
            Token::Word(word) if word.keyword == Keyword::Alter => false,
            _ => true,
        })
        .map(|token| token.clone())
        .collect::<Vec<_>>();

    if tokens.is_empty() {
        return None;
    }

    if !match_keyword_at_position(Keyword::Alter, &tokens, 0)
        || !match_keyword_at_position(Keyword::Table, &tokens, 2)
    {
        return None;
    }

    let database_name_pos = if match_keyword_at_position(Keyword::Only, &tokens, 4) {
        6
    } else {
        4
    };

    let table_name_pos = if match_keyword_at_position(Keyword::Only, &tokens, 4) {
        8
    } else {
        6
    };

    let from_database_name = match get_word_value_at_position(&tokens, database_name_pos) {
        Some(database_name) => database_name,
        None => return None,
    };

    let from_table_name = match get_word_value_at_position(&tokens, table_name_pos) {
        Some(table_name) => table_name,
        None => return None,
    };

    let next_foreign_tokens = tokens
        .iter()
        .skip_while(|token| match token {
            Token::Word(word) if word.keyword == Keyword::Foreign => false,
            _ => true,
        })
        .map(|token| token.clone())
        .collect::<Vec<_>>();

    let from_property = match get_word_value_at_position(&next_foreign_tokens, 5) {
        Some(property) => property,
        None => return None,
    };

    let to_database_name = match get_word_value_at_position(&next_foreign_tokens, 10) {
        Some(database_name) => database_name,
        None => return None,
    };

    let to_table_name = match get_word_value_at_position(&next_foreign_tokens, 12) {
        Some(table_name) => table_name,
        None => return None,
    };

    let to_property = match get_word_value_at_position(&next_foreign_tokens, 14) {
        Some(property) => property,
        None => return None,
    };

    Some(ForeignKey {
        from_database: from_database_name.to_string(),
        from_table: from_table_name.to_string(),
        from_property: from_property.to_string(),
        to_database: to_database_name.to_string(),
        to_table: to_table_name.to_string(),
        to_property: to_property.to_string(),
    })
}

#[cfg(test)]
mod tests {
    use crate::postgres::{get_alter_table_foreign_key, get_create_table, ForeignKey};
    use dump_parser::postgres::Tokenizer;

    #[test]
    fn check_statements_with_tokens() {
        let q = "SELECT * FROM toto;";
        let tokens = Tokenizer::new(q).tokenize().unwrap();
        assert_eq!(get_create_table(&tokens), None);

        let q = r#"
CREATE TABLE public.order_details (
    order_id smallint NOT NULL,
    product_id smallint NOT NULL,
    unit_price real NOT NULL,
    quantity smallint NOT NULL,
    discount real NOT NULL
);"#;

        let tokens = Tokenizer::new(q).tokenize().unwrap();

        assert_eq!(
            get_create_table(&tokens),
            Some(("public".to_string(), "order_details".to_string()))
        );

        let q = r#"ALTER TABLE public.employees OWNER TO root;"#;
        let tokens = Tokenizer::new(q).tokenize().unwrap();
        assert!(get_alter_table_foreign_key(&tokens).is_none());

        let q = r#"
ALTER TABLE ONLY public.territories
    ADD CONSTRAINT fk_territories_region FOREIGN KEY (region_id) REFERENCES public.region(region_id);
"#;

        let tokens = Tokenizer::new(q).tokenize().unwrap();
        let fk = get_alter_table_foreign_key(&tokens).unwrap();
        assert_eq!(fk.from_database, "public".to_string());
        assert_eq!(fk.from_table, "territories".to_string());
        assert_eq!(fk.from_property, "region_id".to_string());
        assert_eq!(fk.to_database, "public".to_string());
        assert_eq!(fk.to_table, "region".to_string());
        assert_eq!(fk.to_property, "region_id".to_string());
    }
}
