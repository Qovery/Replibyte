use crate::postgres::SubsetQuery::RandomPercent;
use crate::{Bytes, Subset, SubsetTable, SubsetTableRelation};
use dump_parser::postgres::{
    get_column_names_from_insert_into_query, get_column_values_from_insert_into_query,
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::{list_queries_from_dump_reader, ListQueryResult};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::ops::Index;
use std::path::Path;

const COMMENT_CHARS: &str = "--";

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

struct TableStats {
    database: String,
    table: String,
    columns: Vec<String>,
    total_rows: usize,
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

struct Postgres<'a> {
    subset_table_by_database_and_table_name: HashMap<(Database, Table), SubsetTable>,
    dump: &'a Path,
    ref_query: SubsetQuery<'a>,
}

impl<'a> Postgres<'a> {
    pub fn new<R: Read>(
        schema_reader: BufReader<R>,
        dump: &'a Path,
        ref_query: SubsetQuery<'a>,
    ) -> Result<Self, Error> {
        Ok(Postgres {
            subset_table_by_database_and_table_name: get_subset_table_by_database_and_table_name(
                schema_reader,
            )?,
            dump,
            ref_query,
        })
    }

    fn dump_reader(&self) -> BufReader<File> {
        BufReader::new(File::open(self.dump).unwrap())
    }
}

impl<'a> Subset for Postgres<'a> {
    fn data_rows<F: Fn(Bytes)>(&self, data: F) {
        let table_stats = table_stats_by_database_and_table_name(self.dump_reader());

        let (database, table, rows) = match self.ref_query {
            SubsetQuery::RandomPercent {
                database,
                table,
                percent,
            } => (
                database,
                table,
                list_percent_of_insert_into_rows(
                    percent,
                    table_stats
                        .get(&(database.to_string(), table.to_string()))
                        .unwrap(),
                    self.dump_reader(),
                ),
            ),
        };

        let ref_subset_table = self
            .subset_table_by_database_and_table_name
            .get(&(database.to_string(), table.to_string()))
            .unwrap(); // FIXME catch not found subset table

        let subset_tables = self
            .subset_table_by_database_and_table_name
            .values()
            .collect::<Vec<_>>();

        // ordered all subset tables from the subset table
        let related_tables = ref_subset_table.find_related_subset_tables(&subset_tables);

        // TODO pick percent rows from ref table
        // TODO for each ref row - iter over each related tables and filter over the to_property value
        // TODO for

        list_queries_from_dump_reader(self.dump_reader(), COMMENT_CHARS, |query| {
            // TODO

            ListQueryResult::Continue
        });

        // TODO
    }
}

fn list_percent_of_insert_into_rows<R: Read>(
    percent: u8,
    table_stats: &TableStats,
    dump_reader: BufReader<R>,
) -> Vec<String> {
    let mut insert_into_bytes = vec![];

    if percent == 0 {
        return insert_into_bytes;
    }

    let percent = if percent > 100 { 100 } else { percent };

    let total_rows_to_pick = table_stats.total_rows as f32 * percent as f32 / 100.0;
    let modulo = (table_stats.total_rows as f32 / total_rows_to_pick) as usize;

    let mut counter = 1usize;
    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);
        let tokens = trim_tokens(&tokens, Keyword::Insert);

        if match_keyword_at_position(Keyword::Insert, &tokens, 0)
            && match_keyword_at_position(Keyword::Into, &tokens, 2)
        {
            if counter % modulo == 0 {
                insert_into_bytes.push(query.to_string());
            }

            counter += 1;
        }

        ListQueryResult::Continue
    });

    insert_into_bytes
}

fn filter_insert_into_rows<R: Read, F: FnMut(&str)>(
    column: &str,
    value: &str,
    dump_reader: BufReader<R>,
    table_stats: &TableStats,
    mut rows: F,
) -> Result<(), Error> {
    let column_idx = match table_stats
        .columns
        .iter()
        .position(|r| r.as_str() == column)
    {
        Some(idx) => idx,
        None => {
            return Err(Error::new(
                ErrorKind::Other,
                format!(
                    "table {} does not contain column {}",
                    table_stats.table, column
                ),
            ))
        }
    };

    let mut total_visited_rows = 0usize;
    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let mut query_res = ListQueryResult::Continue;
        let tokens = get_tokens_from_query_str(query);
        let tokens = trim_tokens(&tokens, Keyword::Insert);

        if match_keyword_at_position(Keyword::Insert, &tokens, 0)
            && match_keyword_at_position(Keyword::Into, &tokens, 2)
            && get_word_value_at_position(&tokens, 4) == Some(table_stats.database.as_str())
            && get_word_value_at_position(&tokens, 6) == Some(table_stats.table.as_str())
        {
            let column_values = get_column_values_from_insert_into_query(&tokens)
                .iter()
                .filter_map(|x| match *x {
                    Token::Word(word) => Some(word.value.clone()),
                    Token::SingleQuotedString(word) => Some(word.clone()),
                    Token::Number(x, y) => Some(match y {
                        false => x.clone(),
                        true => format!("-{}", x),
                    }),
                    _ => None,
                })
                .collect::<Vec<_>>();

            if *column_values.index(column_idx) == value {
                rows(query)
            }

            if total_visited_rows > table_stats.total_rows {
                // early break to avoid parsing the dump while we have already parsed all the table rows
                query_res = ListQueryResult::Break;
            }

            total_visited_rows += 1;
        }

        query_res
    });

    Ok(())
}

fn table_stats_by_database_and_table_name<R: Read>(
    dump_reader: BufReader<R>,
) -> HashMap<(Database, Table), TableStats> {
    let mut table_stats_by_database_and_table_name =
        HashMap::<(Database, Table), TableStats>::new();

    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);

        let _ = match get_create_table(&tokens) {
            Some((database, table)) => {
                table_stats_by_database_and_table_name.insert(
                    (database.clone(), table.clone()),
                    TableStats {
                        database,
                        table,
                        columns: vec![],
                        total_rows: 0,
                    },
                );
            }
            None => {}
        };

        // remove potential whitespaces
        let tokens = trim_tokens(&tokens, Keyword::Insert);

        if match_keyword_at_position(Keyword::Insert, &tokens, 0)
            && match_keyword_at_position(Keyword::Into, &tokens, 2)
        {
            if let Some(database) = get_word_value_at_position(&tokens, 4) {
                if let Some(table) = get_word_value_at_position(&tokens, 6) {
                    match table_stats_by_database_and_table_name
                        .get_mut(&(database.to_string(), table.to_string()))
                    {
                        Some(table_stats) => {
                            if table_stats.total_rows == 0 {
                                // I assume that the INSERT INTO row has all the column set
                                let columns = get_column_names_from_insert_into_query(&tokens)
                                    .iter()
                                    .map(|name| name.to_string())
                                    .collect::<Vec<_>>();

                                table_stats.columns = columns;
                            }

                            table_stats.total_rows += 1;
                        }
                        None => {
                            // should not happen because INSERT INTO must come after CREATE TABLE
                        }
                    }
                }
            }
        }

        ListQueryResult::Continue
    });

    table_stats_by_database_and_table_name
}

fn trim_tokens(tokens: &Vec<Token>, keyword: Keyword) -> Vec<Token> {
    tokens
        .iter()
        .skip_while(|token| match *token {
            Token::Word(word) if word.keyword == keyword => false,
            _ => true,
        })
        .map(|token| token.clone()) // FIXME - do not clone token
        .collect::<Vec<_>>()
}

fn get_subset_table_by_database_and_table_name<R: Read>(
    schema_reader: BufReader<R>,
) -> Result<HashMap<(Database, Table), SubsetTable>, Error> {
    let mut subset_table_by_database_and_table_name =
        HashMap::<(Database, Table), SubsetTable>::new();

    list_queries_from_dump_reader(schema_reader, COMMENT_CHARS, |query| {
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

        ListQueryResult::Continue
    })?;

    Ok(subset_table_by_database_and_table_name)
}

fn get_create_table(tokens: &Vec<Token>) -> Option<(Database, Table)> {
    let tokens = trim_tokens(&tokens, Keyword::Create);

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
    let tokens = trim_tokens(&tokens, Keyword::Alter);

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
    use crate::postgres::{
        filter_insert_into_rows, get_alter_table_foreign_key, get_create_table,
        get_subset_table_by_database_and_table_name, list_percent_of_insert_into_rows,
        table_stats_by_database_and_table_name, ForeignKey,
    };
    use dump_parser::postgres::Tokenizer;
    use std::fs::File;
    use std::io::{BufReader, Read};
    use std::path::Path;

    const SCHEMA: &str = r#"
--
-- Name: customer_customer_demo; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customer_customer_demo (
    customer_id bpchar NOT NULL,
    customer_type_id bpchar NOT NULL
);


--
-- Name: customer_demographics; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customer_demographics (
    customer_type_id bpchar NOT NULL,
    customer_desc text
);


--
-- Name: customers; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.customers (
    customer_id bpchar NOT NULL,
    company_name character varying(40) NOT NULL,
    contact_name character varying(30),
    contact_title character varying(30),
    address character varying(60),
    city character varying(15),
    region character varying(15),
    postal_code character varying(10),
    country character varying(15),
    phone character varying(24),
    fax character varying(24)
);

--
-- Name: customer_customer_demo pk_customer_customer_demo; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_customer_demo
    ADD CONSTRAINT pk_customer_customer_demo PRIMARY KEY (customer_id, customer_type_id);


--
-- Name: customer_demographics pk_customer_demographics; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_demographics
    ADD CONSTRAINT pk_customer_demographics PRIMARY KEY (customer_type_id);


--
-- Name: customers pk_customers; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customers
    ADD CONSTRAINT pk_customers PRIMARY KEY (customer_id);

--
-- Name: customer_customer_demo fk_customer_customer_demo_customer_demographics; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customer_demographics FOREIGN KEY (customer_type_id) REFERENCES public.customer_demographics(customer_type_id);


--
-- Name: customer_customer_demo fk_customer_customer_demo_customers; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.customer_customer_demo
    ADD CONSTRAINT fk_customer_customer_demo_customers FOREIGN KEY (customer_id) REFERENCES public.customers(customer_id);
        "#;

    fn dump_reader() -> BufReader<File> {
        BufReader::new(
            File::open(
                Path::new("db")
                    .join("postgres")
                    .join("fulldump-with-inserts.sql"),
            )
            .unwrap(),
        )
    }

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

    #[test]
    fn check_subset_table() {
        let schema_reader = BufReader::new(SCHEMA.as_bytes());
        let m = get_subset_table_by_database_and_table_name(schema_reader).unwrap();
        assert!(m.len() > 0);

        let t = m
            .get(&("public".to_string(), "customer_demographics".to_string()))
            .unwrap();

        assert_eq!(t.database, "public".to_string());
        assert_eq!(t.table, "customer_demographics".to_string());
        assert_eq!(t.relations.len(), 0);

        let t = m
            .get(&("public".to_string(), "customer_customer_demo".to_string()))
            .unwrap();

        assert_eq!(t.database, "public".to_string());
        assert_eq!(t.table, "customer_customer_demo".to_string());
        assert_eq!(t.relations.len(), 2);
        assert_eq!(t.related_tables().len(), 2);

        let t = m
            .get(&("public".to_string(), "customers".to_string()))
            .unwrap();

        assert_eq!(t.database, "public".to_string());
        assert_eq!(t.table, "customers".to_string());
        assert_eq!(t.relations.len(), 0);
    }

    #[test]
    fn check_table_stats() {
        let table_stats = table_stats_by_database_and_table_name(dump_reader());
        assert!(table_stats.len() > 0);
        // TODO add more tests to check table.rows size
    }

    #[test]
    fn check_percent_of_rows() {
        let table_stats = table_stats_by_database_and_table_name(dump_reader());
        let first_table_stats = table_stats
            .get(&("public".to_string(), "order_details".to_string()))
            .unwrap();

        let rows = list_percent_of_insert_into_rows(5, first_table_stats, dump_reader());

        assert!(rows.len() < first_table_stats.total_rows)
    }

    #[test]
    fn check_filter_insert_into_rows() {
        let table_stats = table_stats_by_database_and_table_name(dump_reader());
        let first_table_stats = table_stats
            .get(&("public".to_string(), "order_details".to_string()))
            .unwrap();

        let mut found_rows = vec![];
        filter_insert_into_rows(
            "product_id",
            "11",
            dump_reader(),
            first_table_stats,
            |row| {
                found_rows.push(row.to_string());
            },
        )
        .unwrap();

        assert_eq!(found_rows.len(), 38)
    }
}
