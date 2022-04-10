use crate::postgres::SubsetStrategy::RandomPercent;
use crate::{utils, Bytes, Progress, Subset, SubsetTable, SubsetTableRelation};
use dump_parser::postgres::{
    get_column_names_from_insert_into_query, get_column_values_str_from_insert_into_query,
    get_tokens_from_query_str, get_word_value_at_position, match_keyword_at_position, Keyword,
    Token,
};
use dump_parser::utils::{list_queries_from_dump_reader, ListQueryResult};
use std::borrow::BorrowMut;
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, Error, ErrorKind, Read};
use std::ops::Index;
use std::path::Path;
use std::time::SystemTime;

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
    first_insert_into_row_index: usize,
    last_insert_into_row_index: usize,
}

pub enum SubsetStrategy<'a> {
    RandomPercent {
        database: &'a str,
        table: &'a str,
        percent: u8,
    },
}

impl<'a> SubsetStrategy<'a> {
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
    subset_strategy: SubsetStrategy<'a>,
}

impl<'a> Postgres<'a> {
    pub fn new<R: Read>(
        schema_reader: BufReader<R>,
        dump: &'a Path,
        subset_strategy: SubsetStrategy<'a>,
    ) -> Result<Self, Error> {
        Ok(Postgres {
            subset_table_by_database_and_table_name: get_subset_table_by_database_and_table_name(
                schema_reader,
            )?,
            dump,
            subset_strategy,
        })
    }

    fn dump_reader(&self) -> BufReader<File> {
        BufReader::new(File::open(self.dump).unwrap())
    }

    fn reference_rows(
        &self,
        table_stats: &HashMap<(Database, Table), TableStats>,
    ) -> (&str, &str, Vec<String>) {
        match self.subset_strategy {
            SubsetStrategy::RandomPercent {
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
        }
    }

    fn visits<F: FnMut(String)>(
        &self,
        row: String,
        visited_tables: &mut HashSet<(Database, Table)>,
        table_stats: &HashMap<(Database, Table), TableStats>,
        data: &mut F,
        last_to_visit: bool,
    ) -> Result<(), Error> {
        // FIXME unnecessary .as_bytes().to_vec()?
        data(row.clone());

        if last_to_visit {
            return Ok(());
        }

        // tokenize `INSERT INTO ...` row
        let row_tokens = get_tokens_from_query_str(row.as_str());

        // find the database and table names from this row
        let (row_database, row_table) =
            get_insert_into_database_and_table_name(&row_tokens).unwrap();

        let _ = visited_tables.insert((row_database.clone(), row_table.clone()));

        // find the subset table from this row
        let row_subset_table = self
            .subset_table_by_database_and_table_name
            .get(&(row_database.to_string(), row_table.to_string()))
            .unwrap();

        let row_column_names = get_column_names_from_insert_into_query(&row_tokens);
        let row_column_values = get_column_values_str_from_insert_into_query(&row_tokens);

        for row_relation in &row_subset_table.relations {
            let column = row_relation.from_property.as_str();
            // find the value from the current row for the relation column
            let column_idx = row_column_names.iter().position(|x| *x == column).unwrap(); // FIXME unwrap
            let value = row_column_values.get(column_idx).unwrap();

            let database_and_table_tuple =
                (row_relation.database.clone(), row_relation.table.clone());

            // find the table stats for this row
            let row_relation_table_stats = table_stats.get(&database_and_table_tuple).unwrap();
            let s_last_to_visit = visited_tables.contains(&database_and_table_tuple);

            // fetch data from the relational table
            filter_insert_into_rows(
                row_relation.to_property.as_str(),
                value.as_str(),
                self.dump_reader(),
                row_relation_table_stats,
                |row| match self.visits(
                    row.to_string(),
                    visited_tables,
                    table_stats,
                    data,
                    s_last_to_visit,
                ) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("{}", err);
                    }
                },
            );
        }

        Ok(())
    }
}

impl<'a> Subset for Postgres<'a> {
    /// Return every subset rows
    /// Algorithm used:
    /// 1. find the reference table and take the X rows from this table with the appropriate SubsetStrategy
    /// 2. iterate over each row and their relations (0 to N relations)
    /// 3. for each rows from each relations, filter on the id from the parent related row id. (equivalent `SELECT * FROM table_1 INNER JOIN ... WHERE table_1.id = 'xxx';`
    /// 4. do it recursively for table_1.relations[*].relations[*]... but the algo stops when reaching the end or reach a cyclic ref.
    ///
    /// Notes:
    /// a. the algo must visits all the tables, even the one that has no relations.
    fn data_rows<F: FnMut(String), P: FnMut(Progress)>(
        &self,
        mut data: F,
        mut progress: P,
    ) -> Result<(), Error> {
        let table_stats = table_stats_by_database_and_table_name(self.dump_reader());
        let (database, table, rows) = self.reference_rows(&table_stats);

        let mut visited_tables = HashSet::new();
        visited_tables.insert((database.to_string(), table.to_string()));

        let total_rows = rows.len();
        let mut processed_rows = 0usize;
        progress(Progress {
            total_rows,
            processed_rows,
            last_process_time: 0,
        });

        for row in rows {
            let start_time = utils::epoch_millis();
            let _ = self.visits(
                row,
                visited_tables.borrow_mut(),
                &table_stats,
                &mut data,
                false,
            )?;

            processed_rows += 1;

            progress(Progress {
                total_rows,
                processed_rows,
                last_process_time: utils::epoch_millis() - start_time,
            });
        }

        // TODO visit all the others tables

        Ok(())
    }
}

fn list_percent_of_insert_into_rows<R: Read>(
    percent: u8,
    table_stats: &TableStats,
    dump_reader: BufReader<R>,
) -> Vec<String> {
    let mut insert_into_rows = vec![];

    if percent == 0 || table_stats.total_rows == 0 {
        return insert_into_rows;
    }

    let percent = if percent > 100 { 100 } else { percent };

    let total_rows_to_pick = table_stats.total_rows as f32 * percent as f32 / 100.0;
    let modulo = (table_stats.total_rows as f32 / total_rows_to_pick) as usize;

    let mut counter = 1usize;
    list_insert_into_rows(dump_reader, table_stats, |rows| {
        if counter % modulo == 0 {
            insert_into_rows.push(rows.to_string());
        }

        counter += 1;
    });

    insert_into_rows
}

fn list_insert_into_rows<R: Read, F: FnMut(&str)>(
    dump_reader: BufReader<R>,
    table_stats: &TableStats,
    mut rows: F,
) {
    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);
        let tokens = trim_tokens(&tokens, Keyword::Insert);

        if match_keyword_at_position(Keyword::Insert, &tokens, 0)
            && match_keyword_at_position(Keyword::Into, &tokens, 2)
            && get_word_value_at_position(&tokens, 4) == Some(table_stats.database.as_str())
            && get_word_value_at_position(&tokens, 6) == Some(table_stats.table.as_str())
        {
            rows(query.as_ref());
        }

        ListQueryResult::Continue
    });
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
            ));
        }
    };

    let mut query_idx = 0usize;
    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let mut query_res = ListQueryResult::Continue;

        if query_idx >= table_stats.first_insert_into_row_index
            && query_idx <= table_stats.last_insert_into_row_index
        {
            let tokens = get_tokens_from_query_str(query);
            let tokens = trim_tokens(&tokens, Keyword::Insert);

            if match_keyword_at_position(Keyword::Insert, &tokens, 0)
                && match_keyword_at_position(Keyword::Into, &tokens, 2)
                && get_word_value_at_position(&tokens, 4) == Some(table_stats.database.as_str())
                && get_word_value_at_position(&tokens, 6) == Some(table_stats.table.as_str())
            {
                let column_values = get_column_values_str_from_insert_into_query(&tokens);

                if *column_values.index(column_idx) == value {
                    rows(query)
                }
            }
        }

        if query_idx > table_stats.last_insert_into_row_index {
            // early break to avoid parsing the dump while we have already parsed all the table rows
            query_res = ListQueryResult::Break;
        }

        query_idx += 1;
        query_res
    });

    Ok(())
}

fn table_stats_by_database_and_table_name<R: Read>(
    dump_reader: BufReader<R>,
) -> HashMap<(Database, Table), TableStats> {
    let mut table_stats_by_database_and_table_name =
        HashMap::<(Database, Table), TableStats>::new();

    let mut query_idx = 0usize;
    list_queries_from_dump_reader(dump_reader, COMMENT_CHARS, |query| {
        let tokens = get_tokens_from_query_str(query);

        let _ = match get_create_table_database_and_table_name(&tokens) {
            Some((database, table)) => {
                table_stats_by_database_and_table_name.insert(
                    (database.clone(), table.clone()),
                    TableStats {
                        database,
                        table,
                        columns: vec![],
                        total_rows: 0,
                        first_insert_into_row_index: 0,
                        last_insert_into_row_index: 0,
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

                            if table_stats.first_insert_into_row_index == 0 {
                                table_stats.first_insert_into_row_index = query_idx;
                            }

                            table_stats.last_insert_into_row_index = query_idx;
                            table_stats.total_rows += 1;
                        }
                        None => {
                            // should not happen because INSERT INTO must come after CREATE TABLE
                            panic!("Unexpected: INSERT INTO happened before CREATE TABLE while creating table_stats")
                        }
                    }
                }
            }
        }

        query_idx += 1;
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

        if let Some((database, table)) = get_create_table_database_and_table_name(&tokens) {
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

fn get_create_table_database_and_table_name(tokens: &Vec<Token>) -> Option<(Database, Table)> {
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

fn get_insert_into_database_and_table_name(tokens: &Vec<Token>) -> Option<(Database, Table)> {
    let tokens = trim_tokens(&tokens, Keyword::Insert);

    if tokens.is_empty() {
        return None;
    }

    if match_keyword_at_position(Keyword::Insert, &tokens, 0)
        && match_keyword_at_position(Keyword::Into, &tokens, 2)
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
        filter_insert_into_rows, get_alter_table_foreign_key,
        get_create_table_database_and_table_name, get_subset_table_by_database_and_table_name,
        list_percent_of_insert_into_rows, table_stats_by_database_and_table_name, ForeignKey,
        Postgres, SubsetStrategy,
    };
    use crate::Subset;
    use dump_parser::postgres::Tokenizer;
    use std::borrow::BorrowMut;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::rc::Rc;

    fn dump_path() -> PathBuf {
        Path::new("db")
            .join("postgres")
            .join("fulldump-with-inserts.sql")
    }

    fn schema_reader() -> BufReader<File> {
        BufReader::new(
            File::open(Path::new("db").join("postgres").join("fulldump-schema.sql")).unwrap(),
        )
    }

    fn dump_reader() -> BufReader<File> {
        BufReader::new(File::open(dump_path()).unwrap())
    }

    #[test]
    fn check_statements_with_tokens() {
        let q = "SELECT * FROM toto;";
        let tokens = Tokenizer::new(q).tokenize().unwrap();
        assert_eq!(get_create_table_database_and_table_name(&tokens), None);

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
            get_create_table_database_and_table_name(&tokens),
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
        let m = get_subset_table_by_database_and_table_name(schema_reader()).unwrap();
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

    #[test]
    fn check_postgres_subset() {
        let path = dump_path();

        let p = Postgres::new(
            schema_reader(),
            path.as_path(),
            SubsetStrategy::random("public", "orders", 50),
        )
        .unwrap();

        p.data_rows(
            |row| {
                assert!(!row.is_empty());
            },
            |progress| {
                //
                println!(
                    "database subset progression: {}% (last process time: {}ms)",
                    progress.percent(),
                    progress.last_process_time
                );
            },
        )
        .unwrap();

        // TODO check it is smaller than the full dump
    }
}
