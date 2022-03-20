use pest;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "mysql/mysql.pest"]
pub struct SQLParser;

#[cfg(test)]
mod tests_mysql {
    macro_rules! test_tokenize_statement {
        ( $( $name:ident : $s:literal),* ) => {
            $(
                mod $name {
                    #[test]
                    fn test_tokenize_statement() {
                        use crate::mysql::*;

                        let parsed = SQLParser::parse(Rule::file, $s)
                            .expect("unsuccessful parse")
                            .next()
                            .expect("pest failure");

                        parsed.tokens().for_each(|x| println!("{:?}", x));
                    }
                }
            )*
        }
    }

    test_tokenize_statement! {
        create_table: "CREATE TABLE public.orders ( order_id smallint NOT NULL );",
        insert_into: "INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title) VALUES (1, 'Alfreds Futterkiste', 'Maria Anders', NULL);",
        select_star: "SELECT * FROM departments;",
        create_database: "CREATE DATABASE mysql;",
        backtick: "CREATE DATABASE `mysql`;",
        use_statement: "USE `mysql`;",
        drop_table: "DROP TABLE IF EXISTS `columnspriv`;"
    }
}
// TODO dump chinook
// TODO stream test case directly from dump file
