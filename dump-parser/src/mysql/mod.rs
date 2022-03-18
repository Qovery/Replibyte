use pest;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "mysql/mysql.pest"]
pub struct SQLParser;

#[cfg(test)]
mod tests {
    use super::*;

    fn print_tokens(statement: &str) {
        let parsed = SQLParser::parse(Rule::file, statement)
            .expect("unsuccessful parse")
            .next()
            .expect("pest failure");

        parsed.tokens().for_each(|x| println!("{:?}", x));
    }

    #[test]
    fn create_table_statement() {
        print_tokens("CREATE TABLE public.orders ( order_id smallint NOT NULL );");
    }

    #[test]
    fn insert_into_statement() {
        print_tokens("INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title) VALUES (1, 'Alfreds Futterkiste', 'Maria Anders', NULL);");
    }
}
