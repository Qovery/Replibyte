# Dump Parser

Library to parse and edit database dump for Postgres, MySQL and MongoDB.


Example for Postgres
```rust
let q = r"
INSERT INTO public.customers (customer_id, company_name, contact_name, contact_title)
VALUES (1, 'Alfreds Futterkiste', 'Maria Anders', NULL);
";

let mut tokenizer = Tokenizer::new(q);
let tokens_result = tokenizer.tokenize();
assert_eq!(tokens_result.is_ok(), true);

let tokens = trim_pre_whitespaces(tokens_result.unwrap());
let column_values = get_column_values_from_insert_into_query(&tokens);

assert_eq!(
    column_values,
    vec![
        &Token::Number("1".to_string(), false),
        &Token::SingleQuotedString("Alfreds Futterkiste".to_string()),
        &Token::SingleQuotedString("Maria Anders".to_string()),
        &Token::make_keyword("NULL"),
    ]
);
```
