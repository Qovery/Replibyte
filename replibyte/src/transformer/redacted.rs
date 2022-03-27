use crate::transformer::Transformer;
use crate::types::Column;

/// This struct is dedicated to redact a string with a specific character (default to '*').
pub struct RedactedTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl RedactedTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        RedactedTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Default for RedactedTransformer {
    fn default() -> Self {
        RedactedTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl Transformer for RedactedTransformer {
    fn id(&self) -> &str {
        "redacted"
    }

    fn description(&self) -> &str {
        "Obfuscate your sensitive data (string only). [4242 4242 4242 4242]->[424****************]"
    }

    fn database_name(&self) -> &str {
        self.database_name.as_str()
    }

    fn table_name(&self) -> &str {
        self.table_name.as_str()
    }

    fn column_name(&self) -> &str {
        self.column_name.as_str()
    }

    fn transform(&self, column: Column) -> Column {
        match column {
            Column::StringValue(column_name, value) => {
                let new_value = match value.len() {
                    len if len > 3 => {
                        format!("{}{:*<10}", &value[0..3], "*")
                    }
                    _ => value,
                };
                Column::StringValue(column_name, new_value)
            }
            column => column,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer::Transformer, types::Column};

    use super::RedactedTransformer;

    #[test]
    fn redact() {
        let transformer = get_transformer();
        let column = Column::StringValue(
            "credit_card_number".to_string(),
            "4242 4242 4242 4242".to_string(),
        );
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value.to_owned(), "424**********")
    }

    fn get_transformer() -> RedactedTransformer {
        RedactedTransformer::new("github", "users", "credit_card_number")
    }
}
