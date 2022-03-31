use serde::{Deserialize, Serialize};

use crate::transformer::Transformer;
use crate::types::Column;

/// This struct is dedicated to redact a string with a specific character (default to '*').
pub struct RedactedTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    options: RedactedTransformerOptions,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub struct RedactedTransformerOptions {
    pub character: char,
    pub width: u8,
}

impl Default for RedactedTransformerOptions {
    fn default() -> Self {
        RedactedTransformerOptions {
            character: '*',
            width: 10,
        }
    }
}

impl RedactedTransformer {
    pub fn new<S>(
        database_name: S,
        table_name: S,
        column_name: S,
        options: RedactedTransformerOptions,
    ) -> Self
    where
        S: Into<String>,
    {
        RedactedTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
            options,
        }
    }
}

impl Default for RedactedTransformer {
    fn default() -> Self {
        RedactedTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
            options: RedactedTransformerOptions::default(),
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
                        format!(
                            "{}{}",
                            &value[0..3],
                            self.options
                                .character
                                .to_string()
                                .repeat(self.options.width.into())
                        )
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

    use super::{RedactedTransformer, RedactedTransformerOptions};

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

    #[test]
    fn strings_lower_than_3_chars_remains_visible() {
        let transformer = get_transformer();
        let column = Column::StringValue("credit_card_number".to_string(), "424".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value.to_owned(), "424")
    }

    #[test]
    fn redact_with_custom_char() {
        let transformer = RedactedTransformer::new(
            "github",
            "users",
            "credit_card_number",
            RedactedTransformerOptions {
                character: '#',
                width: 20,
            },
        );
        let column = Column::StringValue(
            "credit_card_number".to_string(),
            "4242 4242 4242 4242".to_string(),
        );
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value.to_owned(), "424####################")
    }

    fn get_transformer() -> RedactedTransformer {
        RedactedTransformer::new(
            "github",
            "users",
            "credit_card_number",
            RedactedTransformerOptions::default(),
        )
    }
}
