use crate::transformer::Transformer;
use crate::types::Column;
use fake::faker::internet::raw::SafeEmail;
use fake::locales::EN;
use fake::Fake;

/// This struct is dedicated to replacing a string by an email address.
pub struct EmailTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl EmailTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        EmailTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Default for EmailTransformer {
    fn default() -> Self {
        EmailTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl Transformer for EmailTransformer {
    fn id(&self) -> &str {
        "email"
    }

    fn description(&self) -> &str {
        "Generate an email address (string only). [john.doe@company.com]->[tony.stark@avengers.com]"
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
                    len if len == 0 => value,
                    _ => SafeEmail(EN).fake(),
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

    use super::EmailTransformer;

    #[test]
    fn transform_email_with_number_value() {
        let expected_value = 34;
        let transformer = get_transformer();
        let column = Column::NumberValue("email".to_string(), expected_value);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();

        assert_eq!(transformed_value.to_owned(), expected_value)
    }

    #[test]
    fn transform_email_with_float_value() {
        let expected_value = 1.5;
        let transformer = get_transformer();
        let column = Column::FloatNumberValue("email".to_string(), expected_value);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.float_number_value().unwrap();

        assert_eq!(transformed_value.to_owned(), expected_value)
    }

    #[test]
    fn transform_email_with_empty_string_value() {
        let expected_value = "";
        let transformer = get_transformer();
        let column = Column::StringValue("email".to_string(), expected_value.to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert_eq!(transformed_value, expected_value)
    }

    #[test]
    fn transform_email_with_string_value() {
        let transformer = get_transformer();
        let column = Column::StringValue("email".to_string(), "john.doe@company.com".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert!(!transformed_value.is_empty());
        assert_ne!(transformed_value, "john.doe@company.com".to_string());
    }

    fn get_transformer() -> EmailTransformer {
        EmailTransformer::new("github", "users", "email")
    }
}
