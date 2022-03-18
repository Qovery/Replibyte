use crate::transformer::Transformer;
use crate::types::Column;
use fake::faker::name::raw::FirstName;
use fake::locales::EN;
use fake::Fake;

/// This struct is dedicated to replacing string by a first name.
pub struct FirstNameTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl FirstNameTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        FirstNameTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Transformer for FirstNameTransformer {
    fn id(&self) -> &str {
        "first_name"
    }

    fn description(&self) -> &str {
        "Generate a first name (string only). [Lucas]->[Georges]"
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
            Column::NumberValue(column_name, value) => Column::NumberValue(column_name, value),
            Column::FloatNumberValue(column_name, value) => {
                Column::FloatNumberValue(column_name, value)
            }
            Column::StringValue(column_name, value) => {
                let new_value = if value == "" {
                    "".to_string()
                } else {
                    FirstName(EN).fake()
                };

                Column::StringValue(column_name, new_value)
            }
            Column::CharValue(column_name, value) => Column::CharValue(column_name, value),
            Column::None(column_name) => Column::None(column_name),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer::Transformer, types::Column};

    use super::FirstNameTransformer;

    #[test]
    fn transform_first_name_with_number_value() {
        let expected_value = 34;
        let transformer = get_transformer();
        let column = Column::NumberValue("first_name".to_string(), expected_value);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();

        assert_eq!(transformed_value.to_owned(), expected_value)
    }

    #[test]
    fn transform_first_name_with_float_value() {
        let expected_value = 1.5;
        let transformer = get_transformer();
        let column = Column::FloatNumberValue("first_name".to_string(), expected_value);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.float_number_value().unwrap();

        assert_eq!(transformed_value.to_owned(), expected_value)
    }

    #[test]
    fn transform_first_name_with_empty_string_value() {
        let expected_value = "";
        let transformer = get_transformer();
        let column = Column::StringValue("first_name".to_string(), expected_value.to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert_eq!(transformed_value, expected_value)
    }

    #[test]
    fn transform_first_name_with_string_value() {
        let transformer = get_transformer();
        let column = Column::StringValue("first_name".to_string(), "Lucas".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert!(!transformed_value.is_empty());
        assert_ne!(transformed_value, "Lucas".to_string());
    }

    fn get_transformer() -> FirstNameTransformer {
        FirstNameTransformer::new("github", "users", "first_name")
    }
}
