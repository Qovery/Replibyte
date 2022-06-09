use crate::transformer::Transformer;
use crate::types::Column;

pub struct KeepFirstCharTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl KeepFirstCharTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        KeepFirstCharTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Default for KeepFirstCharTransformer {
    fn default() -> Self {
        KeepFirstCharTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl Transformer for KeepFirstCharTransformer {
    fn id(&self) -> &str {
        "keep-first-char"
    }

    fn description(&self) -> &str {
        "Keep only the first character of the column."
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

    fn database_and_table_and_column_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.database_name(),
            self.table_name(),
            self.column_name()
        )
    }

    fn transform(&self, column: Column) -> Column {
        match column {
            Column::NumberValue(column_name, value) => {
                Column::NumberValue(column_name, get_first_digit(value))
            }
            Column::StringValue(column_name, value) => {
                let new_value = match value.len() {
                    len if len > 1 => {
                        if let Some(first_char) = value.chars().next() {
                            first_char.to_string()
                        } else {
                            "".to_string()
                        }
                    }

                    _ => value,
                };

                Column::StringValue(column_name, new_value)
            }
            column => column,
        }
    }
}

fn get_first_digit(mut number: i128) -> i128 {
    while number >= 10 {
        number /= 10;
    }

    number
}

#[cfg(test)]
mod tests {
    use crate::{transformer::Transformer, types::Column};

    use super::KeepFirstCharTransformer;

    #[test]
    fn transform_keep_first_char_only_with_number_value() {
        let transformer = get_transformer();
        let column = Column::NumberValue("a_column".to_string(), 123);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();
        assert_eq!(transformed_value.to_owned(), 1);

        let transformer = get_transformer();
        let column = Column::NumberValue("a_column".to_string(), 1);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();
        assert_eq!(transformed_value.to_owned(), 1);
    }

    #[test]
    fn transform_doesnt_change_with_float_value() {
        let expected_value = 1.5;
        let transformer = get_transformer();
        let column = Column::FloatNumberValue("a_column".to_string(), expected_value);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.float_number_value().unwrap();

        assert_eq!(transformed_value.to_owned(), expected_value);
    }

    #[test]
    fn transform_doesnt_change_with_empty_string_value() {
        let expected_value = "";
        let transformer = get_transformer();
        let column = Column::StringValue("a_column".to_string(), expected_value.to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value, expected_value);
    }

    #[test]
    fn transform_keep_only_first_char_with_string_value() {
        let transformer = get_transformer();
        let column = Column::StringValue("a_column".to_string(), "Lucas".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value, "L".to_string());

        let column = Column::StringValue("a_column".to_string(), "L".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();
        assert_eq!(transformed_value, "L".to_string());
    }

    fn get_transformer() -> KeepFirstCharTransformer {
        KeepFirstCharTransformer::new("github", "users", "a_column")
    }
}
