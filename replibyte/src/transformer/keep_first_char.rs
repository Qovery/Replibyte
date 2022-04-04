use crate::transformer::Transformer;
use crate::types::{Column, NumberValue};

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
                let first_digit = match value {
                    NumberValue::I32(i) => {
                        NumberValue::I32(get_first_digit_signed(i as i128) as i32)
                    }
                    NumberValue::I64(i) => {
                        NumberValue::I64(get_first_digit_signed(i as i128) as i64)
                    }
                    NumberValue::I128(i) => NumberValue::I128(get_first_digit_signed(i)),
                    NumberValue::U32(u) => {
                        NumberValue::U32(get_first_digit_unsigned(u as u128) as u32)
                    }
                    NumberValue::U64(u) => {
                        NumberValue::U64(get_first_digit_unsigned(u as u128) as u64)
                    }
                    NumberValue::U128(u) => NumberValue::U128(get_first_digit_unsigned(u)),
                };
                Column::NumberValue(column_name, first_digit)
            }
            Column::StringValue(column_name, value) => {
                let new_value = match value.len() {
                    len if len > 1 => {
                        if let Some(first_char) = value.chars().nth(0) {
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

fn get_first_digit_signed(mut number: i128) -> i128 {
    while number >= 10 {
        number /= 10;
    }

    number
}

fn get_first_digit_unsigned(mut number: u128) -> u128 {
    while number >= 10 {
        number /= 10;
    }

    number
}

#[cfg(test)]
mod tests {
    use crate::{
        transformer::Transformer,
        types::{Column, FloatNumberValue, NumberValue},
    };

    use super::KeepFirstCharTransformer;

    #[test]
    fn transform_keep_first_char_only_with_number_value() {
        let transformer = get_transformer();
        let column = Column::NumberValue("a_column".to_string(), NumberValue::I32(123));
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();
        assert_eq!(transformed_value.to_owned(), NumberValue::I32(1));

        let transformer = get_transformer();
        let column = Column::NumberValue("a_column".to_string(), NumberValue::I32(1));
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();
        assert_eq!(transformed_value.to_owned(), NumberValue::I32(1));
    }

    #[test]
    fn transform_doesnt_change_with_float_value() {
        let expected_value = FloatNumberValue::F64(1.5);
        let transformer = get_transformer();
        let column = Column::FloatNumberValue("a_column".to_string(), expected_value.clone());
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
