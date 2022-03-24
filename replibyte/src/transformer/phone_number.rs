use crate::transformer::Transformer;
use crate::types::Column;
use fake::faker::phone_number::raw::PhoneNumber;
use fake::locales::EN;
use fake::Fake;

/// This struct is dedicated to replacing a string by an email address.
pub struct PhoneNumberTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl PhoneNumberTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        PhoneNumberTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Transformer for PhoneNumberTransformer {
    fn id(&self) -> &str {
        "phone-number"
    }

    fn description(&self) -> &str {
        "Generate a phone number (string only)."
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
                Column::StringValue(column_name, PhoneNumber(EN).fake())
            }
            column => column,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer::Transformer, types::Column};

    use super::PhoneNumberTransformer;

    #[test]
    fn transform_string_with_a_phone_number() {
        let transformer = get_transformer();
        let column = Column::StringValue("phone_number".to_string(), "+123456789".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert!(!transformed_value.is_empty());
        assert_ne!(transformed_value, "+123456789".to_string());
    }

    fn get_transformer() -> PhoneNumberTransformer {
        PhoneNumberTransformer::new("github", "users", "phone_number")
    }
}
