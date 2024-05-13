use crate::transformer::Transformer;
use crate::types::Column;
use fake::faker::creditcard::raw::CreditCardNumber;
use fake::locales::EN;
use fake::Fake;

/// This struct is dedicated to replacing a credit card string.
#[derive(Default)]
pub struct CreditCardTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl CreditCardTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        CreditCardTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}



impl Transformer for CreditCardTransformer {
    fn id(&self) -> &str {
        "credit-card"
    }

    fn description(&self) -> &str {
        "Generate a credit card number (string only)."
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
            Column::StringValue(column_name, _value) => {
                Column::StringValue(column_name, CreditCardNumber(EN).fake())
            }
            column => column,
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{transformer::Transformer, types::Column};

    use super::CreditCardTransformer;

    #[test]
    fn transform_string_with_a_credit_card() {
        let transformer = get_transformer();
        let column = Column::StringValue("credit_card".to_string(), "4242424242424242".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert!(!transformed_value.is_empty());
        assert_ne!(transformed_value, "4242424242424242".to_string());
    }

    fn get_transformer() -> CreditCardTransformer {
        CreditCardTransformer::new("github", "users", "credit_card")
    }
}
