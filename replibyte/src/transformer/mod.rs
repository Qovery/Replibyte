use crate::transformer::credit_card::CreditCardTransformer;
use crate::transformer::email::EmailTransformer;
use crate::transformer::first_name::FirstNameTransformer;
use crate::transformer::keep_first_char::KeepFirstCharTransformer;
use crate::transformer::phone_number::PhoneNumberTransformer;
use crate::transformer::random::RandomTransformer;
use crate::transformer::redacted::RedactedTransformer;
use crate::transformer::transient::TransientTransformer;
use crate::types::Column;

pub mod credit_card;
pub mod email;
pub mod first_name;
pub mod keep_first_char;
pub mod phone_number;
pub mod random;
pub mod redacted;
pub mod transient;

pub fn transformers() -> Vec<Box<dyn Transformer>> {
    vec![
        Box::new(EmailTransformer::default()),
        Box::new(FirstNameTransformer::default()),
        Box::new(PhoneNumberTransformer::default()),
        Box::new(RandomTransformer::default()),
        Box::new(KeepFirstCharTransformer::default()),
        Box::new(TransientTransformer::default()),
        Box::new(CreditCardTransformer::default()),
        Box::new(RedactedTransformer::default()),
    ]
}

/// Trait to implement to create a custom Transformer.
pub trait Transformer: Sync {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn database_name(&self) -> &str;
    fn table_name(&self) -> &str;
    fn column_name(&self) -> &str;
    fn database_and_table_name(&self) -> String {
        format!("{}.{}", self.database_name(), self.table_name())
    }
    fn database_and_table_and_column_name(&self) -> String {
        format!(
            "{}.{}.{}",
            self.database_name(),
            self.table_name(),
            self.column_name()
        )
    }
    fn transform(&self, column: Column) -> Column;
}
