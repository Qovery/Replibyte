use crate::types::Column;

pub mod email;
pub mod first_name;
pub mod keep_first_char;
pub mod phone_number;
pub mod random;
pub mod transient;

pub enum Transformers {
    Random,
    Transient,
    FirstName,
    Email,
    KeepFirstChar,
    PhoneNumber,
}

/// Trait to implement to create a custom Transformer.
pub trait Transformer {
    fn id(&self) -> &str;
    fn description(&self) -> &str;
    fn database_name(&self) -> &str;
    fn table_name(&self) -> &str;
    fn column_name(&self) -> &str;
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
