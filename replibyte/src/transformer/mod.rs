use crate::types::Column;

pub mod random;
pub mod transient;

pub const TRANSFORMERS: [&str; 2] = ["transient", "random"];

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
