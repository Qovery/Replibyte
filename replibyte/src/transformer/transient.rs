use crate::transformer::Transformer;
use crate::types::Column;

/// This transformer will not make any changes.
pub struct TransientTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl Default for TransientTransformer {
    fn default() -> Self {
        TransientTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl TransientTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        TransientTransformer {
            table_name: table_name.into(),
            column_name: column_name.into(),
            database_name: database_name.into(),
        }
    }
}

impl Transformer for TransientTransformer {
    fn id(&self) -> &str {
        "transient"
    }

    fn description(&self) -> &str {
        "Does not modify the value."
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
        column
    }
}
