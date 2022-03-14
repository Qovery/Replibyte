use crate::types::Column;
use rand::distributions::{Alphanumeric, Standard};
use rand::rngs::ThreadRng;
use rand::Rng;

pub trait Transformer {
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

/// make no transformation
pub struct NoTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl Default for NoTransformer {
    fn default() -> Self {
        NoTransformer {
            database_name: String::from("database_name"),
            table_name: String::from("no_table_name"),
            column_name: String::from("no_name"),
        }
    }
}

impl NoTransformer {
    pub fn new<S: Into<String>>(database_name: S, table_name: S, column_name: S) -> Self {
        NoTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Transformer for NoTransformer {
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

/// This transformer generate a random element
pub struct RandomTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl RandomTransformer {
    pub fn new<S: Into<String>>(database_name: S, table_name: S, column_name: S) -> Self {
        RandomTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
        }
    }
}

impl Transformer for RandomTransformer {
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
        let mut random = rand::thread_rng();

        match column {
            Column::NumberValue(column_name, _) => {
                Column::NumberValue(column_name, random.gen::<i128>())
            }
            Column::FloatNumberValue(column_name, _) => {
                Column::FloatNumberValue(column_name, random.gen::<f64>())
            }
            Column::StringValue(column_name, value) => {
                let new_value = random
                    .sample_iter(&Alphanumeric)
                    .take(value.len())
                    .map(char::from)
                    .collect::<String>();

                Column::StringValue(column_name, new_value)
            }
            Column::CharValue(column_name, _) => {
                Column::CharValue(column_name, random.gen::<char>())
            }
            Column::None(column_name) => Column::None(column_name),
        }
    }
}
