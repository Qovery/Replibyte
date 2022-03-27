use crate::transformer::Transformer;
use crate::types::Column;
use rand::distributions::Alphanumeric;
use rand::Rng;

/// This struct is dedicated to generating random elements.
pub struct RandomTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
}

impl RandomTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S) -> Self
    where
        S: Into<String>,
    {
        RandomTransformer {
            table_name: table_name.into(),
            column_name: column_name.into(),
            database_name: database_name.into(),
        }
    }
}

impl Default for RandomTransformer {
    fn default() -> Self {
        RandomTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
        }
    }
}

impl Transformer for RandomTransformer {
    fn id(&self) -> &str {
        "random"
    }

    fn description(&self) -> &str {
        "Randomize value but keep the same length (string only). [AAA]->[BBB]"
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
