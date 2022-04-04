use crate::transformer::Transformer;
use crate::types::{Column, FloatNumberValue, NumberValue};
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
            Column::NumberValue(column_name, value) => {
                let random_value: NumberValue = match value {
                    NumberValue::I32(_) => NumberValue::I32(random.gen::<i32>()),
                    NumberValue::I64(_) => NumberValue::I64(random.gen::<i64>()),
                    NumberValue::I128(_) => NumberValue::I128(random.gen::<i128>()),
                    NumberValue::U32(_) => NumberValue::U32(random.gen::<u32>()),
                    NumberValue::U64(_) => NumberValue::U64(random.gen::<u64>()),
                    NumberValue::U128(_) => NumberValue::U128(random.gen::<u128>()),
                };
                Column::NumberValue(column_name, random_value)
            }
            Column::FloatNumberValue(column_name, value) => {
                let random_value: FloatNumberValue = match value {
                    FloatNumberValue::F32(_) => FloatNumberValue::F32(random.gen::<f32>()),
                    FloatNumberValue::F64(_) => FloatNumberValue::F64(random.gen::<f64>()),
                };
                Column::FloatNumberValue(column_name, random_value)
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
