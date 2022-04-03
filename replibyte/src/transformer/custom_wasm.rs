use crate::transformer::Transformer;
use crate::types::Column;
use wasmer_runtime::{func, imports, instantiate, Ctx, Instance, Value};

/// This struct is dedicated to replacing a credit card string.
pub struct CustomWasmTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    instance: Option<Instance>,
}

impl CustomWasmTransformer {
    pub fn new<S>(database_name: S, table_name: S, column_name: S, wasm_bytes: Vec<u8>) -> Self
    where
        S: Into<String>,
    {
        let import_object = imports! {};
        // Compile our webassembly into an `Instance`.
        let mut instance = instantiate(&wasm_bytes, &import_object).unwrap();
        CustomWasmTransformer {
            database_name: database_name.into(),
            table_name: table_name.into(),
            column_name: column_name.into(),
            instance: Some(instance),
        }
    }
    fn custom_wasm_transform_i64(&self, value: i64) -> i64 {
        // Call our exported function!
        if let Some(instance) = &self.instance {
            match instance.call("transform_i64", &[Value::I64(value)]) {
                Ok(result) => result[0].to_u128() as i64,
                Err(err) => {
                    println!("Error: {:?}", err);
                    value
                }
            }
        } else {
            value
        }
    }
}

impl Default for CustomWasmTransformer {
    fn default() -> Self {
        CustomWasmTransformer {
            database_name: String::default(),
            table_name: String::default(),
            column_name: String::default(),
            instance: Option::default(),
        }
    }
}

impl Transformer for CustomWasmTransformer {
    fn id(&self) -> &str {
        "custom-wasm"
    }

    fn description(&self) -> &str {
        "Provide a custom transformation function in a Wasm module (string or number (i32/i64, u32/u64, f32/f64) )."
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
                todo!()
            }
            Column::NumberValue(column_name, value) => Column::NumberValue(
                column_name,
                self.custom_wasm_transform_i64(value as i64) as i128,
            ),
            Column::FloatNumberValue(column_name, value) => {
                // Column::StringValue(column_name, custom_wasm_transform_f64(value))
                todo!()
            }
            column => column,
        }
    }
}

#[cfg(test)]
mod tests {
    use wasmer::wat2wasm;

    use crate::{transformer::Transformer, types::Column};

    use super::CustomWasmTransformer;

    fn get_transformer() -> CustomWasmTransformer {
        CustomWasmTransformer::new(
            "test",
            "users",
            "number",
            wat2wasm(
                br#"
                (module
                    (type $add_one_t (func (param i64) (result i64)))
                    (func $add_one_f (type $add_one_t) (param $value i64) (result i64)
                    local.get $value
                    i64.const 1
                    i64.add)
                    (export "transform_i64" (func $add_one_f)))
                "#,
            )
            .unwrap()
            .to_vec(),
        )
    }

    #[test]
    fn transform_i64_add_one() {
        let transformer = get_transformer();
        let column = Column::NumberValue("number".to_string(), 1);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();

        assert_eq!(transformed_value, &(2 as i128));
    }
}
