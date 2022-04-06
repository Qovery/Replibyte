use crate::transformer::Transformer;
use crate::types::Column;

use wasmer::{wat2wasm, Instance, Module, Store};
use wasmer_wasi::{Pipe, WasiEnv, WasiState};

pub type WasmError = Box<dyn std::error::Error>;
pub struct CustomWasmTransformer {
    database_name: String,
    table_name: String,
    column_name: String,
    wasi_env: WasiEnv,
    instance: Instance,
}

impl CustomWasmTransformer {
    pub fn new<S>(
        database_name: S,
        table_name: S,
        column_name: S,
        wasm_bytes: Vec<u8>,
    ) -> Result<Self, WasmError>
    where
        S: Into<String>,
    {
        // Create a Store
        let store = Store::default();

        // Compile the Wasm module
        let module = Module::new(&store, wasm_bytes)?;

        // Create the `WasiEnv` with the stdio pipes
        let input = Pipe::new();
        let output = Pipe::new();
        let mut wasi_env = WasiState::new("wasm-transformer")
            .stdin(Box::new(input))
            .stdout(Box::new(output))
            .finalize()?;

        // Import object related to WASI,
        // and attach it to the Wasm instance
        let import_object = wasi_env.import_object(&module)?;
        let instance = Instance::new(&module, &import_object)?;

        Ok({
            CustomWasmTransformer {
                database_name: database_name.into(),
                table_name: table_name.into(),
                column_name: column_name.into(),
                wasi_env,
                instance,
            }
        })
    }
    fn call_wasm_module(&self, value: &str) -> Result<String, WasmError> {
        // Access WasiState in a nested scope to ensure we're not holding
        // the mutex after we need it.
        {
            let mut state = self.wasi_env.state();
            let wasi_stdin = state.fs.stdin_mut()?.as_mut().unwrap();
            // Write to the stdin pipe
            writeln!(wasi_stdin, "{}", value)?;
        }

        // Call the `_start` function
        let start = self.instance.exports.get_function("_start")?;
        start.call(&[])?; //TODO support calling with parameters

        let mut state = self.wasi_env.state();
        let wasi_stdout = state.fs.stdout_mut()?.as_mut().unwrap();
        // Read from the stdout pipe
        let mut buf = String::new();
        wasi_stdout.read_to_string(&mut buf)?;

        Ok(buf.trim().into())
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
                Column::StringValue(column_name, self.call_wasm_module(value.as_str()).unwrap())
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

    fn get_wat_transformer() -> CustomWasmTransformer {
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
        .unwrap()
    }

    fn get_wasm_transformer(path: &str) -> CustomWasmTransformer {
        let wasm_bytes = std::fs::read(path).unwrap();
        CustomWasmTransformer::new("test", "users", "number", wasm_bytes).unwrap()
    }

    #[test]
    fn transform_wat_add_one() {
        let transformer = get_wat_transformer();
        let column = Column::NumberValue("number".to_string(), 1);
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.number_value().unwrap();

        assert_eq!(transformed_value, &2);
    }

    #[test]
    fn transform_wasm_reverse_string() {
        let transformer = get_wasm_transformer("../examples/wasm-transformer-reverse-string.wasm");
        let column = Column::StringValue("string".to_string(), "reverse_it".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert_eq!(transformed_value, "ti_esrever".to_string());
    }
}
