use crate::transformer::Transformer;
use crate::types::Column;

use serde::{Deserialize, Serialize};
use wasmer::{wat2wasm, Instance, Module, Store};
use wasmer_wasi::{Pipe, WasiEnv, WasiState};

pub type WasmError = Box<dyn std::error::Error>;

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct CustomWasmTransformerOptions {
    pub path: String,
}
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

impl Default for CustomWasmTransformer {
    fn default() -> Self {
        CustomWasmTransformer {
            database_name: "database_name".into(),
            table_name: "table_name".into(),
            column_name: "column_name".into(),
            wasi_env: WasiState::new("default").finalize().unwrap(),
            instance: Instance::new(
                &Module::new(
                    &Store::default(),
                    vec![
                        // 'add one' transformer
                        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, 0x01, 0x06, 0x01, 0x60,
                        0x01, 0x7f, 0x01, 0x7f, 0x03, 0x02, 0x01, 0x00, 0x07, 0x0b, 0x01, 0x07,
                        0x61, 0x64, 0x64, 0x5f, 0x6f, 0x6e, 0x65, 0x00, 0x00, 0x0a, 0x09, 0x01,
                        0x07, 0x00, 0x20, 0x00, 0x41, 0x01, 0x6a, 0x0b, 0x00, 0x1a, 0x04, 0x6e,
                        0x61, 0x6d, 0x65, 0x01, 0x0a, 0x01, 0x00, 0x07, 0x61, 0x64, 0x64, 0x5f,
                        0x6f, 0x6e, 0x65, 0x02, 0x07, 0x01, 0x00, 0x01, 0x00, 0x02, 0x70, 0x30,
                    ],
                )
                .unwrap(),
                &wasmer::ImportObject::default(),
            )
            .unwrap(),
        }
    }
}

impl Transformer for CustomWasmTransformer {
    fn id(&self) -> &str {
        "custom-wasm"
    }

    fn description(&self) -> &str {
        "Provide a custom transformer as a wasm (WebAssembly) module."
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
            Column::NumberValue(column_name, value) => Column::NumberValue(
                column_name,
                self.call_wasm_module(value.to_string().as_str())
                    .unwrap()
                    .parse::<i128>()
                    .unwrap(),
            ),
            Column::FloatNumberValue(column_name, value) => Column::FloatNumberValue(
                column_name,
                self.call_wasm_module(value.to_string().as_str())
                    .unwrap()
                    .parse::<f64>()
                    .unwrap(),
            ),
            Column::CharValue(column_name, value) => Column::CharValue(
                column_name,
                self.call_wasm_module(value.to_string().as_str())
                    .unwrap()
                    .parse::<char>()
                    .unwrap(),
            ),
            Column::None(column_name) => Column::None(column_name),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        transformer::{custom_wasm::CustomWasmTransformer, Transformer},
        types::Column,
    };

    fn get_wasm_transformer(path: &str) -> CustomWasmTransformer {
        let wasm_bytes = std::fs::read(path).unwrap();
        CustomWasmTransformer::new("test", "users", "number", wasm_bytes).unwrap()
    }

    #[test]
    fn transform_wasm_reverse_string() {
        let transformer = get_wasm_transformer("../examples/wasm/wasm-transformer-reverse-string.wasm");
        let column = Column::StringValue("string".to_string(), "reverse_it".to_string());
        let transformed_column = transformer.transform(column);
        let transformed_value = transformed_column.string_value().unwrap();

        assert_eq!(transformed_value, "ti_esrever".to_string());
    }
}
