# RepliByte with a custom WebAssembly transformer

- This folder contains an example of a wasm (WebAssembly) transformer which reads the column value (in this case, a string) input from stdin, transforms it (in this case, reverses it), and then writes the result to stdout.

 - The file `wasm-transformer-reverse-string.wasm`  was originally written in rust, and compiled to a `wasm32-wasi` target binary.

 - A great way to implement your own custom wasm transformer would be to write it in a [language which supports WebAssembly in a WASI environment](https://www.fermyon.com/wasm-languages/webassembly-language-support) and then compile it to a `.wasm` file.

 - In the following section, we will demonstrate how to implement a custom wasm transformer by using Rust (to understand how to do this with other languages, we suggest reading more about [`wasm`](https://developer.mozilla.org/en-US/docs/WebAssembly) and [`WASI`](https://wasi.dev/)).

<br>

## Implenting a custom transformer with Rust
---
<br>

First, start a new cargo project:
```sh
cargo init my-custom-wasm-transformer
```
Go to `src/main.rs` in the newly created project and write some code:
```rust
// This is actually the source of the `.wasm` file in this example. Feel free to edit it !

use std::io;

fn main() {
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();

    let output: String = input.chars().rev().collect();

    println!("{}", output);
}
```

Add `wasm32-wasi` to your targets:
```sh
rustup target add wasm32-wasi
```
Build:
```sh
cargo build --release --target wasm32-wasi
```

You will your freshly built custom wasm transformer here:

 `target/wasm32-wasi/debug/my-custom-wasm-transformer.wasm`

 The only thing that's left is to edit the `path` option in `replibyte.yaml`:

 ```yaml
# ...
    - database: <your-db-name>
      table: <your-table-name>
      columns:
        - name: <your-column-name>
          transformer_name: custom-wasm
          transformer_options:
            path: "path/to/your/custom-wasm-transformer.wasm"
# ...
 ```

 
That's it!