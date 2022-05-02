---
sidebar_position: 3
---

# Web Assembly transformer

- This folder contains an example of a wasm (WebAssembly) transformer which reads the column value (in this case, a string) input from stdin, transforms it (in this case, reverses it), and then writes the result to stdout.
- The file `wasm-transformer-reverse-string.wasm` was originally written in rust, and compiled to a `wasm32-wasi` target binary.
- A great way to implement your own custom wasm transformer would be to write it in a [language which supports WebAssembly in a WASI environment](https://www.fermyon.com/wasm-languages/webassembly-language-support) and then compile it to a `.wasm` file.
- In the following section, we will demonstrate how to implement a custom wasm transformer by using Rust (to understand how to do this with other languages, we suggest reading more about [`wasm`](https://developer.mozilla.org/en-US/docs/WebAssembly) and [`WASI`](https://wasi.dev/)).

## How it works

RepliByte's communication with external `wasm` modules is implemented with the use of pipes:
1. The column value which needs to be transformed is written to stdin by RepliByte. This will always be a single column value.
2. The wasm module should read the value from stdin and **transform** it (this is where your custom implementation comes in).
3. The wasm module should write the transformed value to stdout.
4. RepliByte reads the transformed value from stdout. RepliByte will expect to read a single column value, anything else will cause a runtime error.

As long as you start with reading from stdin and end with printing to stdout, you can go as crazy as you want with the implementation of your custom transformers.

## Implementing a custom transformer with Rust

First, start a new cargo project:

```shell
cargo init my-custom-wasm-transformer
```

Go to `src/main.rs` in the newly created project and write some code:

```rust
// This is actually the source of the `.wasm` file in this example. Feel free to edit it !
fn main() {
    // Read input value from stdin
    let mut input = String::new();
    std::io::stdin().read_line(&mut input).unwrap();
    
    // Transform the value as you see fit (in this case we just reverse the string)
    let output: String = input.chars().rev().collect();
    
    // Write transformed value to stdout (simply print)
    println!("{}", output);
}
```

Add `wasm32-wasi` to your targets:

```shell
rustup target add wasm32-wasi
```

Build:

```shell
cargo build --release --target wasm32-wasi
```

You will find your freshly built custom wasm transformer here:

`target/wasm32-wasi/release/my-custom-wasm-transformer.wasm`

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

