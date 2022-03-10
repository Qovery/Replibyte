# RepliByte

```rust
let mut source = Postgres::new("postgres://root:password@localhost:5432", false);
source.set_transformer(Transformer::None);

let bridge = S3::new();

let mut task = FullBackupTask::new(source, bridge);
task.run()
```
