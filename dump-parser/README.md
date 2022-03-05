# Dump Parser

Library to parse and edit database dump for Postgres, MySQL and MongoDB.

```rust
let dp = DumpParser::new("./db/dump.sql", Type::Postgres);

// get type
dp.dump_type(); // Postgres

// list databases
dp.databases();

// list tables
let db = dp.get_database("db_name");
db.tables("db_name");

// get table "table_name"
let table = db.get_table("table_name");

// list over table rows
for row in table.rows() {
    let mut column = row.get_column("name");
    // update column
    column.set_value(format!("{} name updated", column.value()));
}

let _ = dp.save("./db/dump-updated.sql");
```
