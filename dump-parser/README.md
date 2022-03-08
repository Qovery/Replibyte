# Dump Parser

Library to parse and edit database dump for Postgres, MySQL and MongoDB.

```rust
let db = Postgres::new("../db/postgres/fulldump-with-inserts.sql");

// get type
db.database_type(); // Postgres

// list databases
db.databases();

// list tables
let db = dp.get_database("db_name");
db.tables();

// get table "table_name"
let table = db.get_table("table_name");

// list over table rows
for row in table.rows() {
    let mut column = row.get_column("name");
    // update column
    column.set_value(format!("{} name updated", column.value()));
}

let _ = db.save("./db/dump-updated.sql");
```
