# Subset

Subset is a Rust crate to scale down a database to a more reasonable size. So it can be used in staging, test and development environments.

## Usage

```rust
fn main() -> Result<(), Error> {
    // equivalent `SELECT * FROM public.users WHERE random() < 0.05;`
    let ref_query = subset::postgres::SubsetQuery::RandomPercent("public", "users", 5);
    
    let psql = subset::Postgres::new(schema_reader, dump_reader_callback, ref_query)?;
    
    // Get graph
    let graph: Graph = psql.graph();
    // TODO check graph
    
    // by calling rows()
    for row in psql.rows() {
        // streamed data from dump_reader
    }
}
```
