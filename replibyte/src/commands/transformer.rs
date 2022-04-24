use crate::transformer::transformers;
use crate::utils::table;

/// display all transformers available
pub fn list() {
    let mut table = table();
    table.set_titles(row!["name", "description"]);

    for transformer in transformers() {
        table.add_row(row![transformer.id(), transformer.description()]);
    }

    let _ = table.printstd();
}
