pub mod postgres;

#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SubsetTable {
    pub database: String,
    pub table: String,
    pub relations: Vec<SubsetTableRelation>,
}

impl SubsetTable {
    pub fn new<S: Into<String>>(
        database: S,
        table: S,
        relations: Vec<SubsetTableRelation>,
    ) -> Self {
        SubsetTable {
            database: database.into(),
            table: table.into(),
            relations,
        }
    }
}

/// Representing a query where...
/// database -> is the targeted database
/// table -> is the targeted table
/// from_property is the parent table property referencing the target table `to_property`
#[derive(Debug, Hash, Eq, PartialEq)]
pub struct SubsetTableRelation {
    pub database: String,
    pub table: String,
    pub from_property: String,
    pub to_property: String,
}

impl SubsetTableRelation {
    pub fn new<S: Into<String>>(database: S, table: S, from_property: S, to_property: S) -> Self {
        SubsetTableRelation {
            database: database.into(),
            table: table.into(),
            from_property: from_property.into(),
            to_property: to_property.into(),
        }
    }
}

trait Subset {
    fn ordered_tables(&self) -> Vec<SubsetTable>;
    fn rows(&self); // TODO callback row by row
}
