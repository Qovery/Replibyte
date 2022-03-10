pub struct Row {
    pub table_name: String,
    pub columns: Vec<Column>,
}

pub enum Column {
    IntValue(String, i64),
    UIntValue(String, u64),
    StringValue(String, String),
    CharValue(String, char),
    None,
}
