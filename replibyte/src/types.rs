pub struct Row {
    pub table_name: String,
    pub columns: Vec<Column>,
}

pub enum Column {
    NumberValue(String, i128),
    FloatNumberValue(String, f64),
    StringValue(String, String),
    CharValue(String, char),
    None,
}
