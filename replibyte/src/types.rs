pub type OriginalRow = Row;

#[derive(Clone)]
pub struct Row {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Clone)]
pub enum Column {
    NumberValue(String, i128),
    FloatNumberValue(String, f64),
    StringValue(String, String),
    CharValue(String, char),
    None(String),
}

impl Column {
    pub fn name(&self) -> &str {
        match self {
            Column::NumberValue(name, _) => name.as_str(),
            Column::FloatNumberValue(name, _) => name.as_str(),
            Column::StringValue(name, _) => name.as_str(),
            Column::CharValue(name, _) => name.as_str(),
            Column::None(name) => name.as_str(),
        }
    }

    pub fn number_value(&self) -> Option<&i128> {
        match self {
            Column::NumberValue(_, value) => Some(value),
            _ => None,
        }
    }

    pub fn string_value(&self) -> Option<&str> {
        match self {
            Column::StringValue(_, value) => Some(value.as_str()),
            _ => None,
        }
    }

    pub fn float_number_value(&self) -> Option<&f64> {
        match self {
            Column::FloatNumberValue(_, value) => Some(value),
            _ => None,
        }
    }

    pub fn char_value(&self) -> Option<&char> {
        match self {
            Column::CharValue(_, value) => Some(value),
            _ => None,
        }
    }
}
