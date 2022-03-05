pub type Row = Vec<Column>;

pub struct Column {
    name: String,
    value: String,
}

impl Column {
    pub fn new<S: Into<String>, T: Into<String>>(name: S, value: T) -> Self {
        Column {
            name: name.into(),
            value: value.into(),
        }
    }
}
