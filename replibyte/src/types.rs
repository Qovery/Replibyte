pub type Bytes = Vec<u8>;
pub type OriginalQuery = Query;

pub type Queries = Vec<Query>;

pub fn to_bytes(queries: Queries) -> Bytes {
    queries
        .into_iter()
        .flat_map(|query| {
            let mut bytes = query.0;
            bytes.push(b'\n');
            bytes
        })
        .collect::<Vec<_>>()
}

#[derive(Debug, Clone)]
pub struct Query(pub Vec<u8>);

impl Query {
    pub fn data(&self) -> &Vec<u8> {
        &self.0
    }
}

#[derive(Clone)]
pub struct InsertIntoQuery {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NumberValue {
    I32(i32),
    I64(i64),
    I128(i128),
    U32(u32),
    U64(u64),
    U128(u128),
}

impl std::fmt::Display for NumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NumberValue::I32(val) => write!(f, "{}", val),
            NumberValue::I64(val) => write!(f, "{}", val),
            NumberValue::I128(val) => write!(f, "{}", val),
            NumberValue::U32(val) => write!(f, "{}", val),
            NumberValue::U64(val) => write!(f, "{}", val),
            NumberValue::U128(val) => write!(f, "{}", val),
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum FloatNumberValue {
    F32(f32),
    F64(f64),
}

impl std::fmt::Display for FloatNumberValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FloatNumberValue::F32(val) => write!(f, "{}", val),
            FloatNumberValue::F64(val) => write!(f, "{}", val),
        }
    }
}

#[derive(Clone)]
pub enum Column {
    NumberValue(String, NumberValue),
    FloatNumberValue(String, FloatNumberValue),
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

    pub fn number_value(&self) -> Option<&NumberValue> {
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

    pub fn float_number_value(&self) -> Option<&FloatNumberValue> {
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
