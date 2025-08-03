use bytes::{Bytes as BytesBuf, BytesMut};
use std::borrow::Cow;

pub type Bytes = Vec<u8>;
pub type OriginalQuery = Query;
pub type Queries = Vec<Query>;

/// Zero-copy byte container for better memory efficiency
#[derive(Debug, Clone)]
pub struct ZeroCopyBytes<'a> {
    data: Cow<'a, [u8]>,
}

impl<'a> ZeroCopyBytes<'a> {
    pub fn borrowed(data: &'a [u8]) -> Self {
        Self {
            data: Cow::Borrowed(data),
        }
    }

    pub fn owned(data: Vec<u8>) -> Self {
        Self {
            data: Cow::Owned(data),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

pub fn to_bytes(queries: Queries) -> Bytes {
    // Pre-calculate capacity to avoid reallocations
    let estimated_size: usize = queries.iter().map(|q| q.0.len() + 1).sum();
    let mut result = Vec::with_capacity(estimated_size);

    for query in queries {
        result.extend(query.0);
        result.push(b'\n');
    }

    result
}

/// Optimized version using BytesMut for zero-copy operations
pub fn to_bytes_optimized(queries: &[OptimizedQuery]) -> BytesBuf {
    let estimated_size: usize = queries.iter().map(|q| q.len() + 1).sum();
    let mut result = BytesMut::with_capacity(estimated_size);

    for query in queries {
        result.extend_from_slice(query.as_slice());
        result.extend_from_slice(b"\n");
    }

    result.freeze()
}

/// Memory pool for reusing query allocations
pub struct QueryPool {
    queries: std::sync::Mutex<Vec<OptimizedQuery>>,
    max_size: usize,
}

impl QueryPool {
    pub fn new(max_size: usize) -> Self {
        Self {
            queries: std::sync::Mutex::new(Vec::with_capacity(max_size)),
            max_size,
        }
    }

    pub fn get(&self, capacity: usize) -> OptimizedQuery {
        if let Ok(mut queries) = self.queries.lock() {
            if let Some(mut query) = queries.pop() {
                query.data.clear();
                if query.data.capacity() < capacity {
                    query.data.reserve(capacity - query.data.capacity());
                }
                return query;
            }
        }
        OptimizedQuery::new(capacity)
    }

    pub fn return_query(&self, query: OptimizedQuery) {
        if let Ok(mut queries) = self.queries.lock() {
            if queries.len() < self.max_size {
                queries.push(query);
            }
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Query(pub Vec<u8>);

/// Memory-optimized query that avoids unnecessary copies
#[derive(Debug)]
pub struct OptimizedQuery {
    data: BytesMut,
}

impl OptimizedQuery {
    pub fn new(capacity: usize) -> Self {
        Self {
            data: BytesMut::with_capacity(capacity),
        }
    }

    pub fn from_slice(data: &[u8]) -> Self {
        let mut buf = BytesMut::with_capacity(data.len());
        buf.extend_from_slice(data);
        Self { data: buf }
    }

    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }

    pub fn push(&mut self, byte: u8) {
        self.data.extend_from_slice(&[byte]);
    }

    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn freeze(self) -> BytesBuf {
        self.data.freeze()
    }
}

impl Query {
    pub fn data(&self) -> &Vec<u8> {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Create query with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self(Vec::with_capacity(capacity))
    }

    /// Extend from slice without reallocating if possible
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.0.extend_from_slice(data);
    }
}

#[derive(Clone)]
pub struct InsertIntoQuery {
    pub table_name: String,
    pub columns: Vec<Column>,
}

#[derive(Clone)]
pub enum Column {
    NumberValue(String, i128),
    FloatNumberValue(String, f64),
    StringValue(String, String),
    CharValue(String, char),
    BooleanValue(String, bool),
    None(String),
}

impl Column {
    pub fn name(&self) -> &str {
        match self {
            Column::NumberValue(name, _) => name.as_str(),
            Column::FloatNumberValue(name, _) => name.as_str(),
            Column::StringValue(name, _) => name.as_str(),
            Column::CharValue(name, _) => name.as_str(),
            Column::BooleanValue(name, _) => name.as_str(),
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

    pub fn boolean_value(&self) -> Option<&bool> {
        match self {
            Column::BooleanValue(_, value) => Some(value),
            _ => None,
        }
    }
}
