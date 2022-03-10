use crate::types::Row;

pub trait Transformer {
    fn transform(&self, row: Row) -> Row;
}

/// make no transformation
pub struct NoTransformer;

impl Default for NoTransformer {
    fn default() -> Self {
        NoTransformer {}
    }
}

impl Transformer for NoTransformer {
    fn transform(&self, row: Row) -> Row {
        row
    }
}
