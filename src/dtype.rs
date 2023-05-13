use polars::prelude::*;

#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Quantity,
    Coordinate,
    String,
    DateTime,
    Entity,
}

impl From<DataType> for Expr {
    fn from(value: DataType) -> Self {
        lit(u64::from(&value))
    }
}

impl From<&DataType> for u64 {
    fn from(value: &DataType) -> Self {
        match value {
            DataType::Quantity => 1,
            DataType::Coordinate => 2,
            DataType::String => 3,
            DataType::DateTime => 4,
            DataType::Entity => 5,
        }
    }
}
