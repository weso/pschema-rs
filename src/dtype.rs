use polars::prelude::*;
use std::fmt::Display;

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

impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: u64 = self.into();
        write!(f, "{}", value)
    }
}
