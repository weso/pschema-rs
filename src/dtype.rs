use polars::prelude::*;
use std::fmt::Display;

/// This code defines an enumeration called `DataType` with five possible variants:
/// `Quantity`, `Coordinate`, `String`, `DateTime`, and `Entity`. The
/// `#[derive(Clone, Debug, PartialEq)]` attribute macros are used to automatically
/// generate implementations of the `Clone`, `Debug`, and `PartialEq` traits for the
/// `DataType` enum. This allows instances of the enum to be cloned, printed for
/// debugging purposes, and compared for equality using the `==` operator.
#[derive(Clone, Debug, PartialEq)]
pub enum DataType {
    Quantity,
    Coordinate,
    String,
    DateTime,
    Entity,
}

/// This implementation allows instances of the `DataType` enum to be converted into
/// `Expr` instances. It does so by calling the `lit` function with the result of
/// converting the `DataType` variant into a `u64` using the `From` implementation
/// defined for `&DataType`. The resulting `Expr` represents a literal value that
/// can be used in Polars expressions.
impl From<DataType> for Expr {
    fn from(value: DataType) -> Self {
        lit(u64::from(&value))
    }
}

/// This implementation allows for conversion from a reference to a `DataType` enum
/// variant to a `u64` integer. It matches the variant of the `DataType` enum and
/// returns a corresponding `u64` value. This is used in the `From<DataType> for
/// Expr` implementation to convert a `DataType` variant into a literal `Expr` value
/// that can be used in Polars expressions. It is also used in the `Display`
/// implementation to convert a `DataType` variant into a string representation of
/// its corresponding `u64` value.
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

/// This code defines a display implementation for the `DataType` enum, which allows
/// instances of the enum to be formatted as strings using the `format!` macro or
/// other formatting methods. The `fmt` method takes a reference to a `Formatter`
/// object and returns a `Result` indicating whether the formatting was successful.
/// Inside the method, the `into` method is called on `self` to convert the
/// `DataType` variant into a `u64` integer, which is then written to the formatter
/// using the `write!` macro. This allows the `DataType` enum to be displayed as its
/// corresponding `u64` value when formatted as a string.
impl Display for DataType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value: u64 = self.into();
        write!(f, "{}", value)
    }
}
