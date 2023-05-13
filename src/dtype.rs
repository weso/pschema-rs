use polars::prelude::*;

pub enum DataType {
    Int,
    Float,
    String,
    Bool,
    Date,
    DateTime,
    Time,
    Duration,
    List(Box<DataType>),
    Map(Box<DataType>, Box<DataType>),
    Struct(Vec<(String, DataType)>),
    Entity,
}

impl DataType {
    pub fn is_entity(&self) -> bool {
        matches!(self, DataType::Entity)
    }
}

impl From<DataType> for Expr {
    fn from(value: DataType) -> Self {
        match value {
            DataType::Int => lit("int"),
            DataType::Float => lit("float"),
            DataType::String => lit("string"),
            DataType::Bool => lit("bool"),
            DataType::Date => lit("date"),
            DataType::DateTime => lit("datetime"),
            DataType::Time => lit("time"),
            DataType::Duration => lit("duration"),
            DataType::List(inner) => {
                let inner = Expr::from(*inner);
                lit(format!("list<{}>", inner))
            }
            DataType::Map(key, value) => {
                let key = Expr::from(*key);
                let value = Expr::from(*value);
                lit(format!("map<{}, {}>", key, value))
            }
            DataType::Struct(fields) => {
                let fields = fields
                    .into_iter()
                    .map(|(name, dtype)| format!("{}: {}", name, Expr::from(dtype)))
                    .collect::<Vec<_>>()
                    .join(", ");
                lit(format!("struct<{}>", fields))
            }
            DataType::Entity => lit("entity"),
        }
    }
}