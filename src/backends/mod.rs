use polars::prelude::DataFrame;

/// `pub mod duckdb_dump;` is creating a public module named `duckdb`. This
/// module contains code related to dumping data from a DuckDB database.
pub mod duckdb;
/// `pub mod duckdb_dump;` is creating a public module named `parquet`. This
/// module contains code related to dumping data from a Parquet file.
pub mod parquet;

pub mod ntriples;

pub trait Backend {
    fn import(path: &str) -> Result<DataFrame, String>;
    fn export(path: &str, df: &mut DataFrame) -> Result<(), String>;
}
