use polars::prelude::DataFrame;

/// `pub mod duckdb_dump;` is creating a public module named `duckdb_dump`. This
/// module contains code related to dumping data from a DuckDB database.
pub mod duckdb;

pub trait Backend {
    fn import(path: &str) -> Result<DataFrame, String>;
    fn export(path: &str, df: DataFrame) -> Result<(), String>;
}
