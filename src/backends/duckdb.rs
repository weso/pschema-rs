use duckdb::arrow::array::{Array, UInt32Array};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use polars::frame::DataFrame;
use polars::prelude::NamedFrom;
use polars::series::Series;
use pregel_rs::pregel::Column;
use rayon::iter::ParallelIterator;
use rayon::prelude::IntoParallelIterator;
use std::path::Path;
use strum::IntoEnumIterator;
use wikidata_rs::dtype::DataType;
use wikidata_rs::id::Id;

use super::Backend;

pub struct DuckDB;

/// The `DuckDB` block defines a Rust module that contains `import` and `export`.
impl Backend for DuckDB {
    /// This function retrieves data from a DuckDB database and returns it as a
    /// DataFrame.
    ///
    /// Arguments:
    ///
    /// * `path`: The path to the DuckDB database file.
    ///
    /// Returns:
    ///
    /// This function returns a `Result<DataFrame, String>`, where the `DataFrame`
    /// is the result of querying and processing data from a DuckDB database, and
    /// the `String` is an error message in case any error occurs during the
    /// execution of the function.
    fn import(path: &str) -> Result<DataFrame, String> {
        let format = |id: DataType| {
            format!(
                "SELECT src_id, property_id, CAST({:} AS UINTEGER) FROM {:}",
                u32::from(Id::DataType(id.to_owned())),
                id.as_ref()
            )
        };

        let stmt = DataType::iter()
            .map(|dtype| match dtype {
                DataType::Quantity => format(DataType::Quantity),
                DataType::Coordinate => format(DataType::Coordinate),
                DataType::String => format(DataType::String),
                DataType::DateTime => format(DataType::DateTime),
                DataType::Entity => format!(
                    "SELECT src_id, property_id, dst_id FROM {:}",
                    dtype.as_ref()
                ),
            })
            .collect::<Vec<String>>()
            .join(" UNION ");

        let connection: Connection = match Path::new(path).try_exists() {
            Ok(true) => match Connection::open(Path::new(path)) {
                Ok(connection) => connection,
                Err(_) => return Err(String::from("Cannot connect to the database")),
            },
            _ => return Err(String::from("Make sure you provide an existing path")),
        };

        let mut statement = match connection.prepare(stmt.as_ref()) {
            Ok(statement) => statement,
            Err(error) => return Err(format!("Cannot prepare the provided statement {}", error)),
        };

        let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
            Ok(arrow) => arrow.collect(),
            Err(_) => return Err(String::from("Error executing the Arrow query")),
        };

        Ok(batches
            .into_par_iter()
            .map(|batch| {
                match DataFrame::new(vec![
                    Series::new(
                        Column::Subject.as_ref(),
                        // because we know that the first column is the src_id
                        batch
                            .column(0)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values(),
                    ),
                    Series::new(
                        Column::Predicate.as_ref(),
                        // because we know that the second column is the property_id
                        batch
                            .column(1)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values(),
                    ),
                    Series::new(
                        Column::Object.as_ref(),
                        // because we know that the third column is the dst_id
                        batch
                            .column(2)
                            .as_any()
                            .downcast_ref::<UInt32Array>()
                            .unwrap()
                            .values(),
                    ),
                ]) {
                    Ok(tmp_dataframe) => tmp_dataframe,
                    Err(_) => DataFrame::empty(),
                }
            })
            .reduce(DataFrame::empty, |acc, e| acc.vstack(&e).unwrap()))
    }

    fn export(_path: &str, _df: DataFrame) -> Result<(), String> {
        todo!()
    }
}
