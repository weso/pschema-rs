use crate::dtype::DataType;
use duckdb::arrow::array::{Array, Int32Array};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use std::path::Path;

pub struct DumpUtils;

/// The `impl DumpUtils` block defines a Rust module that contains two functions:
/// `series_from_duckdb` and `edges_from_duckdb`.
impl DumpUtils {
    /// The function converts an Int32Array into a Series object with a specified column
    /// identifier.
    ///
    /// Arguments:
    ///
    /// * `column_identifier`: The `column_identifier` parameter is a `Column` object
    /// that represents the name and data type of a column in a table. It is used to
    /// create a new `Series` object with the same name and data type as the original
    /// column.
    /// * `array_ref`: `array_ref` is a reference to an array that implements the
    /// `Array` trait, which is a trait for Arrow arrays. The `Array` trait provides a
    /// common interface for working with different types of Arrow arrays, such as
    /// `Int32Array`, `Float64Array`, `BooleanArray`,
    ///
    /// Returns:
    ///
    /// A `Series` object is being returned.
    fn series_from_duckdb(column_identifier: Column, array_ref: &Arc<dyn Array>) -> Series {
        Series::new(
            column_identifier.as_ref(),
            array_ref
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .values()
                .to_vec(),
        )
    }

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
    pub fn edges_from_duckdb(path: &str) -> Result<DataFrame, String> {
        let stmt = format!(
            "
            select src_id, property_id, dst_id, {:} from edge
            union
            select src_id, property_id, dst_id, {:} from coordinate
            union
            select src_id, property_id, dst_id, {:} from quantity
            union
            select src_id, property_id, dst_id, {:} from string
            union
            select src_id, property_id, dst_id, {:} from time
            ",
            DataType::Entity,
            DataType::Coordinate,
            DataType::Quantity,
            DataType::String,
            DataType::DateTime,
        );

        let connection: Connection = match Path::new(path).try_exists() {
            Ok(true) => match Connection::open(Path::new(path)) {
                Ok(connection) => connection,
                Err(_) => return Err(String::from("Cannot connect to the database")),
            },
            _ => return Err(String::from("Make sure you provide an existing path")),
        };

        let mut statement = match connection.prepare(&*stmt) {
            Ok(statement) => statement,
            Err(_) => return Err(String::from("Cannot prepare the provided statement")),
        };

        let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
            Ok(arrow) => arrow.collect(),
            Err(_) => return Err(String::from("Error executing the Arrow query")),
        };

        let mut edges = DataFrame::default();

        for batch in batches {
            // See the STATEMENT constant to understand the following lines :D
            let src_id = batch.column(0); // because we know that the first column is the src_id
            let p_id = batch.column(1); // because we know that the second column is the property_id
            let dst_id = batch.column(2); // because we know that the third column is the dst_id
            let dtype_id = batch.column(3); // because we know that the fourth column is the dtype

            let srcs = Self::series_from_duckdb(Column::Src, src_id);
            let properties = Self::series_from_duckdb(Column::Custom("property_id"), p_id);
            let dsts = Self::series_from_duckdb(Column::Dst, dst_id);
            let dtypes = Self::series_from_duckdb(Column::Custom("dtype"), dtype_id);

            let tmp_dataframe = match DataFrame::new(vec![srcs, properties, dsts, dtypes]) {
                Ok(tmp_dataframe) => tmp_dataframe,
                Err(_) => return Err(String::from("Error creating the DataFrame")),
            };

            edges = match edges.vstack(&tmp_dataframe) {
                Ok(dataframe) => dataframe,
                Err(_) => return Err(String::from("Error vertically stacking the DataFrames")),
            };
        }

        Ok(edges)
    }
}
