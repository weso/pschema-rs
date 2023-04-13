use duckdb::arrow::array::{ArrayRef, Int32Array};
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use polars::prelude::*;
use pregel_rs::graph_frame::{GraphFrame, GraphFrameError};
use pregel_rs::pregel::ColumnIdentifier;
use pregel_rs::pregel::ColumnIdentifier::{Custom, Dst, Src};

fn series_from_duckdb(
    column_identifier: ColumnIdentifier,
    array_ref: &ArrayRef,
) -> Series {
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

/// This function creates a new `GraphFrame` from a duckdb connection. This is, the function will
/// create a new `GraphFrame` by selecting all the entities from the tables and concatenating them
/// into a unique set of edges. The connection should contain the following tables:
///     - Edge
///     - Coordinate
///     - Quantity
///     - String
///     - Time
///
/// Arguments:
///
/// * `connection`: A duckdb connection.
///
/// Returns:
///
/// The `from_duckdb` function returns a `Result<Self>` where `Self` is the `GraphFrame` struct.
/// The `Ok` variant of the `Result` contains an instance of `GraphFrame` initialized  with the
/// provided `connection`.
pub fn from_duckdb(connection: Connection) -> Result<GraphFrame, GraphFrameError> {
    let mut statement = match connection.prepare(
        "select src_id, property_id, dst_id from edge
            union
            select src_id, property_id, dst_id from coordinate
            union
            select src_id, property_id, dst_id from quantity
            union
            select src_id, property_id, dst_id from string
            union
            select src_id, property_id, dst_id from time",
    ) {
        Ok(statement) => statement,
        Err(_) => {
            return Err(GraphFrameError::DuckDbError(
                "Cannot prepare the provided statement",
            ))
        }
    };

    let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
        Ok(arrow) => arrow.collect(),
        Err(_) => {
            return Err(GraphFrameError::DuckDbError(
                "Error executing the Arrow query",
            ))
        }
    };

    let mut dataframe = DataFrame::default();

    for batch in batches {
        let src_id = batch.column(0); // because we know that the first column is the src_id
        let p_id = batch.column(1); // because we know that the second column is the property_id
        let dst_id = batch.column(2); // because we know that the third column is the dst_id

        let srcs = series_from_duckdb(Src, src_id);
        let properties = series_from_duckdb(Custom("property_id".to_string()), p_id);
        let dsts = series_from_duckdb(Dst, dst_id);

        let tmp_dataframe = match DataFrame::new(vec![srcs, properties, dsts]) {
            Ok(tmp_dataframe) => tmp_dataframe,
            Err(_) => return Err(GraphFrameError::DuckDbError("Error creating the DataFrame")),
        };

        dataframe = match dataframe.vstack(&tmp_dataframe) {
            Ok(dataframe) => dataframe,
            Err(_) => {
                return Err(GraphFrameError::DuckDbError(
                    "Error stacking the DataFrames",
                ))
            }
        };
    }

    GraphFrame::from_edges(dataframe)
}

fn main() {
    println!("Hello, world!");
}
