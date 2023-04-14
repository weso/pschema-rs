use std::path::Path;
use duckdb::arrow::array::Int32Array;
use duckdb::arrow::record_batch::RecordBatch;
use duckdb::Connection;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrameError;
use pregel_rs::pregel::ColumnIdentifier;

const STATEMENT: &str = "
    select src_id, property_id, dst_id from edge
    union
    select src_id, property_id, dst_id from coordinate
    union
    select src_id, property_id, dst_id from quantity
    union
    select src_id, property_id, dst_id from string
    union
    select src_id, property_id, dst_id from time
";

pub struct DumpUtils;

impl DumpUtils {

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

    pub fn edges_from_duckdb(path: &str) -> Result<DataFrame, &str> {
        let connection: Connection = match Path::new(path) {
            Ok(path) => match Connection::open(path) {
                Ok(connection) => connection,
                Err(_) => return Err("Cannot connect to the database")
            },
            Err(_) => return Err("Cannot open the provided path")
        };

        let mut statement = match connection.prepare(STATEMENT) {
            Ok(statement) => statement,
            Err(_) => return Err("Cannot prepare the provided statement")
        };

        let batches: Vec<RecordBatch> = match statement.query_arrow([]) {
            Ok(arrow) => arrow.collect(),
            Err(_) => return Err("Error executing the Arrow query")
        };

        let mut edges = DataFrame::default();

        for batch in batches {
            // See the STATEMENT constant to understand the following lines :D
            let src_id = batch.column(0); // because we know that the first column is the src_id
            let p_id = batch.column(1); // because we know that the second column is the property_id
            let dst_id = batch.column(2); // because we know that the third column is the dst_id

            let srcs = series_from_duckdb(ColumnIdentifier::Src, src_id);
            let properties = series_from_duckdb(ColumnIdentifier::Custom("property_id".to_string()), p_id);
            let dsts = series_from_duckdb(ColumnIdentifier::Dst, dst_id);

            let tmp_dataframe = match DataFrame::new(vec![srcs, properties, dsts]) {
                Ok(tmp_dataframe) => tmp_dataframe,
                Err(_) => return Err("Error creating the DataFrame"),
            };

            edges = match edges.vstack(&tmp_dataframe) {
                Ok(dataframe) => dataframe,
                Err(_) => return Err("Error stacking the DataFrames")
            };
        }

        Ok(edges)
    }

}