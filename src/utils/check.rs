use polars::error::PolarsError;
use polars::frame::DataFrame;
use pregel_rs::pregel::Column;

pub(crate) fn check_field(edges: &DataFrame, column: Column) -> Result<(), PolarsError> {
    if edges.schema().get_field(column.as_ref()).is_none() {
        return Err(PolarsError::SchemaFieldNotFound(
            column.as_ref().to_string().into(),
        ));
    } else if edges.column(column.as_ref()).unwrap().len() == 0 {
        return Err(PolarsError::NoData(column.as_ref().to_string().into()));
    }
    Ok(())
}
