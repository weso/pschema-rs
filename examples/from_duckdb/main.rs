use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::id::Id;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::{Shape, WShape};

fn main() -> Result<(), String> {
    // Define validation rules
    let start = Shape::WShape(WShape::new(
        "City",
        Id::from("P31").into(),
        Id::from("Q515").into(),
    ));

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/3000lines.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start).validate(graph) {
            Ok(result) => {
                println!("Schema validation result:");
                println!(
                    "{:?}",
                    result
                        .lazy()
                        .select(&[col("id"), col("labels")])
                        .filter(col("labels").is_not_null())
                        .collect()
                );
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
