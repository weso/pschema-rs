use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::{WShape};

fn main() -> Result<(), String> {
    // Define validation rules
    let start = WShape::new("A", 1000000031, 331769);

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/example.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start.into()).validate(graph) {
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
        Err(_) => Err(String::from("Cannot create a GraphFrame")),
    }
}
