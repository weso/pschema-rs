use ego_tree::tree;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::pschema::PSchema;
use pschema_rs::rules::WShape;

fn main() -> Result<(), String> {
    // Define validation rules
    let tree = tree! { // TODO: remove ego_tree I think is not necessary :D
        WShape::new("A", 31, 1000000571) => {
            WShape::new("B", 31, 1000000571) => {
                WShape::new("C", 31, 1000000571),
                WShape::new("D", 31, 1000000571),
            },
            WShape::new("E", 31, 1000000571) => {
                WShape::new("F", 31, 1000000571),
                WShape::new("G", 31, 1000000571),
            },
            WShape::new("H", 31, 1000000571),
        }
    };

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/example.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(tree).validate(graph) {
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
