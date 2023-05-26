use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::{Shape, WShape};
use std::time::Instant;
use wikidata_rs::id::Id;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() {
    // Define validation rules
    let shape = Shape::WShape(WShape::new(
        "City",
        Id::from("P31").into(),
        Id::from("Q515").into(),
    ));

    // Load Wikidata entities
    if let Ok(edges) = DumpUtils::edges_from_duckdb("../wd2duckdb/1million_lines.duckdb") {
        // Perform schema validation
        if let Ok(graph) = GraphFrame::from_edges(edges) {
            let start = Instant::now();
            let _ = PSchema::new(shape).validate(graph);
            let duration = start.elapsed();

            println!("Time elapsed in validate() is: {:?}", duration);
        }
    }
}
