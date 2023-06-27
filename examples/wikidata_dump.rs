use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::duckdb::DuckDB;
use pschema_rs::backends::parquet::Parquet;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::NodeConstraint;
use pschema_rs::shape::shex::Shape;
use pschema_rs::shape::shex::TripleConstraint;
use std::time::Instant;
use wikidata_rs::id::Id;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(target_env = "msvc")]
use mimalloc::MiMalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

#[cfg(target_env = "msvc")]
#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

fn main() -> Result<(), String> {
    // Define validation rules
    let shape = Shape::TripleConstraint(TripleConstraint::new(
        "City",
        u32::from(Id::from("P31")),
        NodeConstraint::Value(u32::from(Id::from("Q515"))),
    ));

    // Load Wikidata entities
    let edges = match DuckDB::import("../wd2duckdb/wikidata-20170821-all.duckdb") {
        Ok(edges) => edges,
        Err(_) => return Err(String::from("Error creating the edges :(")),
    };

    let graph = match GraphFrame::from_edges(edges) {
        Ok(graph) => graph,
        Err(_) => return Err(String::from("Error creating the graph :(")),
    };

    // Perform schema validation
    let start = Instant::now();
    match PSchema::new(shape).validate(graph) {
        Ok(mut subset) => {
            let duration = start.elapsed();
            println!("Time elapsed in validate() is: {:?}", duration);
            Parquet::export("wikidata-20170821-subset.parquet", &mut subset)
        }
        Err(_) => return Err(String::from("Error creating the sub-graph :(")),
    }
}
