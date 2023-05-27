use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::duckdb::DuckDB;
use pschema_rs::backends::parquet::Parquet;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::{Shape, WShape};
use pschema_rs::utils::symbol_table::SymbolTable;
use std::time::Instant;
use wikidata_rs::id::Id;

#[cfg(not(target_env = "msvc"))]
use jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

fn main() -> Result<(), String> {
    // We define the Symbol Table as a control structure for handling conversions
    // between str and u8 data. This is done due to the performance gain
    let symbol_table = SymbolTable::new();

    // Define validation rules
    let shape = Shape::WShape(WShape::new(
        symbol_table.insert("City"),
        Id::from("P31").into(),
        Id::from("Q515").into(),
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
        Ok(subset) => {
            let duration = start.elapsed();
            println!("Time elapsed in validate() is: {:?}", duration);
            Parquet::export("wikidata-20170821-subset.parquet", subset)
        }
        Err(_) => return Err(String::from("Error creating the sub-graph :(")),
    }
}
