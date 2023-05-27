use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::duckdb::DuckDB;
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

fn main() {
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
    if let Ok(edges) = DuckDB::import("../wd2duckdb/wikidata-20170821-all.duckdb") {
        // Perform schema validation
        if let Ok(graph) = GraphFrame::from_edges(edges) {
            let start = Instant::now();
            let subset = PSchema::new(shape).validate(graph);
            let duration = start.elapsed();

            println!("Time elapsed in validate() is: {:?}", duration);

            let _ = DuckDB::export("wikidata-20170821-subset.duckdb", subset.unwrap());
        }
    }
}
