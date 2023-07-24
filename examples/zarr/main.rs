use std::time::Instant;

use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::ntriples::NTriples;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::{NodeConstraint, ShapeAnd, TripleConstraint};

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
    let shape = ShapeAnd::new(
        "CALM5_Image",
        vec![
            TripleConstraint::new(
                "HPA040725",
                "<http://purl.org/dc/terms/isPartOf>",
                NodeConstraint::Value("<http://www.proteinatlas.org/search/HPA040725>"),
            )
            .into(),
            TripleConstraint::new(
                "Image",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                NodeConstraint::Value("<http://www.wikidata.org/entity/Q478798>"),
            )
            .into(),
        ],
    )
    .into();

    // Load Wikidata entities
    let edges = NTriples::import("hpa_omero.nt")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => {
            let start = Instant::now();
            match PSchema::new(shape).validate(graph) {
                Ok(mut subset) => {
                    let duration = start.elapsed();
                    println!("Time elapsed in validate() is: {:?}", duration);
                    NTriples::export("hpa_omero-subset.nt", &mut subset)
                }
                Err(error) => Err(error.to_string()),
            }
        }
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
