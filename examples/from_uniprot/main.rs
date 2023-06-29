use std::time::Instant;

use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::ntriples::NTriples;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::{NodeConstraint, ShapeAnd, ShapeReference, TripleConstraint};

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
        "protein",
        vec![
            TripleConstraint::new(
                "IsProtein",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                NodeConstraint::Value("<http://purl.uniprot.org/core/Protein>"),
            )
            .into(),
            ShapeReference::new(
                "annotation",
                "<http://purl.uniprot.org/core/annotation>",
                TripleConstraint::new(
                    "IsGlycosylation",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    NodeConstraint::Value(
                        "<http://purl.uniprot.org/core/Glycosylation_Annotation>",
                    ),
                )
                .into(),
            )
            .into(),
        ],
    )
    .into();

    // Load Wikidata entities
    let edges = NTriples::import("uniprotkb_reviewed_viruses_10239_0.nt")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => {
            let start = Instant::now();
            match PSchema::new(shape).validate(graph) {
                Ok(mut subset) => {
                    let duration = start.elapsed();
                    println!("Time elapsed in validate() is: {:?}", duration);
                    NTriples::export("uniprotkb_reviewed_viruses_10239_0-subset.nt", &mut subset)
                }
                Err(error) => Err(error.to_string()),
            }
        }
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
