use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::ntriples::NTriples;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::NodeConstraint;
use pschema_rs::shape::shex::Shape;
use pschema_rs::shape::shex::TripleConstraint;

fn main() -> Result<(), String> {
    // Define validation rules
    let start: Shape<&str> = Shape::TripleConstraint(TripleConstraint::new(
        "Actor2825",
        "<http://data.linkedmdb.org/resource/oddlinker/link_source>",
        NodeConstraint::Value("<http://data.linkedmdb.org/resource/actor/2825>"),
    ));

    // Load Wikidata entities
    let edges = NTriples::import("./examples/from_ntriples/linkedmdb-latest-dump.nt")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start).validate(graph) {
            Ok(mut result) => NTriples::export("linkedmdb-latest-subset.nt", &mut result),
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
