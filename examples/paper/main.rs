use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::ntriples::NTriples;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::NodeConstraint;
use pschema_rs::shape::shex::Shape;
use pschema_rs::shape::shex::TripleConstraint;

fn main() -> Result<(), String> {
    // Define validation rules
    let start: Shape<&str> = ShapeAnd::new(
        "Person",
        vec![
            TripleConstraint::new(
                "Date",
                "<http://example.org/dateOfBirth>",
                NodeConstraint::Any,
            )
            .into(),
            ShapeReference::new(
                "Place",
                "<http://example.org/placeOfBirth>"
                TripleConstraint::new(
                    "Country",
                    "<http://example.org/country>",
                    NodeConstraint::Any,
                )
                .into()
            )
            .into(),
            TripleConstraint::new(
                "Organization",
                "<http://example.org/employer>",
                NodeConstraint::Any,
            )
            .into(),
        ],
    )
    .into();

    // Load Wikidata entities
    let edges = NTriples::import("./examples/paper/paper.nt")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start).validate(graph) {
            Ok(mut result) => NTriples::export("paper-subset.nt", &mut result),
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
