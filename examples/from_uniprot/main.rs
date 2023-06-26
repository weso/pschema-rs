use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::backends::ntriples::NTriples;
use pschema_rs::backends::parquet::Parquet;
use pschema_rs::backends::Backend;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::shex::{ShapeAnd, ShapeReference, TripleConstraint};

fn main() -> Result<(), String> {
    // Define validation rules
    let start = ShapeAnd::new(
        "protein",
        vec![
            TripleConstraint::new(
                "IsProtein",
                "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                "<http://purl.uniprot.org/core/Protein>",
            )
            .into(),
            ShapeReference::new(
                "annotation",
                "<http://purl.uniprot.org/core/annotation>",
                TripleConstraint::new(
                    "IsGlycosylation",
                    "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>",
                    "<http://purl.uniprot.org/core/Glycosylation_Annotation>",
                )
                .into(),
            )
            .into(),
        ],
    )
    .into();

    // Load Wikidata entities
    let edges = NTriples::import("./examples/from_uniprot/uniprotkb_reviewed_viruses_10239.nt")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start).validate(graph) {
            Ok(subset) => Parquet::export("uniprotkb_reviewed_viruses_10239.parquet", subset),
            Err(error) => Err(error.to_string()),
        },
        Err(error) => Err(format!("Cannot create a GraphFrame: {}", error)),
    }
}
