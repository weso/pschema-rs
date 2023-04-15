use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::pschema::{PSchema, Rule, RuleType};

fn main() -> Result<(), String> {
    // Create a new Pregel-based schema validator
    let mut validator = PSchema::new();

    // Define validation rules
    let rule1 = Rule::new(31, 1000000530, 96, RuleType::Inclusive);

    validator.add_rule(rule1);

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/example.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => Ok(validator.validate(graph, 1)),
        Err(_) => Err(String::from("Cannot create a GraphFrame")),
    }
}
