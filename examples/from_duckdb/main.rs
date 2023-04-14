use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::pschema::{PSchema, Rule, RuleType};

fn main() -> Result<(), String> {
    // Create a new Pregel-based schema validator
    let mut validator = PSchema::new();

    // Define validation rules
    let rule1 = Rule::new("instance_of", "Q5", "Q215627", RuleType::Inclusive);
    let rule2 = Rule::new(
        "country_of_citizenship",
        "Q30",
        "Q215627",
        RuleType::Inclusive,
    );
    let rule3 = Rule::new("gender", "Q6581097", "Q215627", RuleType::Exclusive);

    validator.add_rule(rule1);
    validator.add_rule(rule2);
    validator.add_rule(rule3);

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/example.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => Ok(validator.validate(graph, 10)),
        Err(_) => Err(String::from("Cannot create a GraphFrame")),
    }
}
