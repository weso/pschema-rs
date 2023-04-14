use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{ColumnIdentifier, MessageReceiver, Pregel, PregelBuilder};

pub struct PSchema {
    rules: Vec<Rule>,
}

pub struct Rule {
    src: String,
    dst: String,
    property: String,
    rule_type: RuleType,
}

#[derive(Debug, PartialEq)]
pub enum RuleType {
    Inclusive,
    Exclusive,
}

enum Message {
    Validate,
    Checked,
    WaitFor,
}

enum ValidationState {
    Undefined,
    Pending,
    WaitingFor,
    Ok,
    Failed
}

impl PSchema {
    pub fn new() -> Self {
        Self { rules: vec![] }
    }

    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    pub fn validate(&self, graph: GraphFrame, max_iterations: u8) -> Vec<Entity> {
        let rule = self.rules.get(0).unwrap(); // TODO: this is just for testing purposes

        let pregel = PregelBuilder::new(graph)
            .max_iterations(max_iterations)
            .with_vertex_column(ColumnIdentifier::Custom("state".to_string()))
            .initial_message(lit(ValidationState::Undefined)) // we pass the Undefined state to all vertices
            .send_messages(
                MessageReceiver::Dst, lit(0)
            )
            .aggregate_messages(lit(0))
            .v_prog(lit(0))
            .build();

        println!("{:?}", pregel.run());

        vec![]
    }
}

impl Rule {
    pub fn new(src: &str, dst: &str, property: &str, rule_type: RuleType) -> Self {
        Self {
            src: src.to_string(),
            dst: dst.to_string(),
            property: property.to_string(),
            rule_type,
        }
    }
}