use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{ColumnIdentifier, MessageReceiver, Pregel, PregelBuilder};

pub struct PSchema {
    rules: Vec<Rule>,
}

pub struct Rule {
    src: i32,
    dst: i32,
    property_id: i32,
    rule_type: RuleType,
}

#[derive(Debug, PartialEq)]
pub enum RuleType {
    Inclusive,
    Exclusive,
}

pub enum Message {
    Validate,
    Checked,
    WaitFor,
}

impl Literal for Message {
    fn lit(self) -> Expr {
        match self {
            Message::Validate => lit(0),
            Message::Checked => lit(1),
            Message::WaitFor => lit(2),
        }
    }
}

pub enum ValidationState {
    Undefined,
    Pending,
    WaitingFor,
    Ok,
    Failed,
}

impl Literal for ValidationState {
    fn lit(self) -> Expr {
        match self {
            ValidationState::Undefined => lit(0),
            ValidationState::Pending => lit(1),
            ValidationState::WaitingFor => lit(2),
            ValidationState::Ok => lit(3),
            ValidationState::Failed => lit(4),
        }
    }
}

impl PSchema {
    pub fn new() -> Self {
        Self { rules: vec![] }
    }

    pub fn add_rule(&mut self, rule: Rule) {
        self.rules.push(rule);
    }

    fn send_messages_src() -> Expr {
        when(col(ColumnIdentifier::Custom("state").as_ref())
            .eq(ValidationState::Pending.lit())
        ).then(Message::WaitFor.lit())
        .when(col(ColumnIdentifier::Custom("state").as_ref())
            .eq(ValidationState::WaitingFor.lit())
            .and(Pregel::dst(ColumnIdentifier::Custom("state"))
                .eq(ValidationState::Ok.lit())
            )
        ).then(Message::Checked.lit())
        .when(col(ColumnIdentifier::Custom("state").as_ref())
            .eq(ValidationState::WaitingFor.lit())
            .and(Pregel::dst(ColumnIdentifier::Custom("state"))
                .eq(ValidationState::Failed.lit())
            )
        ).then(Message::Checked.lit())
        .otherwise(Message::Checked.lit()) // TODO: what to do with otherwise?
    }

    fn send_messages_dst() -> Expr {
        when(col(ColumnIdentifier::Custom("state").as_ref())
            .eq(ValidationState::Pending.lit())
        ).then(Message::Validate.lit())
        .otherwise(Message::Checked.lit()) // TODO: what to do with otherwise?
    }

    fn agg_messages(active_rule: Rule) -> Expr {
        when(Pregel::msg(None).eq(Message::Validate.lit()))
            .then(
                active_rule.validate(
                    col(ColumnIdentifier::Id.as_ref()),
                    Pregel::edge(ColumnIdentifier::Dst),
                    col(ColumnIdentifier::Custom("property_id").as_ref()),
                )
            )
        .otherwise(lit(-1))
    }

    fn v_prog() -> Expr {
        when(
            Pregel::msg(None)
                .eq(ValidationState::Ok.lit())
                .or(Pregel::msg(None))
                .eq(ValidationState::Failed.lit())
                .and(col(ColumnIdentifier::Custom("state").as_ref())
                .eq(ValidationState::Undefined.lit()))
        ).then(Pregel::msg(None))
        .otherwise(lit(-1))
    }

    pub fn validate(&mut self, graph: GraphFrame, max_iterations: u8) {
        let rule = self.rules.pop().unwrap(); // TODO: this is just for testing purposes

        let pregel = PregelBuilder::new(graph)
            .max_iterations(max_iterations)
            .with_vertex_column(ColumnIdentifier::Custom("state"))
            .initial_message(lit(ValidationState::Undefined)) // we pass the Undefined state to all vertices
            .send_messages(MessageReceiver::Src, Self::send_messages_src())
            .send_messages(MessageReceiver::Dst, Self::send_messages_dst())
            .aggregate_messages(Self::agg_messages(rule))
            .v_prog(Self::v_prog())
            .build();

        let result = pregel.run().unwrap();

        println!( // TODO: this is just for testing purposes
            "{:?}",
            result
        );

        println!( // TODO: this is just for testing purposes
            "{:?}",
             result
                 .lazy()
                 .select(&[col("id"), col("state")])
                 .filter(col("state").eq(ValidationState::Ok.lit()))
                 .collect()
        );
    }
}

impl Default for PSchema {
    fn default() -> Self {
        Self::new()
    }
}

impl Rule {
    pub fn new(src: i32, dst: i32, property_id: i32, rule_type: RuleType) -> Self {
        Self { src,  dst, property_id, rule_type }
    }

    pub fn validate(self, src: Expr, dst: Expr, property_id: Expr) -> Expr {
        let predicate = src.eq(self.src.lit())
            .and(dst.eq(self.dst.lit()))
            .and(property_id.eq(self.property_id.lit()));

        match self.rule_type {
            RuleType::Inclusive => {
                when(predicate)
                    .then(lit(1))
                    .otherwise(lit(0))
            }
            RuleType::Exclusive => {
                when(predicate)
                    .then(lit(0))
                    .otherwise(lit(1))
            }
        }
    }
}
