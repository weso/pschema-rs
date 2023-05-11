use crate::shape::{Shape, ShapeIterator, Validate};
use crate::shape::Shape::{WShape, WShapeComposite, WShapeRef};

use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};
use std::ops::Add;

pub struct PSchema {
    start: Shape,
}

impl PSchema {
    pub fn new(start: Shape) -> PSchema {
        Self { start }
    }

    pub fn validate(&self, graph: GraphFrame) -> Result<DataFrame, PolarsError> {
        // First, we need to define the maximum number of iterations that will be executed by the
        // algorithm. In this case, we will execute the algorithm until the tree converges, so we
        // set the maximum number of iterations to the number of vertices in the tree.
        let max_iterations = self.start.clone().iter().count() as u8; // maximum number of iterations
        let tree_send_messages = self.start.clone(); // binding to avoid borrow checker error
        let mut send_messages_iter = tree_send_messages.iter(); // iterator to send messages
        let tree_v_prog = self.start.clone(); // binding to avoid borrow checker error
        let mut v_prog_iter = tree_v_prog.iter(); // iterator to update vertices
        v_prog_iter.next(); // skip the leaf nodes :D
                            // Then, we can define the algorithm that will be executed on the graph. The algorithm
                            // will be executed in parallel on all vertices of the graph.
        let pregel = PregelBuilder::new(graph)
            .max_iterations(if max_iterations > 1 { max_iterations - 1 } else { 1 }) // This is a Theorem :D
            .with_vertex_column(Column::Custom("labels"))
            .initial_message(Self::initial_message())
            .send_messages_function(MessageReceiver::Src, || {
                Self::send_messages(send_messages_iter.by_ref())
            })
            .aggregate_messages_function(Self::aggregate_messages)
            .v_prog_function(|| Self::v_prog(v_prog_iter.by_ref()))
            .build();
        // Finally, we can run the algorithm and get the result. The result is a DataFrame
        // containing the labels of the vertices.
        match pregel.run() {
            Ok(result) => Ok(result),
            Err(error) => Err(error),
        }
    }

    fn initial_message() -> Expr {
        lit(NULL)
    }

    fn send_messages(iterator: &mut ShapeIterator) -> Expr {
        let mut ans = lit(""); // TODO: can this be changed by NULL?
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    WShape(shape) => ans.add(shape.validate()),
                    WShapeRef(shape) => ans.add(shape.validate()),
                    _ => ans,
                }
            }
        }
        ans
    }

    fn aggregate_messages() -> Expr {
        Column::msg(None)
            .filter(Column::msg(None).neq(lit(NULL)))
            .explode()
    }

    fn v_prog(iterator: &mut ShapeIterator) -> Expr {
        let mut ans = Column::msg(None);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                if let WShapeComposite(shape) = node {
                    ans = match concat_list([ans.to_owned(), shape.validate()]) {
                        Ok(x) => x,
                        Err(_) => ans,
                    }
                }
            }
        }
        ans.arr().unique()
    }
}

#[cfg(test)]
mod tests {
    use ego_tree::tree;
    use polars::df;
    use pregel_rs::graph_frame::GraphFrame;
    use pregel_rs::pregel::Column;

    fn paper_graph() -> Result<GraphFrame, String> {
        let edges = match df![
            Column::Src.as_ref() => ["Q80", "Q80", "Q84", "Q80", "Q80", "Q3320352", "Q92743", "Q92743", "Q42944"],
            Column::Custom("property_id").as_ref() => ["P31", "P19", "P17", "P108", "P166", "P17", "P31", "P166"],
            Column::Dst.as_ref() => ["Q5", "Q84", "Q145", "Q42944", "Q3320352", "Q29", "Q29", "Q5", "Q3320352"],
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        match GraphFrame::from_edges(edges) {
            Ok(graph) => Ok(graph),
            Err(_) => Err(String::from("Error creating the GraphFrame from edges")),
        }
    }

    fn paper_schema() {
        tree! {
            Rule::new("Researcher", 31, 31, 1000000571) => {
                Rule::new("Human", "Q80", 31, 1000000571),
                Rule::new("Place", 31, 31, 1000000571) => {
                    Rule::new("F", 31, 31, 1000000571),
                    Rule::new("G", 31, 31, 1000000571),
                },
                Rule::new("Country", 31, 31, 1000000571),
            }
        }
    }

    #[test]
    fn simple_test() {}

    #[test]
    fn paper_test() {}
}
