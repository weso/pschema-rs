use crate::rules::Rule;
use crate::sp_tree::{SPTree, SPTreeIterator};
use ego_tree::Tree;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};
use std::ops::Add;

pub struct PSchema {
    tree: SPTree<Rule>,
}

impl PSchema {
    pub fn new(tree: Tree<Rule>) -> PSchema {
        Self {
            tree: SPTree::new(tree),
        }
    }

    pub fn validate(&self, graph: GraphFrame) -> Result<DataFrame, PolarsError> {
        // First, we need to define the maximum number of iterations that will be executed by the
        // algorithm. In this case, we will execute the algorithm until the tree converges, so we
        // set the maximum number of iterations to the number of vertices in the tree.
        let max_iterations = self.tree.clone().iter().count() as u8; // maximum number of iterations
        let tree_send_messages = self.tree.clone(); // binding to avoid borrow checker error
        let mut send_messages_iter = tree_send_messages.iter(); // iterator to send messages
        let tree_v_prog = self.tree.clone(); // binding to avoid borrow checker error
        let mut v_prog_iter = tree_v_prog.iter(); // iterator to update vertices
        v_prog_iter.next(); // skip the leaf nodes :D
                            // Then, we can define the algorithm that will be executed on the graph. The algorithm
                            // will be executed in parallel on all vertices of the graph.
        let pregel = PregelBuilder::new(graph)
            .max_iterations(max_iterations - 1)
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

    fn send_messages(iterator: &mut SPTreeIterator<Rule>) -> Expr {
        let mut ans = lit(""); // TODO: can this be changed by NULL?
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                let rule = node.value();
                if node.children().count() == 0 {
                    // In case of leaf node
                    ans = ans.add(rule.validate()); // try to validate :D
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

    fn v_prog(iterator: &mut SPTreeIterator<Rule>) -> Expr {
        let mut ans = Column::msg(None);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                let rule = node.value();
                let children: Vec<&Rule> = node.children().map(|x| x.value()).collect();
                if node.children().count() == 0 {
                    // In case of leaf node
                    continue; // we don't need to validate leaf nodes
                }
                ans = match concat_list([ans.to_owned(), rule.validate_children(children)]) {
                    Ok(x) => x,
                    Err(_) => ans,
                }
            }
        }
        ans.arr().unique()
    }
}
