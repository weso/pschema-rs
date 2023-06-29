use crate::shape::shape_tree::{ShapeTree, ShapeTreeItem};
use crate::shape::shex::{Shape, Validate};
use crate::utils::check::check_field;

use polars::enable_string_cache;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};

/// The `PSchema` struct has a single field `start` of type `Shape`.
///
/// Properties:
///
/// * `start`: `start` is a property of the `PSchema` struct which is of type
/// `Shape`. It represents the starting shape of a particular schema or data
/// structure.
pub struct PSchema<T: Literal + Clone> {
    start: Shape<T>,
}

/// This code implements a Pregel algorithm for graph processing using the
/// Polars library in Rust. The `PSchema` struct has methods to validate a graph and
/// run the Pregel algorithm on it. The `validate` method checks if the graph has
/// the required columns and if they are not empty. The Pregel algorithm is defined
/// using the `PregelBuilder` and its methods to specify the maximum number of
/// iterations, the vertex column, the initial message, the send messages function,
/// the aggregate messages function, and the vertex program function. The
/// `send_messages` function sends
impl<T: Literal + Clone> PSchema<T> {
    /// This is a constructor function for a Rust struct called PSchema that takes a
    /// Shape parameter and returns a new instance of the struct.
    ///
    /// Arguments:
    ///
    /// * `start`: The `start` parameter is of type `Shape` and is used to initialize
    /// the `start` field of the `PSchema` struct. It represents the starting shape of
    /// the schema.
    ///
    /// Returns:
    ///
    /// A new instance of the `PSchema` struct with the `start` field set to the `start`
    /// parameter passed to the `new` function.
    pub fn new(start: Shape<T>) -> PSchema<T> {
        Self { start }
    }

    /// The function validates a graph and runs a Pregel algorithm on it to get the
    /// labels of the vertices. The objective here is to create a subgraph of the
    /// original graph that contains only the vertices that conform to a certain
    /// shape. The shape is defined by the `start` field of the `PSchema` struct.
    ///
    /// Arguments:
    ///
    /// * `graph`: A `GraphFrame` object representing the graph to be processed. It
    /// contains two `DataFrame` objects: `vertices` and `edges`. The `vertices`
    /// `DataFrame` contains information about the vertices in the graph, while the
    /// `edges` `DataFrame` contains information about the edges in the graph
    ///
    /// Returns:
    ///
    /// a `Result<DataFrame, PolarsError>`. If the function executes successfully,
    /// it returns an `Ok(DataFrame)` containing the labels of the vertices. If
    /// there is an error during execution, it returns an `Err(PolarsError)` with a
    /// description of the error.
    pub fn validate(self, graph: GraphFrame) -> PolarsResult<DataFrame> {
        enable_string_cache(true);
        // First, we check if the graph has the required columns. If the graph does not have the
        // required columns or in case they are empty, we return an error. The required columns are:
        //  - `subject`: the source vertex of the edge
        //  - `predicate`: the label identifying the edge
        //  - `object`: the label identifying the destination vertex
        check_field(&graph.edges, Column::Subject)?;
        check_field(&graph.edges, Column::Predicate)?;
        check_field(&graph.edges, Column::Object)?;
        // Secondly, we create two iterators for the nodes in the `Shape Expression` tree. The former
        // is used to validate those nodes that will be considered in the send messages phase, while
        // the latter is used during the phase where the vertices are updated.
        let start = self.start;
        let mut send_messages_iter = ShapeTree::new(start.to_owned()).into_iter(); // iterator to send messages
        let pregel = PregelBuilder::new(graph.to_owned())
            .max_iterations(ShapeTree::new(start).iterations())
            .with_vertex_column(Column::Custom("labels"))
            .initial_message(Self::initial_message())
            .send_messages_function(MessageReceiver::Subject, || {
                Self::send_messages(send_messages_iter.by_ref())
            })
            .aggregate_messages_function(Self::aggregate_messages)
            .v_prog_function(Self::v_prog)
            .build();
        // Finally, we can run the algorithm and get the result. The result is a DataFrame
        // containing the labels of the vertices.
        match pregel.run() {
            Ok(result) => result
                .lazy()
                .filter(
                    col(Column::Custom("labels").as_ref())
                        .list()
                        .lengths()
                        .gt(0),
                )
                .left_join(
                    graph.edges.lazy(),
                    Column::VertexId.as_ref(),
                    Column::Subject.as_ref(),
                )
                .select(&[
                    col(Column::VertexId.as_ref()).alias(Column::Subject.as_ref()),
                    col(Column::Predicate.as_ref()),
                    col(Column::Object.as_ref()),
                    col(Column::Custom("labels").as_ref()),
                ])
                .collect(),
            Err(error) => Err(error),
        }
    }

    fn initial_message() -> Expr {
        lit(NULL)
    }

    fn send_messages(iterator: &mut dyn Iterator<Item = ShapeTreeItem<T>>) -> Expr {
        let mut ans = lit(NULL);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    Shape::TripleConstraint(shape) => shape.validate(ans),
                    Shape::ShapeReference(shape) => shape.validate(ans),
                    Shape::ShapeAnd(shape) => shape.validate(ans),
                    Shape::ShapeOr(shape) => shape.validate(ans),
                    Shape::Cardinality(shape) => shape.validate(ans),
                }
            }
        }
        ans.cast(DataType::Categorical(None))
    }

    /// The function returns an expression that aggregates messages by exploding a
    /// column and dropping NULL values.
    ///
    /// Returns:
    ///
    /// The function `aggregate_messages()` returns an expression that selects the `msg`
    /// column from a DataFrame, explodes the column (i.e., creates a new row for each
    /// element in the column), and drops any rows that have NULL values in the
    /// resulting column.
    fn aggregate_messages() -> Expr {
        Column::msg(None).drop_nulls()
    }

    fn v_prog() -> Expr {
        Column::msg(None)
    }
}

#[cfg(test)]
mod tests {
    use crate::pschema::PSchema;
    use crate::shape::shex::Shape;
    use crate::utils::examples::Value::*;
    use crate::utils::examples::*;

    use polars::df;
    use polars::prelude::*;
    use pregel_rs::graph_frame::GraphFrame;
    use pregel_rs::pregel::Column;
    use pregel_rs::pregel::Column::*;

    fn assert(expected: DataFrame, actual: DataFrame) -> Result<(), String> {
        let count = actual
            .lazy()
            .groupby([Column::Subject.as_ref()])
            .agg([col("labels").first()])
            .sort(Column::Subject.as_ref(), Default::default())
            .select([col("labels").list().lengths()])
            .collect()
            .unwrap();
        println!("count: {:?}", count);
        match count == expected {
            true => Ok(()),
            false => return Err(String::from("The DataFrames are not equals")),
        }
    }

    fn test<T: Literal + Clone>(
        graph: Result<GraphFrame, String>,
        result: Vec<u32>,
        schema: Shape<T>,
    ) -> Result<(), String> {
        let graph = match graph {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };
        let expected = match DataFrame::new(vec![Series::new(Custom("labels").as_ref(), result)]) {
            Ok(expected) => expected,
            Err(_) => return Err(String::from("Error creating the expected DataFrame")),
        };
        match PSchema::new(schema).validate(graph) {
            Ok(actual) => {
                println!("actual: {:?}", actual);
                assert(expected, actual)
            }
            Err(error) => {
                println!("{}", error);
                Err(error.to_string())
            }
        }
    }

    #[test]
    fn simple_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32, 1u32], simple_schema())
    }

    #[test]
    fn paper_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], paper_schema())
    }

    #[test]
    fn complex_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], complex_schema())
    }

    #[test]
    fn reference_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], reference_schema())
    }

    #[test]
    fn optional_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], optional_schema())
    }

    #[test]
    fn conditional_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32, 1u32, 1u32], conditional_schema())
    }

    #[test]
    fn any_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32, 1u32, 1u32], any_schema())
    }

    #[test]
    fn cardinality_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], cardinality_schema())
    }

    #[test]
    fn vprog_to_vprog_test() -> Result<(), String> {
        test(paper_graph(), vec![1u32], vprog_to_vprog_schema())
    }

    #[test]
    fn invalid_graph() -> Result<(), String> {
        let edges = match df![
            Column::Subject.as_ref() => [
                TimBernersLee,
                TimBernersLee,
                London,
                TimBernersLee,
                TimBernersLee,
                Award,
                VintCerf,
                CERN,
                TimBernersLee,
            ]
            .iter()
            .map(Value::id)
            .collect::<Vec<_>>(),
            Column::Object.as_ref() => [
                Human,
                London,
                UnitedKingdom,
                CERN,
                Award,
                Spain,
                Human,
                Award,
                TimBernersLee,
            ]
            .iter()
            .map(Value::id)
            .collect::<Vec<_>>(),
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let graph = match GraphFrame::from_edges(edges) {
            Ok(graph) => graph,
            Err(_) => return Err(String::from("Error creating the GraphFrame from edges")),
        };

        match PSchema::new(simple_schema()).validate(graph) {
            Ok(_) => Err(String::from("An error should have occurred")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn empty_graph() -> Result<(), String> {
        let vertices = match df![
            Column::VertexId.as_ref() => Series::default(),
        ] {
            Ok(vertices) => vertices,
            Err(_) => return Err(String::from("Error creating the vertices DataFrame")),
        };

        let edges = match df![
            Column::Subject.as_ref() => Series::default(),
            Column::Predicate.as_ref() => Series::default(),
            Column::Object.as_ref() => Series::default(),
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let graph = match GraphFrame::new(vertices, edges) {
            Ok(graph) => graph,
            Err(_) => return Err(String::from("Error creating the GraphFrame from edges")),
        };

        match PSchema::new(simple_schema()).validate(graph) {
            Ok(_) => Err(String::from("An error should have occurred")),
            Err(_) => Ok(()),
        }
    }
}
