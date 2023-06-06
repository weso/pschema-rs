use crate::shape::Shape::{WShape, WShapeComposite, WShapeLiteral, WShapeRef};
use crate::shape::{Shape, ShapeTree, ShapeTreeItem, Validate};

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
pub struct PSchema {
    start: Shape,
}

/// This code implements a Pregel algorithm for graph processing using the
/// Polars library in Rust. The `PSchema` struct has methods to validate a graph and
/// run the Pregel algorithm on it. The `validate` method checks if the graph has
/// the required columns and if they are not empty. The Pregel algorithm is defined
/// using the `PregelBuilder` and its methods to specify the maximum number of
/// iterations, the vertex column, the initial message, the send messages function,
/// the aggregate messages function, and the vertex program function. The
/// `send_messages` function sends
impl PSchema {
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
    pub fn new(start: Shape) -> PSchema {
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
    pub fn validate(&self, graph: GraphFrame) -> PolarsResult<DataFrame> {
        // First, we check if the graph has the required columns. If the graph does not have the
        // required columns, we return an error. The required columns are:
        //  - src: the source vertex of the edge
        //  - dst: the destination vertex of the edge
        //  - property_id: the property id of the edge
        //  - dtype: the data type of the property
        // Then, for each column we check if the column is empty. If the column is empty, we return
        // an error.
        if graph.edges.schema().get_field("src").is_none() {
            return Err(PolarsError::SchemaFieldNotFound("src".into()));
        } else if graph.edges.column("src").unwrap().len() == 0 {
            return Err(PolarsError::NoData("src".into()));
        }
        if graph.edges.schema().get_field("dst").is_none() {
            return Err(PolarsError::SchemaFieldNotFound("dst".into()));
        } else if graph.edges.column("dst").unwrap().len() == 0 {
            return Err(PolarsError::NoData("dst".into()));
        }
        if graph.edges.schema().get_field("property_id").is_none() {
            return Err(PolarsError::SchemaFieldNotFound("property_id".into()));
        } else if graph.edges.column("property_id").unwrap().len() == 0 {
            return Err(PolarsError::NoData("property_id".into()));
        }
        if graph.edges.schema().get_field("dtype").is_none() {
            return Err(PolarsError::SchemaFieldNotFound("dtype".into()));
        } else if graph.edges.column("dtype").unwrap().len() == 0 {
            return Err(PolarsError::NoData("dtype".into()));
        }
        // First, we need to define the maximum number of iterations that will be executed by the
        // algorithm. In this case, we will execute the algorithm until the tree converges, so we
        // set the maximum number of iterations to the number of vertices in the tree.
        let shape_tree = ShapeTree::new(self.start.to_owned());
        let mut send_messages_iter = shape_tree.to_owned().into_iter(); // iterator to send messages
        let mut v_prog_iter = shape_tree.to_owned().into_iter(); // iterator to update vertices
        v_prog_iter.next(); // skip the leaf nodes :D
                            // Then, we can define the algorithm that will be executed on the graph. The algorithm
                            // will be executed in parallel on all vertices of the graph.
        let pregel = PregelBuilder::new(graph)
            .max_iterations(shape_tree.iterations())
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
            Ok(result) => result
                .lazy()
                .select(&[
                    col(Column::Id.as_ref()),
                    col(Column::Custom("labels").as_ref()),
                ])
                .filter(col("labels").is_not_null())
                .with_common_subplan_elimination(false)
                .with_streaming(true)
                .collect(),
            Err(error) => Err(error),
        }
    }

    /// The function returns a NULL value.
    ///
    /// Returns:
    ///
    /// The function `initial_message()` is returning a NULL value, represented by the
    /// `NULL` literal.
    fn initial_message() -> Expr {
        lit(NULL)
    }

    /// The function `send_messages` takes a mutable iterator of shape nodes and returns
    /// a concatenated expression of validated shapes.
    ///
    /// Arguments:
    ///
    /// * `iterator`: `iterator` is a mutable reference to a `ShapeIterator` object. It
    /// is used to iterate over a collection of nodes, where each node is a `WShape`,
    /// `WShapeRef`, or `WShapeLiteral`. The function `send_messages` validates each
    /// shape in the collection. To do so, what we validate are the leave nodes for each
    /// iteration of the algorithm.
    ///
    /// Returns:
    ///
    /// an expression (`Expr`) which is the result of concatenating the validation
    /// results of the shapes obtained from the `ShapeIterator`. If the concatenation
    /// fails, the function returns a NULL literal.
    fn send_messages(iterator: &mut dyn Iterator<Item = ShapeTreeItem>) -> Expr {
        let mut ans = lit(NULL);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    WShape(shape) => shape.validate(ans),
                    WShapeRef(shape) => shape.validate(ans),
                    WShapeLiteral(shape) => shape.validate(ans),
                    WShapeComposite(_) => ans,
                }
            }
        }
        ans
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
        Column::msg(None).filter(Column::msg(None).is_not_null())
    }

    /// The function takes a shape iterator, validates the shapes in it, concatenates
    /// the validation results, and returns a unique array.
    ///
    /// Arguments:
    ///
    /// * `iterator`: The `iterator` parameter is a mutable reference to a
    /// `ShapeIterator`. It is used to iterate over a collection of `WShape` nodes.
    ///
    /// Returns:
    ///
    /// The function `v_prog` returns an `Expr` which is the result of calling the
    /// `unique` method on an array created from the `ans` variable.
    fn v_prog(iterator: &mut dyn Iterator<Item = ShapeTreeItem>) -> Expr {
        let mut ans = Column::msg(None);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    WShape(_) => ans,
                    WShapeRef(_) => ans,
                    WShapeLiteral(_) => ans,
                    WShapeComposite(shape) => shape.validate(ans),
                }
            }
        }
        ans
    }
}

#[cfg(test)]
mod tests {
    use crate::pschema::tests::TestEntity::*;
    use crate::pschema::PSchema;
    use crate::shape::ShapeTree;
    use crate::shape::{Shape, WShape, WShapeComposite, WShapeLiteral, WShapeRef};
    use polars::df;
    use polars::prelude::*;
    use pregel_rs::graph_frame::GraphFrame;
    use pregel_rs::pregel::Column;
    use wikidata_rs::dtype::DataType;
    use wikidata_rs::id::Id;

    enum TestEntity {
        Human,
        TimBernersLee,
        VintCerf,
        InstanceOf,
        CERN,
        Award,
        Spain,
        Country,
        Employer,
        BirthPlace,
        BirthDate,
        London,
        AwardReceived,
        UnitedKingdom,
    }

    impl TestEntity {
        fn id(&self) -> u32 {
            let id = match self {
                Human => Id::from("Q5"),
                TimBernersLee => Id::from("Q80"),
                VintCerf => Id::from("Q92743"),
                InstanceOf => Id::from("P31"),
                CERN => Id::from("Q42944"),
                Award => Id::from("Q3320352"),
                Spain => Id::from("Q29"),
                Country => Id::from("P17"),
                Employer => Id::from("P108"),
                BirthPlace => Id::from("P19"),
                BirthDate => Id::from("P569"),
                London => Id::from("Q84"),
                AwardReceived => Id::from("P166"),
                UnitedKingdom => Id::from("Q145"),
            };
            u32::from(id)
        }
    }

    fn paper_graph() -> Result<GraphFrame, String> {
        let edges = match df![
            Column::Src.as_ref() => [
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
            .map(TestEntity::id)
            .collect::<Vec<_>>(),
            Column::Custom("property_id").as_ref() => [
                InstanceOf,
                BirthPlace,
                Country,
                Employer,
                AwardReceived,
                Country,
                InstanceOf,
                AwardReceived,
                BirthDate,
            ]
            .iter()
            .map(TestEntity::id)
            .collect::<Vec<_>>(),
            Column::Dst.as_ref() => [
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
            .map(TestEntity::id)
            .collect::<Vec<_>>(),
            Column::Custom("dtype").as_ref() => [
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::Entity,
                DataType::DateTime,
            ]
            .iter()
            .map(u8::from)
            .collect::<Vec<_>>(),
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        match GraphFrame::from_edges(edges) {
            Ok(graph) => Ok(graph),
            Err(_) => Err(String::from("Error creating the GraphFrame from edges")),
        }
    }

    fn simple_schema() -> Shape {
        Shape::WShape(WShape::new(1, InstanceOf.id(), Human.id()))
    }

    fn paper_schema() -> Shape {
        WShapeComposite::new(
            1,
            vec![
                WShape::new(2, InstanceOf.id(), Human.id()).into(),
                WShape::new(3, BirthPlace.id(), London.id()).into(),
                WShapeLiteral::new(4, BirthDate.id(), DataType::DateTime).into(),
            ],
        )
        .into()
    }

    fn complex_schema() -> Shape {
        WShapeComposite::new(
            1,
            vec![
                WShape::new(2, InstanceOf.id(), Human.id()).into(),
                WShapeRef::new(
                    3,
                    BirthPlace.id(),
                    Shape::from(WShape::new(5, Country.id(), UnitedKingdom.id())),
                )
                .into(),
                WShapeLiteral::new(4, BirthDate.id(), DataType::DateTime).into(),
            ],
        )
        .into()
    }

    fn reference_schema() -> Shape {
        WShapeRef::new(
            1,
            BirthPlace.id(),
            Shape::from(WShape::new(2, Country.id(), UnitedKingdom.id())),
        )
        .into()
    }

    fn test(expected: DataFrame, actual: DataFrame) -> Result<(), String> {
        let count = actual
            .lazy()
            .sort("id", Default::default())
            .select([col("labels").arr().lengths()])
            .collect()
            .unwrap();
        match count == expected {
            true => Ok(()),
            false => return Err(String::from("The DataFrames are not equals")),
        }
    }

    #[test]
    fn simple_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let expected = match DataFrame::new(vec![Series::new("labels", [1u32, 1u32])]) {
            Ok(expected) => expected,
            Err(_) => return Err(String::from("Error creating the expected DataFrame")),
        };

        match PSchema::new(simple_schema()).validate(graph) {
            Ok(actual) => test(expected, actual),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn paper_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let expected = match DataFrame::new(vec![Series::new("labels", [4u32, 1u32])]) {
            Ok(expected) => expected,
            Err(_) => return Err(String::from("Error creating the expected DataFrame")),
        };

        match PSchema::new(paper_schema()).validate(graph) {
            Ok(actual) => test(expected, actual),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn complex_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let expected = match DataFrame::new(vec![Series::new("labels", [4u32, 1u32])]) {
            Ok(expected) => expected,
            Err(_) => return Err(String::from("Error creating the expected DataFrame")),
        };

        println!("{}", ShapeTree::new(complex_schema()).into_iter().count());

        match PSchema::new(complex_schema()).validate(graph) {
            Ok(actual) => {
                println!("{}", actual);
                test(expected, actual)
            }
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn reference_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let expected = match DataFrame::new(vec![Series::new("labels", [1u32])]) {
            Ok(expected) => expected,
            Err(_) => return Err(String::from("Error creating the expected DataFrame")),
        };

        match PSchema::new(reference_schema()).validate(graph) {
            Ok(actual) => test(expected, actual),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn invalid_graph() -> Result<(), String> {
        let edges = match df![
            Column::Src.as_ref() => [
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
            .map(TestEntity::id)
            .collect::<Vec<_>>(),
            Column::Dst.as_ref() => [
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
            .map(TestEntity::id)
            .collect::<Vec<_>>(),
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let graph = match GraphFrame::from_edges(edges) {
            Ok(graph) => graph,
            Err(_) => return Err(String::from("Error creating the GraphFrame from edges")),
        };

        let schema = simple_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(_) => Err(String::from("An error should have occurred")),
            Err(_) => Ok(()),
        }
    }

    #[test]
    fn empty_graph() -> Result<(), String> {
        let vertices = match df![
            Column::Id.as_ref() => Series::default(),
        ] {
            Ok(vertices) => vertices,
            Err(_) => return Err(String::from("Error creating the vertices DataFrame")),
        };

        let edges = match df![
            Column::Src.as_ref() => Series::default(),
            Column::Custom("property_id").as_ref() => Series::default(),
            Column::Dst.as_ref() => Series::default(),
            Column::Custom("dtype").as_ref() => Series::default(),
        ] {
            Ok(edges) => edges,
            Err(_) => return Err(String::from("Error creating the edges DataFrame")),
        };

        let graph = match GraphFrame::new(vertices, edges) {
            Ok(graph) => graph,
            Err(_) => return Err(String::from("Error creating the GraphFrame from edges")),
        };

        let schema = simple_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(_) => Err(String::from("An error should have occurred")),
            Err(_) => Ok(()),
        }
    }
}
