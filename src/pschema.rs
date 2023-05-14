use crate::shape::Shape::{WShape, WShapeComposite, WShapeLiteral, WShapeRef};
use crate::shape::{Shape, ShapeIterator, Validate};

use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::{Column, MessageReceiver, PregelBuilder};

pub struct PSchema {
    start: Shape,
}

impl PSchema {
    pub fn new(start: Shape) -> PSchema {
        Self { start }
    }

    pub fn validate(&self, graph: GraphFrame) -> Result<DataFrame, PolarsError> {
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
        let max_iterations = self.start.clone().iter().count() as u8; // maximum number of iterations
        let tree_send_messages = self.start.clone(); // binding to avoid borrow checker error
        let mut send_messages_iter = tree_send_messages.iter(); // iterator to send messages
        let tree_v_prog = self.start.clone(); // binding to avoid borrow checker error
        let mut v_prog_iter = tree_v_prog.iter(); // iterator to update vertices
        v_prog_iter.next(); // skip the leaf nodes :D
                            // Then, we can define the algorithm that will be executed on the graph. The algorithm
                            // will be executed in parallel on all vertices of the graph.
        let pregel = PregelBuilder::new(graph)
            .max_iterations(if max_iterations > 1 {
                // This is a Theorem :D
                max_iterations - 1
            } else {
                1
            })
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
        let mut ans = lit(NULL);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    WShape(shape) => match concat_list([shape.validate(), ans.to_owned()]) {
                        Ok(concat) => concat,
                        Err(_) => ans,
                    },
                    WShapeRef(shape) => match concat_list([shape.validate(), ans.to_owned()]) {
                        Ok(concat) => concat,
                        Err(_) => ans,
                    },
                    WShapeLiteral(shape) => match concat_list([shape.validate(), ans.to_owned()]) {
                        Ok(concat) => concat,
                        Err(_) => ans,
                    },
                    _ => ans,
                }
            }
        }
        ans
    }

    fn aggregate_messages() -> Expr {
        Column::msg(None).explode().drop_nulls()
    }

    fn v_prog(iterator: &mut ShapeIterator) -> Expr {
        let mut ans = Column::msg(None);
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                if let WShapeComposite(shape) = node {
                    ans = match concat_list([ans.to_owned(), shape.validate()]) {
                        Ok(concat) => concat,
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
    use crate::dtype::DataType;
    use crate::id::Id;
    use crate::pschema::tests::TestEntity::*;
    use crate::pschema::PSchema;
    use crate::shape::{Shape, WShapeLiteral};
    use crate::shape::{WShape, WShapeComposite};
    use polars::df;
    use polars::prelude::*;
    use pregel_rs::graph_frame::GraphFrame;
    use pregel_rs::pregel::Column;

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
        fn id(&self) -> u64 {
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
            u64::from(id)
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
            .map(u64::from)
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
        Shape::WShape(WShape::new("IsHuman", InstanceOf.id(), Human.id()))
    }

    fn paper_schema() -> Shape {
        WShapeComposite::new(
            "Researcher",
            vec![
                WShape::new("IsHuman", InstanceOf.id(), Human.id()).into(),
                WShape::new("BirthLondon", BirthPlace.id(), London.id()).into(),
                WShapeLiteral::new("BirthDate", BirthDate.id(), DataType::DateTime).into(),
            ],
        )
        .into()
    }

    #[test]
    fn simple_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let schema = simple_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(_) => Ok(()),
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn paper_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let schema = paper_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(_) => Ok(()),
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
