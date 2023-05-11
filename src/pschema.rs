use crate::shape::{Shape, ShapeIterator, Validate};
use crate::shape::Shape::{WShape, WShapeComposite, WShapeRef};

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
        let mut ans = lit("");
        if let Some(nodes) = iterator.next() {
            for node in nodes {
                ans = match node {
                    WShape(shape) => match concat_list([ans.to_owned(), shape.validate()]) {
                        Ok(concat) => concat,
                        Err(_) => ans,
                    }
                    WShapeRef(shape) => match concat_list([ans.to_owned(), shape.validate()]) {
                        Ok(concat) => concat,
                        Err(_) => ans,
                    }
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
    use polars::df;
    use polars::prelude::*;
    use pregel_rs::graph_frame::GraphFrame;
    use pregel_rs::pregel::Column;
    use crate::id::Id;
    use crate::pschema::PSchema;
    use crate::pschema::tests::TestEntity::*;
    use crate::shape::Shape;
    use crate::shape::{WShape, WShapeComposite};

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
        TogetherWith,
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
                TogetherWith => Id::from("P170"),
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
                CERN
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
                AwardReceived
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
                Award
            ]
            .iter()
            .map(TestEntity::id)
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
                WShape::new("IsHuman", InstanceOf.id(), Human.id()),
                WShape::new("BirthLondon", BirthPlace.id(), London.id()),
                // TODO: include the date :(
            ]
                .into_iter()
                .map(|x| x.into())
                .collect()
        ).into()
    }

    #[test]
    fn simple_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        let schema = simple_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(result) => {
                println!("Result: {}", result);
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }

    #[test]
    fn paper_test() -> Result<(), String> {
        let graph = match paper_graph() {
            Ok(graph) => graph,
            Err(error) => return Err(error),
        };

        println!("{:}", graph);

        let schema = paper_schema();

        match PSchema::new(schema).validate(graph) {
            Ok(result) => {
                println!("Result: {}", result);
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        }
    }
}
