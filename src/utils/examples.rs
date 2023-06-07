use crate::shape::shape::*;
use crate::utils::examples::Value::*;

use polars::df;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::Column;
use wikidata_rs::dtype::DataType;
use wikidata_rs::id::Id;

pub enum Value {
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

impl Value {
    pub fn id(&self) -> u32 {
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

pub fn paper_graph() -> Result<GraphFrame, String> {
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
        .map(Value::id)
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
        .map(Value::id)
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
        .map(Value::id)
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

pub fn simple_schema() -> Shape {
    TripleConstraint::new(1, InstanceOf.id(), Human.id()).into()
}

pub fn paper_schema() -> Shape {
    ShapeComposite::new(
        1,
        vec![
            TripleConstraint::new(2, InstanceOf.id(), Human.id()).into(),
            TripleConstraint::new(3, BirthPlace.id(), London.id()).into(),
            ShapeLiteral::new(4, BirthDate.id(), DataType::DateTime).into(),
        ],
    )
    .into()
}

pub fn complex_schema() -> Shape {
    ShapeComposite::new(
        1,
        vec![
            TripleConstraint::new(2, InstanceOf.id(), Human.id()).into(),
            ShapeReference::new(
                3,
                BirthPlace.id(),
                TripleConstraint::new(5, Country.id(), UnitedKingdom.id()).into(),
            )
            .into(),
            ShapeLiteral::new(4, BirthDate.id(), DataType::DateTime).into(),
        ],
    )
    .into()
}

pub fn reference_schema() -> Shape {
    ShapeReference::new(
        1,
        BirthPlace.id(),
        TripleConstraint::new(2, Country.id(), UnitedKingdom.id()).into(),
    )
    .into()
}

pub fn optional_schema() -> Shape {
    ShapeComposite::new(
        1,
        vec![
            TripleConstraint::new(2, InstanceOf.id(), Human.id()).into(),
            Cardinality::new(
                TripleConstraint::new(3, AwardReceived.id(), Award.id()).into(),
                Bound::Inclusive(0),
                Bound::Inclusive(1),
            )
            .into(),
        ],
    )
    .into()
}
