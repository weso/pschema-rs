use crate::shape::shex::*;
use crate::utils::examples::Value::*;

use polars::df;
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pregel_rs::pregel::Column;
use wikidata_rs::id::Id;

/// The `pub enum Value` block defines an enumeration of values that correspond to
/// Wikidata IDs for various entities and properties. Each value is associated with
/// a specific Wikidata ID using the `id()` method defined in the same block. These
/// values are used in other functions to create DataFrames and Shapes that
/// reference these Wikidata IDs.
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
    ScienceAward,
    DateTime,
}

/// The `impl Value` block defines a method called `id` for the `Value` enum. This
/// method returns a `u32` value that corresponds to the Wikidata ID of the enum
/// variant. The method uses a `match` statement to match each enum variant to its
/// corresponding Wikidata ID, which is then converted to a `u32` using the `from`
/// method. This method is used in other functions to create DataFrames and Shapes
/// that reference these Wikidata IDs.
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
            ScienceAward => Id::from("Q11448906"),
            DateTime => Id::from("@DateTime"),
        };
        u32::from(id)
    }
}

/// This function creates a graph frame representing a paper graph with nodes and
/// edges.
///
/// Returns:
///
/// a `Result` type with either a `GraphFrame` if the creation of the graph is
/// successful or a `String` with an error message if there was an error creating
/// the graph.
pub fn paper_graph() -> Result<GraphFrame, String> {
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
            Award,
        ]
        .iter()
        .map(Value::id)
        .collect::<Vec<_>>(),
        Column::Predicate.as_ref() => [
            InstanceOf,
            BirthPlace,
            Country,
            Employer,
            AwardReceived,
            Country,
            InstanceOf,
            AwardReceived,
            BirthDate,
            InstanceOf,
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
            DateTime,
            ScienceAward,
        ]
        .iter()
        .map(Value::id)
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

/// This function creates a simple schema for a triple constraint in Rust
/// programming language.
///
/// Returns:
///
/// A `Shape` object is being returned. The `simple_schema` function creates a
/// `TripleConstraint` object that specifies that an instance must have a single
/// value for the `rdf:type` property that is equal to the `Human` class. This
/// `TripleConstraint` object is then converted into a `Shape` object using the
/// `into()` method.
pub fn simple_schema() -> Shape<u32> {
    TripleConstraint::new("IsHuman", InstanceOf.id(), Human.id()).into()
}

/// This Rust function creates a composite shape for a paper schema with triple
/// constraints and a shape literal.
///
/// Returns:
///
/// The function `paper_schema()` returns a `Shape` object that represents a
/// composite shape with three constraints. The first constraint requires that the
/// subject is an instance of the `Human` class. The second constraint requires that
/// the subject has a `BirthPlace` property with a value of `London`. The third
/// constraint requires that the subject has a `BirthDate` property with a value of
/// type `DateTime
pub fn paper_schema() -> Shape<u32> {
    ShapeAnd::new(
        "Researcher",
        vec![
            TripleConstraint::new("Human", InstanceOf.id(), Human.id()).into(),
            TripleConstraint::new("London", BirthPlace.id(), London.id()).into(),
            TripleConstraint::new("DateTime", BirthDate.id(), DateTime.id()).into(),
        ],
    )
    .into()
}

/// The function returns a complex schema for a shape that includes constraints on
/// instance types, references to other shapes, and a literal data type.
///
/// Returns:
///
/// The function `complex_schema()` returns a `Shape` object that represents a
/// complex schema. The schema consists of a composite shape with three components:
/// a triple constraint that specifies that an instance must be of type `Human`, a
/// shape reference that references the `BirthPlace` shape and specifies that the
/// country must be `UnitedKingdom`, and a shape literal that specifies the data
/// type of the `Birth
pub fn complex_schema() -> Shape<u32> {
    ShapeAnd::new(
        "Researcher",
        vec![
            TripleConstraint::new("IsHuman", InstanceOf.id(), Human.id()).into(),
            ShapeReference::new(
                "BirthUnitedKingdom",
                BirthPlace.id(),
                TripleConstraint::new("UnitedKingdom", Country.id(), UnitedKingdom.id()).into(),
            )
            .into(),
            TripleConstraint::new("DateTime", BirthDate.id(), DateTime.id()).into(),
        ],
    )
    .into()
}

/// This function returns a reference schema for an employer and an award received,
/// with a constraint that the award must be an instance of a science award.
///
/// Returns:
///
/// a Shape object that represents a reference schema. The schema includes a
/// ShapeReference with an ID of 1, which references the Employer shape. The
/// Employer shape is then nested within another ShapeReference with an ID of 2,
/// which references the AwardReceived shape. The AwardReceived shape is then
/// constrained to instances of the ScienceAward shape using a TripleConstraint with
/// an ID of 3
pub fn reference_schema() -> Shape<u32> {
    ShapeReference::new(
        "EmployerScienceAward",
        Employer.id(),
        ShapeReference::new(
            "AwardReceivedScienceAward",
            AwardReceived.id(),
            TripleConstraint::new("ScienceAward", InstanceOf.id(), ScienceAward.id()).into(),
        )
        .into(),
    )
    .into()
}

/// This function returns a ShapeComposite representing an optional schema for a
/// human with an optional award received.
///
/// Returns:
///
/// A Shape object is being returned. Specifically, a ShapeComposite object that
/// contains two TripleConstraint objects and a Cardinality object. The
/// ShapeComposite object has an ID of 1 and the TripleConstraint objects have IDs
/// of 2 and 3 respectively. The first TripleConstraint object specifies that the
/// subject must be an instance of the Human class, while the second
/// TripleConstraint object specifies that the subject may have
pub fn optional_schema() -> Shape<u32> {
    ShapeAnd::new(
        "HumanAwardReceived",
        vec![
            TripleConstraint::new("IsHuman", InstanceOf.id(), Human.id()).into(),
            Cardinality::new(
                TripleConstraint::new("SomeAwardReceived", AwardReceived.id(), Award.id()).into(),
                Bound::Inclusive(0),
                Bound::Inclusive(1),
            )
            .into(),
        ],
    )
    .into()
}

pub fn conditional_schema() -> Shape<u32> {
    ShapeOr::new(
        "InstanceOf",
        vec![
            TripleConstraint::new("Human", InstanceOf.id(), Human.id()).into(),
            TripleConstraint::new("ScienceAward", InstanceOf.id(), ScienceAward.id()).into(),
        ],
    )
    .into()
}
