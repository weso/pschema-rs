use polars::lazy::dsl::concat_list;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst, Id};
use wikidata_rs::dtype::DataType;

pub(crate) trait Validate {
    fn validate(self, prev: Expr) -> Expr;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    TripleConstraint(TripleConstraint),
    ShapeReference(Box<ShapeReference>),
    ShapeComposite(ShapeComposite),
    ShapeLiteral(ShapeLiteral),
    Cardinality(Box<Cardinality>),
}

#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Inclusive(u8),
    Exclusive(u8),
}

impl Shape {
    pub fn get_label(&self) -> u8 {
        match self {
            Shape::TripleConstraint(shape) => shape.label,
            Shape::ShapeReference(shape) => shape.label,
            Shape::ShapeComposite(shape) => shape.label,
            Shape::ShapeLiteral(shape) => shape.label,
            Shape::Cardinality(shape) => shape.shape.get_label(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TripleConstraint {
    label: u8,
    property_id: u32,
    dst: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShapeReference {
    label: u8,
    property_id: u32,
    reference: Shape,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShapeComposite {
    label: u8,
    shapes: Vec<Shape>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct ShapeLiteral {
    label: u8,
    property_id: u32,
    dtype: DataType,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Cardinality {
    shape: Shape,
    min: Bound,
    max: Bound,
}

impl TripleConstraint {
    pub fn new(label: u8, property_id: u32, dst: u32) -> Self {
        Self {
            label,
            property_id,
            dst,
        }
    }
}

impl From<TripleConstraint> for Shape {
    fn from(value: TripleConstraint) -> Self {
        Shape::TripleConstraint(value)
    }
}

impl Validate for TripleConstraint {
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::edge(Dst)
                .eq(lit(self.dst))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id))),
        )
        .then(lit(self.label))
        .otherwise(prev)
    }
}

impl ShapeReference {
    pub fn new(label: u8, property_id: u32, dst: Shape) -> Self {
        Self {
            label,
            property_id,
            reference: dst,
        }
    }

    pub fn get_reference(self) -> Shape {
        self.reference
    }
}

impl From<ShapeReference> for Shape {
    fn from(value: ShapeReference) -> Self {
        Shape::ShapeReference(Box::from(value))
    }
}

impl Validate for ShapeReference {
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::dst(Custom("labels"))
                .arr()
                .contains(lit(self.reference.get_label()))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id))),
        )
        .then(lit(self.label))
        .otherwise(prev)
    }
}

impl ShapeComposite {
    pub fn new(label: u8, shapes: Vec<Shape>) -> Self {
        Self { label, shapes }
    }

    pub fn get_shapes(&self) -> Vec<Shape> {
        self.shapes.to_vec()
    }
}

impl From<ShapeComposite> for Shape {
    fn from(value: ShapeComposite) -> Self {
        Shape::ShapeComposite(value)
    }
}

impl Validate for ShapeComposite {
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::msg(None)
                .explode()
                .is_in(lit(Series::from_vec(
                    "vprog",
                    self.shapes
                        .iter()
                        .map(|shape| shape.get_label())
                        .collect::<Vec<_>>(),
                )))
                .sum()
                .over([Id.as_ref()])
                .eq(lit(self.shapes.len() as u8)),
        )
        .then(match concat_list([lit(self.label), prev.to_owned()]) {
            Ok(concat) => concat,
            Err(_) => prev.to_owned(),
        })
        .otherwise(prev)
    }
}

impl ShapeLiteral {
    pub fn new(label: u8, property_id: u32, dtype: DataType) -> Self {
        Self {
            label,
            property_id,
            dtype,
        }
    }
}

impl Validate for ShapeLiteral {
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::edge(Custom("dtype"))
                .eq(self.dtype)
                .and(Column::edge(Dst).eq(Column::src(Id)))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id))),
        )
        .then(self.label)
        .otherwise(prev)
    }
}

impl From<ShapeLiteral> for Shape {
    fn from(value: ShapeLiteral) -> Self {
        Shape::ShapeLiteral(value)
    }
}

impl Cardinality {
    pub fn new(shape: Shape, min: Bound, max: Bound) -> Self {
        Self { shape, min, max }
    }

    pub fn get_shape(self) -> Shape {
        self.shape
    }
}

impl Validate for Cardinality {
    fn validate(self, prev: Expr) -> Expr {
        when(
            lit(true),
            // match self.min {
            //     Bound::Inclusive(min) => Column::msg(None)
            //         .explode()
            //         .filter(
            //             Column::msg(None)
            //                 .explode()
            //                 .eq(lit(self.to_owned().get_shape().get_label())),
            //         )
            //         .sum()
            //         .over([Id.as_ref()])
            //         .gt_eq(lit(min)),
            //     Bound::Exclusive(min) => Column::msg(None)
            //         .explode()
            //         .filter(
            //             Column::msg(None)
            //                 .explode()
            //                 .eq(lit(self.to_owned().get_shape().get_label())),
            //         )
            //         .sum()
            //         .over([Id.as_ref()])
            //         .gt(lit(min)),
            // }
            // .and(match self.max {
            //     Bound::Inclusive(max) => Column::msg(None)
            //         .explode()
            //         .filter(
            //             Column::msg(None)
            //                 .explode()
            //                 .eq(lit(self.to_owned().get_shape().get_label())),
            //         )
            //         .sum()
            //         .over([Id.as_ref()])
            //         .lt_eq(lit(max)),
            //     Bound::Exclusive(max) => Column::msg(None)
            //         .explode()
            //         .filter(
            //             Column::msg(None)
            //                 .explode()
            //                 .eq(lit(self.to_owned().get_shape().get_label())),
            //         )
            //         .sum()
            //         .over([Id.as_ref()])
            //         .lt(lit(max)),
            // }),
        )
        .then(
            match concat_list([lit(self.get_shape().get_label()), prev.to_owned()]) {
                Ok(concat) => concat,
                Err(_) => prev.to_owned(),
            },
        )
        .otherwise(prev)
    }
}

impl From<Cardinality> for Shape {
    fn from(value: Cardinality) -> Self {
        Shape::Cardinality(Box::from(value))
    }
}
