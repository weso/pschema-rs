use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst, Id};

pub(crate) trait Validate {
    fn validate(&self) -> Expr;
    fn get_label(&self) -> &'static str;
}

#[derive(Debug, Default, Clone)]
pub enum Shape {
    WShape(WShape),
    WShapeRef(WShapeRef),
    WShapeComposite(WShapeComposite),
    WNodeConstraint(WNodeConstraint),
}

pub struct WShape {
    label: &'static str,
    property_id: i32,
    dst: i32,
}

pub struct WShapeRef {
    label: &'static str,
    property_id: i32,
    dst: Shape,
}

pub struct WShapeComposite {
    label: &'static str,
    shapes: Vec<Shape>,
}

pub enum WNodeConstraint {
    Empty,
    DataType,
    Entity,
}

impl WShape {
    pub fn new(label: &'static str, dst: i32, property_id: i32) -> Self {
        Self {
            label,
            dst,
            property_id,
        }
    }
}

impl Validate for WShape {
    fn validate(&self) -> Expr {
        when(Column::edge(Dst).eq(lit(self.dst))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id)))
        )
            .then(lit(self.label))
            .otherwise(lit(NULL))
    }

    fn get_label(&self) -> &'static str {
        self.label
    }
}

impl WShapeRef {
    pub fn new(label: &'static str, dst: Shape, property_id: i32) -> Self {
        Self {
            label,
            dst,
            property_id,
        }
    }
}

impl Validate for WShapeRef {
    fn validate(&self) -> Expr {
        when(Column::edge(Dst).eq(self.dst.validate()))
            .then(lit(self.label))
            .otherwise(lit(NULL))
    }

    fn get_label(&self) -> &'static str {
        self.label
    }
}

impl WShapeComposite {
    pub fn new(label: &'static str, shapes: Vec<Shape>) -> Self {
        Self { label, shapes }
    }
}

impl Validate for WShapeComposite {
    fn validate(&self) -> Expr {
        let mut ans = lit(NULL);

        self.shapes.into_iter().for_each(|shape| {
            ans = when(Column::msg(None).arr().contains(lit(shape.get_label())))
                .then(lit(self.label))
                .otherwise(ans.to_owned());
        });

        ans
    }

    fn get_label(&self) -> &'static str {
        self.label
    }
}
