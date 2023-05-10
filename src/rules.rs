use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst, Id};

#[derive(Debug, Default, Clone)]
pub enum Shape {
    WShape(WShape),
    WShapeRef(WShapeRef),
    WNodeConstraint(WNodeConstraint),
}

pub struct WShape {
    label: &'static str,
    property_id: i32,
    dst: i32,
}

pub struct WShapeRef {
    label: &'static str,
    dst: i32,
    property_id: i32,
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
}

impl Validate for WShapeRef {
    fn validate(&self, children: Vec<&Shape>) -> Expr {
        let mut ans = lit(NULL);

        children.into_iter().for_each(|child| {
            ans = when(Column::msg(None).arr().contains(lit(child.label)))
                .then(lit(self.label))
                .otherwise(ans.to_owned());
        });

        ans
    }
}
