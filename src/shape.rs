use std::collections::VecDeque;
use std::ops::Deref;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst};

pub(crate) trait Validate {
    fn validate(self) -> Expr;
    fn get_label(self) -> &'static str;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    WShape(WShape),
    WShapeRef(Box<WShapeRef>),
    WShapeComposite(WShapeComposite),
    WNodeConstraint(WNodeConstraint),
}

impl Shape {
    pub fn iter(self) -> ShapeIterator {
        ShapeIterator {
            shape: self,
            curr: vec![],
            next: vec![],
        }
    }
}

#[derive(Clone)]
pub struct ShapeIterator {
    shape: Shape,
    curr: Vec<Shape>,
    next: Vec<Shape>,
}

impl Iterator for ShapeIterator {
    type Item = Vec<Shape>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut nodes = VecDeque::new(); // We create a queue of nodes
        let mut leaves = Vec::new(); // We create a list of leaves

        nodes.push_front(self.shape.to_owned()); // We add the root node to the queue

        // Iterate over the nodes in the tree using a queue
        while let Some(node) = nodes.pop_front() {
            match &node {
                Shape::WShape(_) => leaves.push(node),
                Shape::WShapeRef(_) => leaves.push(node),
                Shape::WNodeConstraint(_) => leaves.push(node),
                Shape::WShapeComposite(shape) => {
                    if shape.is_subset(&self.curr) {
                        leaves.push(node);
                    } else {
                        for child in &shape.shapes {
                            nodes.push_back(child.to_owned());
                        }
                    }
                }
            }
        }

        self.next = leaves.to_vec();
        if self.curr.contains(&&self.shape) {
            None
        } else {
            self.curr = self.next.to_vec();
            Some(self.next.to_vec())
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct WShape {
    label: &'static str,
    property_id: i32,
    dst: i32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WShapeRef {
    label: &'static str,
    property_id: i32,
    dst: Shape,
}

#[derive(Clone, Debug, PartialEq)]
pub struct WShapeComposite {
    label: &'static str,
    shapes: Vec<Shape>,
}

#[derive(Clone, Debug, PartialEq)]
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

impl From<WShape> for Shape {
    fn from(value: WShape) -> Self {
        Shape::WShape(value)
    }
}

impl Validate for WShape {
    fn validate(self) -> Expr {
        when(Column::edge(Dst).eq(lit(self.dst))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id)))
        )
            .then(lit(self.label))
            .otherwise(lit(NULL))
    }

    fn get_label(self) -> &'static str {
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

impl From<WShapeRef> for Shape {
    fn from(value: WShapeRef) -> Self {
        Shape::WShapeRef(Box::from(value))
    }
}

impl Validate for WShapeRef {
    fn validate(self) -> Expr {
        match self.dst { // TODO: can this be improved?
            Shape::WShape(shape) => {
                when(Column::edge(Dst).eq(shape.validate()))
                    .then(lit(self.label))
                    .otherwise(lit(NULL))
            }
            Shape::WShapeRef(shape) => {
                let unboxed_shape = shape.deref();
                when(Column::edge(Dst).eq(unboxed_shape.clone().validate()))
                    .then(lit(self.label))
                    .otherwise(lit(NULL))
            }
            Shape::WShapeComposite(shape) => {
                when(Column::edge(Dst).eq(shape.validate()))
                    .then(lit(self.label))
                    .otherwise(lit(NULL))
            }
            Shape::WNodeConstraint(shape) => {
                when(Column::edge(Dst).eq(shape.validate()))
                    .then(lit(self.label))
                    .otherwise(lit(NULL))}
        }
    }

    fn get_label(self) -> &'static str {
        self.label
    }
}

impl WShapeComposite {
    pub fn new(label: &'static str, shapes: Vec<Shape>) -> Self {
        Self { label, shapes }
    }

    fn is_subset(&self, set: &Vec<Shape>) -> bool {
        if set.len() < self.shapes.len() { // A smaller set cannot contain a bigger set
            return false; // We return false
        }
        for shape in self.shapes.iter() { // We iterate over the shapes in the set
            if !set.contains(&shape) { // If the shape is not in the set
                return false; // It is not a subset
            }
        }
        true
    }
}

impl From<WShapeComposite> for Shape {
    fn from(value: WShapeComposite) -> Self {
        Shape::WShapeComposite(value)
    }
}

impl Validate for WShapeComposite {
    fn validate(self) -> Expr {
        let mut ans = lit(NULL);

        self.shapes.into_iter().for_each(|shape| {
            let label = match shape {
                Shape::WShape(shape) => shape.get_label(),
                Shape::WShapeRef(shape) => shape.get_label(),
                Shape::WShapeComposite(shape) => shape.get_label(),
                Shape::WNodeConstraint(shape) => shape.get_label(),
            };
            ans = when(Column::msg(None).arr().contains(lit(label)))
                .then(lit(self.label))
                .otherwise(ans.to_owned());
        });

        ans
    }

    fn get_label(self) -> &'static str {
        self.label
    }
}

impl Validate for WNodeConstraint {
    fn validate(self) -> Expr {
        todo!()
    }

    fn get_label(self) -> &'static str {
        "WNodeConstraint"
    }
}