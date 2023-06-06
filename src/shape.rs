use polars::lazy::dsl::concat_list;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst, Id};
use std::collections::VecDeque;
use std::ops::Deref;
use wikidata_rs::dtype::DataType;

pub type ShapeTreeItem = Vec<Shape>;

/// The `Validate` trait defines a method `validate` that returns an `Expr`. This
/// trait is implemented by several structs in the code, and the `validate` method
/// is used to generate an expression that can be used to validate whether a given
/// shape is present in the graph. The `Expr` type is a representation of a logical
/// expression that can be evaluated against a DataFrame, and is used in this code
/// to generate Pregel messages that are sent between nodes in the graph.
pub(crate) trait Validate {
    fn validate(self, prev: Expr) -> Expr;
}

/// This code defines an enum called `Shape` that can hold four different variants:
/// `WShape`, `WShapeRef`, `WShapeComposite`, and `WShapeLiteral`. Each variant
/// corresponds to a different type of shape that can be used to validate a graph.
/// The `#[derive(Clone, Debug, PartialEq)]` macro is used to automatically generate
/// implementations of the `Clone`, `Debug`, and `PartialEq` traits for the `Shape`
/// enum. This allows instances of the `Shape` enum to be cloned, printed for
/// debugging purposes, and compared for equality using the `==` operator.
#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    WShape(WShape),
    WShapeRef(Box<WShapeRef>),
    WShapeComposite(WShapeComposite),
    WShapeLiteral(WShapeLiteral),
}

/// This code defines two methods for the `Shape` enum.
impl Shape {
    /// This function returns the label of a shape object.
    ///
    /// Returns:
    ///
    /// A reference to a static string (`u8`) is being returned. The specific
    /// string returned depends on the variant of the `Shape` enum that `self` matches
    /// with in the `match` statement.
    pub fn get_label(&self) -> u8 {
        match self {
            Shape::WShape(shape) => shape.label,
            Shape::WShapeRef(shape) => shape.label,
            Shape::WShapeComposite(shape) => shape.label,
            Shape::WShapeLiteral(shape) => shape.label,
        }
    }
}

/// The above code defines a struct called ShapeIterator with fields for a Shape, a
/// current vector, and a next vector. The way the iterator works is that it first
/// creates a queue of nodes in the graph, and then iterates over the nodes in the
/// queue. For each node, it checks whether the node is a leaf node or not. If the
/// node is a leaf node, then it is added to the next vector. If the node is not a
/// leaf node, then its children are added to the queue. Once all the nodes in the
/// queue have been visited, the next vector is returned as the next set of shapes
/// to be iterated over. The iterator also keeps track of the shapes that have
/// already been visited, and does not return them again. This can be understood as
/// a breadth-first search of the tree.
///
/// Properties:
///
/// * `shape`: The `shape` property is a variable of type `Shape` that holds the
/// current shape being iterated over in the `ShapeIterator`.
/// * `curr`: `curr` is a vector that stores the current set of shapes being
/// iterated over in the `ShapeIterator`. It is used to keep track of the shapes
/// that have already been visited during iteration.
/// * `next`: `next` is a vector of `Shape` objects that represents the next set of
/// shapes to be iterated over in the `ShapeIterator`. This vector is used to store
/// the shapes that will be returned by the iterator's `next()` method. As the
/// iterator progresses, the `next` vector
#[derive(Clone)]
pub struct ShapeTree {
    shapes: Vec<ShapeTreeItem>,
}

impl ShapeTree {
    /// The approach of the problem is to use Reverse Level Order Traversal and store all
    /// the levels in a 2D vector having each of the levels stored in a different row.
    /// The steps to follow are described below:
    ///
    /// 1. Create a vector `nodes` to store the nodes to be evaluated.
    /// 2. Create the shapes `vector` to store the different levels.
    /// 3. Push the `root` node, provided node, into the queue.
    /// 4. Iterate over the `nodes` until there's no value left:
    ///     4.1 Iterate over all the nodes that were there at the beginning of the iteration.
    ///     4.2 Take the first node in the queue and match it against its Enum
    ///         4.2.1 If it is a `WShape` => push it to the temporary vector for the current iteration
    ///         4.2.2 If it is a `WShapeRef` => push it to the temporary vector and enqueue its child
    ///         4.2.3 If it is a `WShapeComposite` => push it to the temporary vector and enqueue its children
    ///         4.2.4 If it is a `WShapeLiteral` => push it to the temporary vector for the current iteration
    ///     4.3 Push the temporary results into the `shapes` vector
    ///     4.4 Clear the temporary results.
    /// 5. Return the `shapes` vector in reverse order
    pub fn new(shape: Shape) -> Self {
        let mut nodes = VecDeque::new(); // We create a queue of nodes
        let mut shapes = Vec::<ShapeTreeItem>::new(); // We create the returning vector
        let mut temp = Vec::<Shape>::new(); // We create a temporal vector

        nodes.push_front(shape.to_owned()); // We add the root node to the queue

        // Iterate over the nodes in the tree using a queue
        while !nodes.is_empty() {
            for _ in 0..nodes.len() {
                match nodes.pop_front() {
                    Some(node) => match &node {
                        Shape::WShape(_) => temp.push(node),
                        Shape::WShapeRef(shape) => {
                            let dst: Shape = Shape::from(shape.deref().to_owned().dst);
                            temp.push(node);
                            nodes.push_back(dst);
                        }
                        Shape::WShapeComposite(shape) => {
                            temp.push(node.to_owned());
                            shape
                                .shapes
                                .iter()
                                .for_each(|shape| nodes.push_back(shape.to_owned()));
                        }
                        Shape::WShapeLiteral(_) => temp.push(node),
                    },
                    None => continue,
                }
            }
            shapes.push(temp.to_owned());
            temp.clear();
        }

        shapes.reverse();

        ShapeTree { shapes }
    }

    pub fn iterations(&self) -> u8 {
        if self.contains_nary() {
            self.shapes.len() as u8 - 1
        } else {
            self.shapes.len() as u8
        }
    }

    fn contains_nary(&self) -> bool {
        for shapes in self.shapes.iter() {
            for shape in shapes.iter() {
                match shape {
                    Shape::WShape(_) => false,
                    Shape::WShapeRef(_) => false,
                    Shape::WShapeComposite(_) => return true,
                    Shape::WShapeLiteral(_) => false,
                };
            }
        }

        false
    }
}

impl IntoIterator for ShapeTree {
    type Item = ShapeTreeItem;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.shapes.into_iter()
    }
}

/// The WShape struct contains a label, property ID, and destination ID.
///
/// Properties:
///
/// * `label`: A string slice that represents the label of the WShape struct.
/// * `property_id`: `property_id` is a field of type `u32` in the `WShape` struct.
/// It is used to store the property identifier associated with the `WShape` object.
/// * `dst`: `dst` is a field of type `u32` in the `WShape` struct. It represents the
///  destination ID of the `WShape` object.
#[derive(Clone, Debug, PartialEq)]
pub struct WShape {
    label: u8,
    property_id: u32,
    dst: u32,
}

/// The WShapeRef struct contains a label, property ID, and a Shape object.
///
/// Properties:
///
/// * `label`: A string slice that represents the label of the WShapeRef struct. It
/// is a static string reference, meaning it has a fixed lifetime and cannot be
/// modified.
/// * `property_id`: `property_id` is an unsigned 32-bit integer that represents the
/// identifier of a property associated with the `WShapeRef` struct.
/// * `dst`: `dst` is a field of type `Shape` in the `WShapeRef` struct. It
/// represents the destination shape that the `WShapeRef` refers to.
#[derive(Clone, Debug, PartialEq)]
pub struct WShapeRef {
    label: u8,
    property_id: u32,
    dst: Shape,
}

/// The `WShapeComposite` struct represents a composite shape made up of multiple
/// `Shape` objects, with a label for identification. It contains a label and a
/// vector of `Shape` objects. The `WShapeComposite` struct implements the `Shape`
/// trait, which allows it to be used in place of a `Shape` object. This is useful
/// because it allows for the creation of composite shapes that can be used in
/// place of individual shapes.
///
/// Properties:
///
/// * `label`: The `label` property is a string slice (`u8`) that
/// represents the label or name of the `WShapeComposite` struct. It is a static
/// string because it has a `'static` lifetime, meaning it will live for the entire
/// duration of the program.
/// * `shapes`: `shapes` is a vector that contains instances of the `Shape` struct.
/// It is a property of the `WShapeComposite` struct, which represents a composite
/// shape made up of multiple individual shapes. The `shapes` vector allows for the
/// storage and manipulation of these individual shapes within the composite shape
#[derive(Clone, Debug, PartialEq)]
pub struct WShapeComposite {
    label: u8,
    shapes: Vec<Shape>,
}

/// The WShapeLiteral struct represents a shape literal with a label, property ID,
/// and data type in Rust.
///
/// Properties:
///
/// * `label`: A string that represents the label of the W-shape literal.
/// * `property_id`: `property_id` is an unsigned 32-bit integer that represents the
/// unique identifier of a property in a W-shape literal. It is used to distinguish
/// between different properties in a W-shape literal.
/// * `dtype`: `dtype` is a field of type `DataType` in the `WShapeLiteral` struct.
/// It represents the data type of the property value. The `DataType` enum can have
/// different variants such as `String`, `Integer`, `Float`, `Boolean`, etc.
/// depending on the type of
#[derive(Clone, Debug, PartialEq)]
pub struct WShapeLiteral {
    label: u8,
    property_id: u32,
    dtype: DataType,
}

impl WShape {
    /// This is a constructor function that creates a new instance of a struct with a
    /// label, property ID, and destination.
    ///
    /// Arguments:
    ///
    /// * `label`: A string slice that represents the label of the edge.
    /// * `property_id`: The `property_id` parameter is an unsigned 64-bit integer that
    /// represents the ID of a property. It is used as a unique identifier for the
    /// property.
    /// * `dst`: `dst` is a `u32` variable that represents the destination node ID of a
    /// directed edge in a graph. In other words, it is the ID of the node that the edge
    /// is pointing to. This parameter is used in the `new` function to create a new
    /// instance of a struct
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the struct that it is defined in.
    /// The struct has three fields: `label` of type `u8`, `property_id` of
    /// type `u32`, and `dst` of type `u32`. The `new` function takes in values for
    /// these fields and returns an instance of the struct with those values.
    pub fn new(label: u8, property_id: u32, dst: u32) -> Self {
        Self {
            label,
            property_id,
            dst,
        }
    }
}

/// The `From` trait for the `WShape` enum ids implemented, allowing
/// it to be converted into a `Shape` enum. This means that a value of `WShape` can
/// be passed as an argument to a function that expects a `Shape` and Rust will
/// automatically convert it to a `Shape` using this implementation.
impl From<WShape> for Shape {
    fn from(value: WShape) -> Self {
        Shape::WShape(value)
    }
}

impl Validate for WShape {
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

impl WShapeRef {
    /// This function creates a new instance of a struct with a label, destination
    /// shape, and property ID.
    ///
    /// Arguments:
    ///
    /// * `label`: A string slice that represents the label of the edge.
    /// * `property_id`: `property_id` is an unsigned 32-bit integer that represents the
    /// ID of a property. It is used as a parameter in the `new` function to create a
    /// new instance of a struct.
    /// * `dst`: `dst` is a parameter of type `Shape` which represents the destination
    /// shape of a graph edge. In graph theory, an edge connects two vertices (or nodes)
    /// and is represented by a pair of vertices. The `dst` parameter specifies the
    /// vertex to which the edge is directed.
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the struct that it is defined in.
    /// The struct has three fields: `label` of type `u8`, `dst` of type
    /// `Shape`, and `property_id` of type `u32`. The `new` function takes in values for
    /// these fields and returns an instance of the struct with those values set.
    pub fn new(label: u8, property_id: u32, dst: Shape) -> Self {
        Self {
            label,
            dst,
            property_id,
        }
    }
}

/// The above code is implementing the `From` trait for the `Shape` enum, where it
/// converts a `WShapeRef` struct into a `Shape` enum variant called `WShapeRef`.
/// The `WShapeRef` struct is being wrapped inside a `Box` before being converted
/// into the `Shape` enum variant.
impl From<WShapeRef> for Shape {
    fn from(value: WShapeRef) -> Self {
        Shape::WShapeRef(Box::from(value))
    }
}

impl Validate for WShapeRef {
    /// The function takes a Shape and returns an Expr based on whether the validation
    /// of the Shape matches the Dst column.
    ///
    /// Returns:
    ///
    /// The function `validate` returns an expression (`Expr`) based on the match result
    /// of `self.dst`. The expression returned depends on the specific variant of
    /// `Shape` that `self.dst` matches with.
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::dst(Custom("labels"))
                .arr()
                .contains(lit(self.dst.get_label()))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id))),
        )
        .then(lit(self.label))
        .otherwise(prev)
    }
}

impl WShapeComposite {
    /// The function checks if a given vector of shapes is a subset of another vector of
    /// shapes.
    ///
    /// Arguments:
    ///
    /// * `label`: A string slice that represents the label of the object being created.
    /// * `shapes`: `shapes` is a vector of `Shape` objects that is passed as a
    /// parameter to the `new` method of a struct. It is used to initialize the `shapes`
    /// field of the struct. The `is_subset` method takes another vector of `Shape`
    /// objects as a parameter and checks
    ///
    /// Returns:
    ///
    /// The code snippet contains two functions. The `new` function returns a new
    /// instance of a struct that contains a label and a vector of shapes. The
    /// `is_subset` function returns a boolean value indicating whether the vector of
    /// shapes passed as an argument is a subset of the vector of shapes contained in
    /// the struct instance. If the argument vector is smaller than the vector in the
    /// struct instance, the function returns
    pub fn new(label: u8, shapes: Vec<Shape>) -> Self {
        Self { label, shapes }
    }
}

/// The `From` trait for the `Shape` enum is implemented for the `WShapeComposite` struct,
/// specifically for the `WShapeComposite` variant. This allows instances of
/// `WShapeComposite` to be converted into `Shape` instances using the `into()`
/// method.
impl From<WShapeComposite> for Shape {
    fn from(value: WShapeComposite) -> Self {
        Shape::WShapeComposite(value)
    }
}

impl Validate for WShapeComposite {
    /// The function takes a label and a list of shapes, and returns an expression that
    /// checks if the label is in the list of shape labels.
    ///
    /// Returns:
    ///
    /// The `validate` function returns an `Expr` object.
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

impl WShapeLiteral {
    /// The function creates a new instance of a struct with a label, property ID, and
    /// data type.
    ///
    /// Arguments:
    ///
    /// * `label`: A string slice that represents the label or name of the property.
    /// * `property_id`: property_id is an unsigned 32-bit integer that represents the
    /// unique identifier of a property. It is used to distinguish one property from
    /// another in a data structure or database.
    /// * `dtype`: `dtype` is a variable of type `DataType`. It is likely an enum that
    /// represents the data type of a property, such as `String`, `Integer`, `Boolean`,
    /// etc.
    ///
    /// Returns:
    ///
    /// It is not clear from the given code snippet what is being returned. This code
    /// snippet only shows the implementation of a `new` function for a struct, but it
    /// does not show any return statement.
    pub fn new(label: u8, property_id: u32, dtype: DataType) -> Self {
        Self {
            label,
            property_id,
            dtype,
        }
    }
}

impl Validate for WShapeLiteral {
    /// This is a Rust function that validates a certain condition and returns a
    /// corresponding expression.
    ///
    /// Returns:
    ///
    /// The `validate` function is returning an expression (`Expr`) that represents a
    /// conditional statement using the `when` function. The expression checks if a
    /// certain condition is true and returns a literal value (`self.label`) if it is,
    /// otherwise it returns a NULL value (`NULL`).
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

/// The above code is implementing the `From` trait for the `Shape` enum,
/// specifically for the variant `WShapeLiteral`. This allows a value of type
/// `WShapeLiteral` to be converted into a `Shape` enum variant using the `into()`
/// method.
impl From<WShapeLiteral> for Shape {
    fn from(value: WShapeLiteral) -> Self {
        Shape::WShapeLiteral(value)
    }
}
