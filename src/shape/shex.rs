use polars::lazy::dsl::concat_list;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Object, Predicate};

/// The above code is defining a trait named `Validate` with a single method
/// `validate`. This trait can be implemented by any type that wants to provide
/// validation functionality. The `validate` method takes in a parameter `prev` of
/// type `Expr` and returns an `Expr`. The implementation of this method will
/// perform some validation on the input `self` and return a modified `Expr` based
/// on the validation result. The `pub(crate)` keyword specifies that this trait is
/// only accessible within the current crate.
pub(crate) trait Validate {
    fn validate(self, prev: Expr) -> Expr;
}

#[derive(Clone, Debug, PartialEq)]
pub enum Shape {
    TripleConstraint(TripleConstraint),
    ShapeReference(Box<ShapeReference>),
    ShapeComposite(ShapeComposite),
    Cardinality(Box<Cardinality>),
}

/// The above code is defining an enumeration type `Bound` in Rust. The `Bound` type
/// has two variants: `Inclusive` and `Exclusive`, each of which takes a single `u8`
/// value as an argument. The `#[derive(Clone, Debug, PartialEq)]` attribute is used
/// to automatically generate implementations of the `Clone`, `Debug`, and
/// `PartialEq` traits for the `Bound` type.
#[derive(Clone, Debug, PartialEq)]
pub enum Bound {
    Inclusive(u8),
    Exclusive(u8),
}

/// The above code is implementing a method `get_label` for the `Shape` struct. This
/// method returns the label of the shape, which is determined by matching the type
/// of the shape and returning the label of the corresponding shape variant. If the
/// shape is of type `Cardinality`, the label of the underlying shape is returned.
impl Shape {
    /// This function returns the label of a given shape.
    ///
    /// Returns:
    ///
    /// The function `get_label` returns an unsigned 8-bit integer, which represents the
    /// label of a shape. The label is obtained by matching the shape with one of the
    /// five possible variants of the `Shape` enum, and then returning the label of the
    /// corresponding shape. If the shape is a `Cardinality` shape, the function
    /// recursively calls `get_label` on the inner shape to obtain its
    pub fn get_label(&self) -> u8 {
        match self {
            Shape::TripleConstraint(shape) => shape.label,
            Shape::ShapeReference(shape) => shape.label,
            Shape::ShapeComposite(shape) => shape.label,
            Shape::Cardinality(shape) => shape.shape.get_label(),
        }
    }
}

/// The `TripleConstraint` struct represents a constraint on a triple with a label,
/// property ID, and destination ID.
///
/// Properties:
///
/// * `label`: The label is a u8 type property that represents a label associated
/// with the triple constraint.
/// * `property_id`: `property_id` is a field in the `TripleConstraint` struct that
/// represents the identifier of the property that the constraint is applied to. It
/// is of type `u32`, which means it can hold an unsigned 32-bit integer value. This
/// field is used to specify the property that the constraint
/// * `dst`: `dst` stands for "destination" and is of type `u32`. It likely
/// represents the ID of the node that the triple constraint is pointing to.
#[derive(Clone, Debug, PartialEq)]
pub struct TripleConstraint {
    label: u8,
    predicate: u32,
    object: u32,
}

/// The `ShapeReference` struct contains a label, property ID, and a reference to a
/// `Shape` object.
///
/// Properties:
///
/// * `label`: The label is a u8 (unsigned 8-bit integer) that represents a unique
/// identifier for the ShapeReference object.
/// * `property_id`: `property_id` is a `u32` (unsigned 32-bit integer) that
/// represents the unique identifier of a property associated with the
/// `ShapeReference`. This identifier can be used to retrieve additional information
/// about the property from a database or other data source.
/// * `reference`: `reference` is a field of type `Shape` that is contained within
/// the `ShapeReference` struct. It is likely a reference to another instance of the
/// `Shape` struct.
#[derive(Clone, Debug, PartialEq)]
pub struct ShapeReference {
    label: u8,
    predicate: u32,
    reference: Shape,
}

/// The `ShapeComposite` struct represents a composite shape made up of multiple
/// `Shape` objects, with a label assigned to it.
///
/// Properties:
///
/// * `label`: The `label` property is a `u8` value that represents a label or
/// identifier for the `ShapeComposite` object.
/// * `shapes`: `shapes` is a vector of `Shape` objects that are part of the
/// `ShapeComposite`. It can hold any number of `Shape` objects and allows for easy
/// manipulation of the composite as a whole.
#[derive(Clone, Debug, PartialEq)]
pub struct ShapeComposite {
    label: u8,
    shapes: Vec<Shape>,
}

/// The `Cardinality` type represents the shape and bounds of a set or sequence.
///
/// Properties:
///
/// * `shape`: The `shape` property is a `Shape` enum that represents the shape of
/// the cardinality. It could be one of the following:
/// * `min`: `min` is a property of the `Cardinality` struct that represents the
/// minimum number of elements allowed in the associated `Shape`. It is of type
/// `Bound`, which is likely an enum that can represent either a specific integer
/// value or an unbounded value (e.g. `Bound::Finite
/// * `max`: `max` is a property of the `Cardinality` struct that represents the
/// maximum number of elements that can be contained within the shape defined by the
/// `shape` property. It is of type `Bound`, which is an enum that can either be
/// `Finite(usize)` to represent a specific number
#[derive(Clone, Debug, PartialEq)]
pub struct Cardinality {
    shape: Shape,
    min: Bound,
    max: Bound,
}

/// The above code is implementing a new function for the `TripleConstraint` struct
/// in Rust. The function takes in three parameters: `label` of type `u8`,
/// `property_id` of type `u32`, and `dst` of type `u32`. It creates a new instance
/// of the `TripleConstraint` struct with the given parameters and returns it.
impl TripleConstraint {
    /// This is a constructor function that creates a new instance of a struct with
    /// three fields: label, property_id, and dst.
    ///
    /// Arguments:
    ///
    /// * `label`: The label parameter is of type u8 and represents a label associated
    /// with the object being created. It could be used to identify or categorize the
    /// object in some way.
    /// * `property_id`: property_id is a 32-bit unsigned integer that represents the ID
    /// of a property. It is used as a parameter in the constructor of a struct or class
    /// to initialize the property_id field of the object being created.
    /// * `dst`: `dst` is a parameter of type `u32` which represents the destination
    /// node ID of a directed edge in a graph. This parameter is used in the `new`
    /// function to create a new instance of a struct that represents a directed edge in
    /// a graph.
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the struct that it belongs to.
    /// The struct is not specified in the code snippet provided, so it is not possible
    /// to determine the exact type being returned.
    pub fn new(label: u8, predicate: u32, object: u32) -> Self {
        Self {
            label,
            predicate,
            object,
        }
    }
}

/// The above code is implementing a conversion from a `TripleConstraint` struct to
/// a `Shape` enum using the `From` trait. It creates a new `Shape` enum variant
/// called `TripleConstraint` and assigns the value of the `TripleConstraint` struct
/// to it. This allows for easier conversion between the two types in Rust code.
impl From<TripleConstraint> for Shape {
    fn from(value: TripleConstraint) -> Self {
        Shape::TripleConstraint(value)
    }
}

/// The above code is implementing the `validate` function for the
/// `TripleConstraint` struct, which is a part of a larger Rust program. The
/// `validate` function takes in a `prev` expression and returns an expression that
/// represents the validation of the `TripleConstraint`.
impl Validate for TripleConstraint {
    /// This function validates an expression by checking if a certain condition is met
    /// and returning a value based on the result.
    ///
    /// Arguments:
    ///
    /// * `prev`: `prev` is an `Expr` object representing the previous expression in a
    /// chain of expressions. It is used in the `otherwise` clause of the `when`
    /// expression to return the previous expression if none of the conditions in the
    /// `when` expression are met.
    ///
    /// Returns:
    ///
    /// The `validate` function is returning an `Expr` object. The value of this
    /// expression depends on the result of the `when` function. If the condition
    /// specified in the `when` function is true, then the `then` function will return a
    /// `lit` expression with the value of `self.label`. Otherwise, the `otherwise`
    /// function will return the `prev` expression.
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::edge(Object)
                .eq(lit(self.object))
                .and(Column::edge(Predicate).eq(lit(self.predicate))),
        )
        .then(lit(self.label))
        .otherwise(prev)
    }
}

/// The above code is implementing a method for a struct called `ShapeReference`.
/// The `new` method takes in a `label` of type `u8`, a `property_id` of type `u32`,
/// and a `dst` of type `Shape`, and returns a new instance of `ShapeReference` with
/// those values. The `get_reference` method takes in `self` and returns the
/// `reference` field of the `ShapeReference` instance.
impl ShapeReference {
    /// This is a constructor function that creates a new instance of a struct with a
    /// label, property ID, and reference to a shape.
    ///
    /// Arguments:
    ///
    /// * `label`: The label is a u8 value that represents the type of the property. For
    /// example, a label of 0 could represent a string property, while a label of 1
    /// could represent a numeric property.
    /// * `property_id`: The `property_id` parameter is a 32-bit unsigned integer that
    /// represents the ID of a property associated with a shape. It is used as a unique
    /// identifier for the property.
    /// * `dst`: `dst` is a parameter of type `Shape` which represents the destination
    /// shape of a relationship. It is used in the context of creating a new
    /// relationship instance with the given `label`, `property_id`, and `dst`.
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the `Self` struct, which contains
    /// the `label`, `property_id`, and `reference` fields.
    pub fn new(label: u8, predicate: u32, reference: Shape) -> Self {
        Self {
            label,
            predicate,
            reference,
        }
    }

    /// This Rust function returns a Shape reference.
    ///
    /// Returns:
    ///
    /// A `Shape` object is being returned.
    pub fn get_reference(self) -> Shape {
        self.reference
    }
}

/// The above code is implementing the `From` trait for the `ShapeReference` struct,
/// which allows creating a `Shape` enum variant `ShapeReference` from a
/// `ShapeReference` struct. The `ShapeReference` struct is wrapped in a `Box` and
/// then converted to the `Shape` enum variant `ShapeReference`.
impl From<ShapeReference> for Shape {
    fn from(value: ShapeReference) -> Self {
        Shape::ShapeReference(Box::from(value))
    }
}

/// The above code is implementing the `validate` function for the `ShapeReference`
/// struct, which is a part of a larger codebase. The `validate` function takes in a
/// previous expression `prev` and returns an expression.
impl Validate for ShapeReference {
    /// This function validates an expression based on certain conditions and returns a
    /// new expression.
    ///
    /// Arguments:
    ///
    /// * `prev`: `prev` is an `Expr` object representing the previous value of a
    /// property. It is used in the `otherwise` clause of a `when` expression to return
    /// the previous value if the conditions in the `when` clause are not met.
    ///
    /// Returns:
    ///
    /// The `validate` function is returning an `Expr` object. The value of this
    /// expression depends on the result of the `when` function. If the condition
    /// specified in the `when` function is true, then the `then` function will return a
    /// `lit` expression with the value of `self.label`. Otherwise, the `otherwise`
    /// function will return the `prev` parameter that was passed
    fn validate(self, prev: Expr) -> Expr {
        when(
            Column::object(Custom("labels"))
                .list()
                .contains(lit(self.reference.get_label()))
                .and(Column::edge(Predicate).eq(lit(self.predicate))),
        )
        .then(lit(self.label))
        .otherwise(prev)
    }
}

/// This is an implementation of the `ShapeComposite` struct, which defines two
/// methods: `new` and `get_shapes`.
impl ShapeComposite {
    /// This is a constructor function that creates a new instance of a struct with a
    /// label and a vector of shapes.
    ///
    /// Arguments:
    ///
    /// * `label`: The `label` parameter is of type `u8` and represents a label or
    /// identifier for the group of shapes.
    /// * `shapes`: `shapes` is a vector of `Shape` objects. It is a parameter of the
    /// `new` function and is used to initialize the `shapes` field of the struct. The
    /// `shapes` field is a vector that holds all the shapes that belong to the object.
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the struct that it is defined in.
    /// The struct has two fields: `label` of type `u8` and `shapes` of type
    /// `Vec<Shape>`. The `Self` keyword refers to the struct itself, so the function is
    /// returning an instance of that struct with the specified `label` and `shapes`.
    pub fn new(label: u8, shapes: Vec<Shape>) -> Self {
        Self { label, shapes }
    }

    /// This function returns a vector of shapes.
    ///
    /// Returns:
    ///
    /// A vector of `Shape` objects is being returned. The `get_shapes` function is a
    /// method of some struct or class that has a field called `shapes`, which is a
    /// collection of `Shape` objects. The `to_vec` method is called on this collection
    /// to create a new vector containing the same `Shape` objects. This new vector is
    /// then returned by the function.
    pub fn get_shapes(&self) -> Vec<Shape> {
        self.shapes.to_vec()
    }
}

/// This is an implementation of the `From` trait for the `ShapeComposite` struct.
/// The `From` trait is a Rust language feature that allows for automatic conversion
/// between types. In this case, it allows a `ShapeComposite` object to be converted
/// into a `Shape` enum variant.
impl From<ShapeComposite> for Shape {
    fn from(value: ShapeComposite) -> Self {
        Shape::ShapeComposite(value)
    }
}

/// This is an implementation of the `Validate` trait for the `ShapeComposite`
/// struct. The `Validate` trait defines a method `validate` that takes an `Expr`
/// argument and returns an `Expr`. The purpose of this trait is to provide a way to
/// validate whether a given `Expr` satisfies certain conditions.
impl Validate for ShapeComposite {
    /// This function validates an expression by checking if all the labels in its
    /// shapes are present in a specific column and concatenating it with a previous
    /// expression if possible.
    ///
    /// Arguments:
    ///
    /// * `prev`: `prev` is an `Expr` object representing the previous expression in a
    /// chain of expressions. It is used in the `otherwise` method call at the end of
    /// the `validate` function to return the previous expression if the `when`
    /// condition is not satisfied.
    ///
    /// Returns:
    ///
    /// The `validate` function is returning an `Expr` object. The returned value is the
    /// result of a chained method call on a `when` expression. The `when` expression
    /// checks if all the labels of the shapes in `self.shapes` are in the
    /// `Column::msg(None)` list. If the condition is true, it concatenates `self.label`
    /// and `prev` using the `
    fn validate(self, prev: Expr) -> Expr {
        when(self.shapes.iter().fold(lit(true), |acc, shape| {
            acc.and(lit(shape.get_label()).is_in(Column::msg(None)))
        }))
        .then(match concat_list([lit(self.label), prev.to_owned()]) {
            Ok(concat) => concat,
            Err(_) => prev.to_owned(),
        })
        .otherwise(prev)
    }
}

/// This is an implementation of the `Cardinality` struct. It defines two methods:
/// `new` and `get_shape`.
impl Cardinality {
    /// This is a constructor function that creates a new instance of a struct with a
    /// given shape, minimum bound, and maximum bound.
    ///
    /// Arguments:
    ///
    /// * `shape`: The shape parameter is of type Shape and represents the geometric
    /// shape of an object. It could be a circle, rectangle, triangle, or any other
    /// shape that can be defined mathematically.
    /// * `min`: `min` is a parameter of type `Bound` that represents the minimum bounds
    /// of the shape. It is used in the constructor of a struct to create a new instance
    /// of the struct with the specified shape and minimum and maximum bounds.
    /// * `max`: `max` is a parameter of type `Bound` that represents the maximum bounds
    /// of the shape. It is used in the `new` function to create a new instance of the
    /// `Self` struct.
    ///
    /// Returns:
    ///
    /// The `new` function is returning an instance of the struct that it is defined in.
    /// The type of the returned value is `Self`, which in this case refers to the
    /// struct that the `new` function is defined in.
    pub fn new(shape: Shape, min: Bound, max: Bound) -> Self {
        Self { shape, min, max }
    }

    /// This Rust function returns the shape of an object.
    ///
    /// Returns:
    ///
    /// A `Shape` object is being returned.
    pub fn get_shape(self) -> Shape {
        self.shape
    }
}

/// This is an implementation of the `Validate` trait for the `Cardinality` struct.
/// The `Validate` trait defines a method `validate` that takes an `Expr` argument
/// and returns an `Expr`. The purpose of this trait is to provide a way to validate
/// whether a given `Expr` satisfies certain conditions.
impl Validate for Cardinality {
    /// The function validates an expression based on the minimum and maximum bounds of
    /// a column's label count.
    ///
    /// Arguments:
    ///
    /// * `prev`: `prev` is an `Expr` parameter representing the previous expression
    /// that was validated. It is used in the `otherwise` clause of the `when`
    /// expression to return the previous expression if the validation fails.
    ///
    /// Returns:
    ///
    /// The `validate` function is returning an `Expr` object.
    fn validate(self, prev: Expr) -> Expr {
        let count = Column::msg(None)
            .list()
            .eval(col("").eq(lit(self.shape.get_label())).cumsum(false), true)
            .list()
            .first();

        when(
            match self.min {
                Bound::Inclusive(min) => count.to_owned().gt_eq(lit(min)),
                Bound::Exclusive(min) => count.to_owned().gt(lit(min)),
            }
            .and(match self.max {
                Bound::Inclusive(max) => count.lt_eq(lit(max)),
                Bound::Exclusive(max) => count.lt(lit(max)),
            }),
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

/// This implementation allows a `Cardinality` struct to be converted into a `Shape`
/// enum variant using the `From` trait. It creates a new `Shape::Cardinality`
/// variant with the `Cardinality` struct wrapped in a `Box`. This allows for more
/// flexibility in working with `Shape` objects, as a `Cardinality` can be treated
/// as a `Shape` in certain contexts.
impl From<Cardinality> for Shape {
    fn from(value: Cardinality) -> Self {
        Shape::Cardinality(Box::from(value))
    }
}
