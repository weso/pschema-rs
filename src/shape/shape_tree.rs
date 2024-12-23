use polars::prelude::Literal;

use crate::shape::shex::Shape;
use std::collections::VecDeque;

pub type ShapeTreeItem<T> = Vec<Shape<T>>;

/// The `ShapeTree` struct contains a vector of `ShapeTreeItem` objects.
///
/// Properties:
///
/// * `shapes`: `shapes` is a vector of `ShapeTreeItem` structs that represents the
/// collection of shapes in the `ShapeTree`. Each `ShapeTreeItem` struct contains
/// information about a single shape, such as its type, position, and size.
#[derive(Clone)]
pub struct ShapeTree<T: Literal + Clone> {
    shapes: Vec<ShapeTreeItem<T>>,
}

impl<T: Literal + Clone> ShapeTree<T> {
    /// The approach to the problem is using Reverse Level Order Traversal and storing all
    /// the levels in a 2D vector having each of the levels stored in a different row.
    /// The steps to follow are described below:
    ///
    /// 1. Create a vector `nodes` to store the nodes to be evaluated.
    /// 2. Create the shapes `vector` to store the different levels.
    /// 3. Push the `root` node, provided node, into the queue.
    /// 4. Iterate over the `nodes` until there's no value left:
    ///     4.1 Iterate over all the nodes that were there at the beginning of the iteration.
    ///     4.2 Take the first node in the queue and match it against its Enum
    ///         4.2.1 If it is a `TripleConstraint` => push it to the temporary vector for the current iteration
    ///         4.2.2 If it is a `ShapeReference` => push it to the temporary vector and enqueue its child
    ///         4.2.3 If it is a `ShapeComposite` => push it to the temporary vector and enqueue its children
    ///         4.2.4 If it is a `ShapeLiteral` => push it to the temporary vector for the current iteration
    ///         4.2.5 If it is a `NumericFacet` => push it to the temporary vector and enqueue its child
    ///     4.3 Push the temporary results into the `shapes` vector
    ///     4.4 Clear the temporary results.
    /// 5. Return the `shapes` vector in reverse order
    pub fn new(shape: Shape<T>) -> Self {
        let mut nodes = VecDeque::new(); // We create a queue of nodes
        let mut shapes = Vec::<ShapeTreeItem<T>>::new(); // We create the returning vector
        let mut temp = Vec::new(); // We create a temporal vector

        nodes.push_front(shape); // We add the root node to the queue

        // Iterate over the nodes in the tree using a queue
        while !nodes.is_empty() {
            for _ in 0..nodes.len() {
                match nodes.pop_front() {
                    Some(node) => match &node {
                        Shape::TripleConstraint(_) => temp.push(node),
                        Shape::ShapeReference(shape) => {
                            temp.push(node.clone());
                            nodes.push_back(shape.clone().get_reference());
                        }
                        Shape::ShapeAnd(shape) => {
                            temp.push(node.clone());
                            shape
                                .get_shapes()
                                .iter()
                                .for_each(|shape| nodes.push_back(shape.clone()));
                        }
                        Shape::ShapeOr(shape) => {
                            temp.push(node.clone());
                            shape
                                .get_shapes()
                                .iter()
                                .for_each(|shape| nodes.push_back(shape.clone()));
                        }
                        Shape::Cardinality(shape) => {
                            temp.push(node.clone());
                            nodes.push_back(shape.clone().get_shape());
                        }
                    },
                    None => continue,
                }
            }
            shapes.push(temp.clone());
            temp.clear();
        }

        shapes.reverse();

        ShapeTree { shapes }
    }

    /// The function returns the number of iterations needed to generate all possible
    /// combinations of shapes in a given object. This is a Theorem than can be seen
    /// in further detail in the paper associated with this project.
    ///
    /// Returns:
    ///
    /// The function `iterations` returns a `u8` value which represents the number of
    /// iterations required to generate all possible combinations of shapes in the
    /// `self` object. If the `self` object contains an n-ary shape, then the number of
    /// iterations is equal to the number of shapes minus one, otherwise it is equal to
    /// the number of shapes.
    pub fn iterations(self) -> u8 {
        self.into_iter().count() as u8
    }
}

impl<T: Literal + Clone> IntoIterator for ShapeTree<T> {
    type Item = ShapeTreeItem<T>;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.shapes.into_iter()
    }
}

#[cfg(test)]
pub mod tests {
    use crate::shape::shape_tree::ShapeTree;
    use crate::utils::examples::*;

    #[test]
    fn simple_schema_test() {
        assert_eq!(1, ShapeTree::new(simple_schema()).into_iter().count())
    }

    #[test]
    fn paper_schema_test() {
        assert_eq!(2, ShapeTree::new(paper_schema()).into_iter().count())
    }

    #[test]
    fn complex_schema_test() {
        assert_eq!(3, ShapeTree::new(complex_schema()).into_iter().count())
    }

    #[test]
    fn reference_schema_test() {
        assert_eq!(3, ShapeTree::new(reference_schema()).into_iter().count())
    }

    #[test]
    fn optional_schema_test() {
        assert_eq!(3, ShapeTree::new(optional_schema()).into_iter().count())
    }

    #[test]
    fn v_prog_to_vprog_schema_test() {
        assert_eq!(
            3,
            ShapeTree::new(vprog_to_vprog_schema()).into_iter().count()
        )
    }

    #[test]
    fn and_reference_schema_test() {
        assert_eq!(
            3,
            ShapeTree::new(and_reference_schema()).into_iter().count()
        )
    }
}
