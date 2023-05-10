use ego_tree::{NodeRef, Tree};
use std::collections::VecDeque;

#[derive(Clone)]
pub struct SPTree<T> {
    tree: Tree<T>,
}

#[derive(Clone)]
pub struct SPTreeIterator<'a, T> {
    sp_tree: &'a SPTree<T>,
    curr: Vec<NodeRef<'a, T>>,
    next: Vec<NodeRef<'a, T>>,
}

impl<'a, T> SPTree<T> {
    pub fn new(tree: Tree<T>) -> Self {
        Self { tree }
    }

    pub fn iter(&'a self) -> SPTreeIterator<'a, T> {
        SPTreeIterator {
            sp_tree: self,
            curr: vec![],
            next: vec![],
        }
    }

    /// Uses iterative breadth-first search.
    fn leaves(&self, prev_leaves: Option<&Vec<NodeRef<T>>>) -> Vec<NodeRef<T>> {
        let mut nodes = VecDeque::new(); // We create a queue of nodes
        let mut leaves = Vec::new(); // We create a list of leaves

        nodes.push_front(self.tree.root()); // We add the root node to the queue

        // Iterate over the nodes in the tree using a queue
        while let Some(node) = nodes.pop_front() {
            // We get the next node from the queue
            if !node.has_children() {
                // In case of a leaf node
                leaves.push(node); // We add the node to the list of leaves
            } else {
                // In case of a non-leaf node
                if self.is_all_children_leaves(node, prev_leaves) {
                    // If all children are leaves
                    leaves.push(node); // We add the node to the list of leaves
                } else {
                    // If not all children are leaves
                    for child in node.children() {
                        // We add the children to the queue
                        nodes.push_back(child);
                    }
                }
            }
        }

        leaves
    }

    fn is_all_children_leaves(&self, node: NodeRef<T>, leaves: Option<&Vec<NodeRef<T>>>) -> bool {
        if let Some(leaves) = leaves {
            // If we have a list of leaves
            let mut children = node.children();
            for child in children.by_ref() {
                if !leaves.contains(&child) {
                    return false;
                }
            }
            return true;
        }
        false
    }
}

impl<'a, T> Iterator for SPTreeIterator<'a, T> {
    type Item = Vec<NodeRef<'a, T>>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next = self.sp_tree.leaves(if self.curr.is_empty() {
            None
        } else {
            Some(&self.curr)
        });

        if self.curr.contains(&self.sp_tree.tree.root()) {
            None
        } else {
            self.curr = self.next.to_vec();
            Some(self.next.to_vec())
        }
    }
}
