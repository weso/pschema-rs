use polars::prelude::*;
use pregel_rs::pregel::Column;
use pregel_rs::pregel::Column::{Custom, Dst, Id};

#[derive(Debug, Default, Clone)]
pub struct Rule {
    identifier: &'static str,
    src: i32,
    dst: i32,
    property_id: i32,
}
impl Rule {
    pub fn new(identifier: &'static str, src: i32, dst: i32, property_id: i32) -> Self {
        Self {
            identifier,
            src,
            dst,
            property_id,
        }
    }

    pub fn validate(&self) -> Expr {
        when(
            Column::src(Id)
                .eq(lit(self.src))
                .and(Column::edge(Dst).eq(lit(self.dst)))
                .and(Column::edge(Custom("property_id")).eq(lit(self.property_id))),
        )
        .then(lit(self.identifier))
        .otherwise(lit(NULL))
    }

    pub fn validate_children(&self, children: Vec<&Rule>) -> Expr {
        let mut ans = lit(false);

        children.into_iter().for_each(|child| {
            ans = when(
                Column::msg(None)
                    .arr()
                    .contains(lit(child.identifier)),
            )
            .then(true)
            .otherwise(ans.to_owned());
        });

        when(ans.eq(true))
            .then(lit(self.identifier))
            .otherwise(lit(NULL))
    }
}
