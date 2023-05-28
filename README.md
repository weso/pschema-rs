# `pschema-rs`

[![CI](https://github.com/angelip2303/pschema-rs/actions/workflows/ci.yml/badge.svg)](https://github.com/angelip2303/pschema-rs/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/angelip2303/pschema-rs/branch/main/graph/badge.svg?token=jgwNdmIYhD)](https://codecov.io/gh/angelip2303/pschema-rs)
[![latest_version](https://img.shields.io/crates/v/pschema-rs)](https://crates.io/crates/pschema-rs)
[![documentation](https://img.shields.io/docsrs/pschema-rs/)](https://docs.rs/pschema-rs/0.0.1/pschema_rs/)

`pschema-rs` is a Rust library that provides a Pregel-based schema validation algorithm for generating subsets of data 
from Wikidata. It is designed to be efficient, scalable, and easy to use, making it suitable for a wide range of applications
that involve processing large amounts of data from Wikidata.

## Features

- **Pregel-based schema validation**: `pschema-rs` uses the Pregel model, a graph-based computation model, to perform 
schema validation on Wikidata entities. This allows for efficient and scalable processing of large datasets.

- **Rust implementation**: `pschema-rs` is implemented in Rust, a systems programming language known for its performance,
memory safety, and concurrency features. This ensures that the library is fast, reliable, and safe to use.

- **Wikidata subset generation**: `pschema-rs` provides functionality to generate subsets of data from Wikidata based on 
schema validation rules. This allows users to filter and extract relevant data from Wikidata based on their specific 
requirements.

- **Customizable validation rules**: `pschema-rs` allows users to define their own validation rules using a simple and 
flexible syntax. This makes it easy to customize the schema validation process according to the specific needs of a given
application.

- **Easy-to-use API**: `pschema-rs` provides a user-friendly API that makes it easy to integrate the library into any Rust
project. The API provides a high-level interface for performing schema validation and generating Wikidata subsets, with
comprehensive documentation and examples to help users get started quickly.

## Installation

To use `pschema-rs` in your Rust project, you can add it as a dependency in your `Cargo.toml` file:

```toml
[dependencies]
pschema = "0.0.1"
```

## Usage

Here's an example of how you can use `pschema-rs` to perform schema validation and generate a subset of data from Wikidata:

```rust
use polars::prelude::*;
use pregel_rs::graph_frame::GraphFrame;
use pschema_rs::duckdb_dump::DumpUtils;
use pschema_rs::id::Id;
use pschema_rs::pschema::PSchema;
use pschema_rs::shape::{Shape, WShape};

fn main() {
    // Define validation rules
    let start = Shape::WShape(
        WShape::new(
            "IsHuman",
            Id::from("P31").into(),
            Id::from("Q331769").into()
        )
    );

    // Load Wikidata entities
    let edges = DumpUtils::edges_from_duckdb("./examples/from_duckdb/example.duckdb")?;

    // Perform schema validation
    match GraphFrame::from_edges(edges) {
        Ok(graph) => match PSchema::new(start).validate(graph) {
            Ok(result) => {
                println!("Schema validation result:");
                println!(
                    "{:?}",
                    result
                        .lazy()
                        .select(&[col("id"), col("labels")])
                        .filter(col("labels").is_not_NULL())
                        .collect()
                );
                Ok(())
            }
            Err(error) => Err(error.to_string()),
        },
        Err(_) => Err(String::from("Cannot create a GraphFrame")),
    }
}
```

For more information on how to define validation rules, load entities from Wikidata, and process subsets of data, refer
to the documentation.

## Related projects

1. [wdsub](https://github.com/weso/wdsub) is an application for generating Wikidata subsets written in Scala.
2. [pschema](https://github.com/weso/pschema) is a Scala-based library which is equivalent to this.

## License

Copyright &copy; 2023 Ángel Iglesias Préstamo (<angel.iglesias.prestamo@gmail.com>)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU General Public License for more details.

You should have received a copy of the GNU General Public License
along with this program.  If not, see <https://www.gnu.org/licenses/>.

**By contributing to this project, you agree to release your
contributions under the same license.**
