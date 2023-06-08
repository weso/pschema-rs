#![doc = include_str!("../README.md")]
/// `pub mod backends;` is creating a public module named `backends`. This module
/// contains code related to different backends or databases that the program
/// can use to store and retrieve data.
pub mod backends;
/// `pub mod pschema;` is creating a public module named `pschema`. This module
/// contains code related to creating knowledge graphs from Wikibase data.
pub mod pschema;
/// `pub mod shape;` is creating a public module named `shape`. This module
/// contains code related to defining and manipulating shapes or structures of data
/// in the codebase.
pub mod shape;
/// `pub mod utils;` is creating a public module named `utils`. This module contains
/// utility functions and helper code that can be used throughout the codebase.
pub mod utils;
