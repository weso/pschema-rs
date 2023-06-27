use std::fs::File;

use polars::prelude::*;

use super::Backend;

pub struct Parquet;

/// The `Parquet` block defines a Rust module that contains `import` and `export`.
impl Backend for Parquet {
    fn import(_path: &str) -> Result<DataFrame, String> {
        todo!()
    }

    fn export(path: &str, mut df: &mut DataFrame) -> Result<(), String> {
        let buffer = match File::create(path) {
            Ok(buffer) => buffer,
            Err(_) => return Err(String::from("Error creating the Parquet file")),
        };

        match ParquetWriter::new(buffer).finish(&mut df) {
            Ok(_) => Ok(()),
            Err(_) => Err(String::from("Error writing to the Parquet file")),
        }
    }
}
