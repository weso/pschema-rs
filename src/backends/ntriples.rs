use std::{fs::File, io::BufReader};

use polars::df;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use rio_api::model::Triple;
use rio_api::parser::TriplesParser;
use rio_turtle::NTriplesParser;
use rio_turtle::TurtleError;

use super::Backend;

pub struct NTriples;

impl Backend for NTriples {
    fn import(path: &str) -> Result<DataFrame, String> {
        let mut subjects = Vec::<String>::new();
        let mut predicates = Vec::<String>::new();
        let mut objects = Vec::<String>::new();

        let reader = BufReader::new(match File::open(path) {
            Ok(file) => file,
            Err(_) => return Err(String::from("Cannot open the file")),
        });
        let mut parser = NTriplesParser::new(reader);

        let mut on_triple = |triple: Triple| {
            {
                subjects.push(triple.subject.to_string());
                predicates.push(triple.predicate.to_string());
                objects.push(triple.object.to_string());
            };
            Ok(())
        } as Result<(), TurtleError>;

        while !parser.is_end() {
            if let Err(_) = parser.parse_step(&mut on_triple) {
                // We skip the line if it is not a valid triple
                continue;
            }
        }

        match df![
            Column::Subject.as_ref() => Series::new(Column::Subject.as_ref(), subjects),
            Column::Predicate.as_ref() => Series::new(Column::Subject.as_ref(), predicates),
            Column::Object.as_ref() => Series::new(Column::Subject.as_ref(), objects),
        ] {
            Ok(edges) => Ok(edges),
            Err(_) => Err(String::from("Error creating the edges DataFrame")),
        }
    }

    fn export(_path: &str, _df: DataFrame) -> Result<(), String> {
        unimplemented!()
    }
}
