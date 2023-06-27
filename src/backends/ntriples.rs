use std::io::BufWriter;
use std::{fs::File, io::BufReader};

use polars::df;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use rio_api::formatter::TriplesFormatter;
use rio_api::model::{NamedNode, Triple};
use rio_api::parser::TriplesParser;
use rio_turtle::NTriplesFormatter;
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
            if parser.parse_step(&mut on_triple).is_err() {
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

    fn export(path: &str, df: &mut DataFrame) -> Result<(), String> {
        let file = File::create(path).unwrap();
        let writer = BufWriter::new(file);
        let mut formatter = NTriplesFormatter::new(writer);

        for i in 0..df.height() {
            let row = match df.get_row(i) {
                Ok(row) => row.0,
                Err(_) => return Err(format!("Error retrieving the {}th row", i)),
            };

            if formatter
                .format(&Triple {
                    subject: match row.get(0) {
                        Some(subject) => match subject {
                            AnyValue::Utf8(iri) => NamedNode {
                                iri: &iri[1..iri.len() - 1],
                            }
                            .into(),
                            _ => {
                                return Err(format!("Cannot parse from non-string at {}th row", i))
                            }
                        },
                        None => {
                            return Err(format!("Error obtaining the subject of the {}th row", i))
                        }
                    },
                    predicate: match row.get(1) {
                        Some(predicate) => match predicate {
                            AnyValue::Utf8(iri) => NamedNode {
                                iri: &iri[1..iri.len() - 1],
                            },
                            _ => {
                                return Err(format!("Cannot parse from non-string at {}th row", i))
                            }
                        },
                        None => {
                            return Err(format!("Error obtaining the predicate of the {}th row", i))
                        }
                    },
                    object: match row.get(2) {
                        Some(object) => match object {
                            AnyValue::Utf8(iri) => NamedNode {
                                iri: &iri[1..iri.len() - 1],
                            }
                            .into(),
                            _ => {
                                return Err(format!("Cannot parse from non-string at {}th row", i))
                            }
                        },
                        None => {
                            return Err(format!("Error obtaining the object of the {}th row", i))
                        }
                    },
                })
                .is_err()
            {
                return Err(format!("Error parsing the {}th row", i));
            }
        }

        match formatter.finish() {
            Ok(_) => Ok(()),
            Err(_) => Err(String::from("Error storing the results to the file")),
        }
    }
}
