use std::io::BufWriter;
use std::{fs::File, io::BufReader};

use polars::df;
use polars::enable_string_cache;
use polars::prelude::*;
use pregel_rs::pregel::Column;
use rio_api::formatter::TriplesFormatter;
use rio_api::model::{Literal, NamedNode, Triple};
use rio_api::parser::TriplesParser;
use rio_turtle::NTriplesFormatter;
use rio_turtle::NTriplesParser;
use rio_turtle::TurtleError;

use super::Backend;

pub struct NTriples;

impl Backend for NTriples {
    fn import(path: &str) -> Result<DataFrame, String> {
        enable_string_cache();

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
                continue;
            }
        }

        match df![
            Column::Subject.as_ref() => Series::new(Column::Subject.as_ptr(), subjects).cast(&DataType::Categorical(None, CategoricalOrdering::Lexical)).unwrap(),
            Column::Predicate.as_ref() => Series::new(Column::Predicate.as_ptr(), predicates).cast(&DataType::Categorical(None, CategoricalOrdering::Lexical)).unwrap(),
            Column::Object.as_ref() => Series::new(Column::Object.as_ptr(), objects).cast(&DataType::Categorical(None, CategoricalOrdering::Lexical)).unwrap(),
        ] {
            Ok(edges) => Ok(edges),
            Err(_) => Err(String::from("Error creating the edges DataFrame")),
        }
    }

    fn export(path: &str, df: &mut DataFrame) -> Result<(), String> {
        let file = File::create(path).unwrap();
        let writer = BufWriter::new(file);
        let mut formatter = NTriplesFormatter::new(writer);

        let df = df
            .clone()
            .lazy()
            .select([
                col(Column::Subject.as_ref()).cast(DataType::String),
                col(Column::Predicate.as_ref()).cast(DataType::String),
                col(Column::Object.as_ref()).cast(DataType::String),
            ])
            .collect()
            .unwrap();

        for i in 0..df.height() {
            let row = match df.get_row(i) {
                Ok(row) => row.0,
                Err(_) => return Err(format!("Error retrieving the {}th row", i)),
            };

            if formatter
                .format(&Triple {
                    subject: match row.get(0) {
                        Some(subject) => match subject {
                            AnyValue::String(iri) => NamedNode {
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
                            AnyValue::String(iri) => NamedNode {
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
                            AnyValue::String(iri) => {
                                if iri.contains("^^") {
                                    let v: Vec<_> = iri.split("^^").collect();
                                    Literal::Typed {
                                        value: &v[0][1..v[0].len() - 1],
                                        datatype: NamedNode {
                                            iri: &v[1][1..v[1].len() - 1],
                                        },
                                    }
                                    .into()
                                } else {
                                    NamedNode {
                                        iri: &iri[1..iri.len() - 1],
                                    }
                                    .into()
                                }
                            }
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
