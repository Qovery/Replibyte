use std::collections::HashMap;
use std::io::{BufReader, BufWriter, Error, ErrorKind, Read};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, InsertIntoQuery, OriginalQuery, Query};

use bson::{Bson, Document};
use dump_parser::mongo::Archive;

pub struct Mongo<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Mongo<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Mongo {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for Mongo<'a> {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Source for Mongo<'a> {
    fn read<F: FnMut(OriginalQuery, Query)>(
        &self,
        transformers: &Vec<Box<dyn Transformer + '_>>,
        query_callback: F,
    ) -> Result<(), Error> {
        let s_port = self.port.to_string();

        let mut process = Command::new("mongodump")
            .args([
                "-h",
                self.host,
                "--port",
                s_port.as_str(),
                "--authenticationDatabase",
                "admin",
                "--db",
                self.database,
                "-u",
                self.username,
                "-p",
                self.password,
                "--archive", // dump to stdin
            ])
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        let stdout = process
            .stdout
            .take()
            .ok_or_else(|| Error::new(ErrorKind::Other, "Could not capture standard output."))?;

        let reader = BufReader::new(stdout);

        read_and_transform(reader, transformers, query_callback);

        match process.wait() {
            Ok(exit_status) => {
                if !exit_status.success() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("command error: {:?}", exit_status.to_string()),
                    ));
                }
            }
            Err(err) => return Err(err),
        }

        Ok(())
    }
}

pub fn recursively_transform_bson(
    key: String,
    bson: Bson,
    transformers: &HashMap<String, &Box<dyn Transformer + '_>>,
) -> Bson {
    let mut column;
    match bson {
        Bson::String(value) => {
            column = Column::StringValue(key.clone(), value.clone());
            column = match transformers.get(key.as_str()) {
                Some(transformer) => transformer.transform(column), // apply transformation on the column
                None => column,
            };
            Bson::String((*column.string_value().unwrap()).to_string())
        }
        Bson::Double(value) => {
            column = Column::FloatNumberValue(key.clone(), value);
            column = match transformers.get(key.as_str()) {
                Some(transformer) => transformer.transform(column), // apply transformation on the column
                None => column,
            };
            Bson::Double(*column.float_number_value().unwrap())
        }
        Bson::Array(arr) => Bson::Array(
            arr.iter()
                .enumerate()
                .map(|(idx, bson)| {
                    recursively_transform_bson(
                        format!("{}.{}", key, idx),
                        bson.clone(),
                        transformers,
                    )
                })
                .collect::<Vec<Bson>>(),
        ),
        Bson::Document(nested_doc) => Bson::Document(recursively_transform_document(
            key,
            nested_doc,
            transformers,
        )),
        Bson::Null => Bson::Null,
        Bson::Int32(value) => {
            column = Column::NumberValue(key.clone(), value as i128);
            column = match transformers.get(key.as_str()) {
                Some(transformer) => transformer.transform(column), // apply transformation on the column
                None => column,
            };
            Bson::Int32(column.number_value().map(|&n| n as i32).unwrap())
        }
        Bson::Int64(value) => {
            column = Column::NumberValue(key.clone(), value as i128);
            column = match transformers.get(key.as_str()) {
                Some(transformer) => transformer.transform(column), // apply transformation on the column
                None => column,
            };
            Bson::Int64(column.number_value().map(|&n| n as i64).unwrap())
        }
        _ => panic!("Unsupported BSON type"), // TODO: handle other types
    }
}

pub fn recursively_transform_document(
    full_key: String,
    mut original_doc: Document,
    transformers: &HashMap<String, &Box<dyn Transformer + '_>>,
) -> Document {
    for (key, bson) in original_doc.clone() {
        original_doc.insert(
            key.clone(),
            recursively_transform_bson(format!("{}.{}", full_key, key), bson, transformers),
        );
    }
    original_doc
}

/// consume reader and apply transformation on INSERT INTO queries if needed
pub fn read_and_transform<R: Read, F: FnMut(OriginalQuery, Query)>(
    reader: BufReader<R>,
    transformers: &Vec<Box<dyn Transformer + '_>>,
    mut query_callback: F,
) {
    // create a map variable with Transformer by column_name
    let mut transformer_by_db_and_table_and_column_name: HashMap<String, &Box<dyn Transformer>> =
        HashMap::with_capacity(transformers.len());

    for transformer in transformers {
        let _ = transformer_by_db_and_table_and_column_name.insert(
            transformer.database_and_table_and_column_name(),
            transformer,
        );
    }
    let original_query = Query(reader.buffer().to_vec());

    let docs_with_prefixes = Archive::new().parse(reader).unwrap(); // TODO handle error
    let mut new_docs = Vec::new();
    for (prefix, doc) in docs_with_prefixes {
        let new_doc = recursively_transform_document(
            prefix, // prefix is <db_name>.<collection_name>
            doc,
            &transformer_by_db_and_table_and_column_name,
        );
        new_docs.push(new_doc);
    }

    let buf = Vec::new();
    let mut writer = BufWriter::new(buf);
    for doc in new_docs {
        doc.to_writer(&mut writer).unwrap(); // address unwraping here
    }
    let query = Query(writer.buffer().to_vec());

    query_callback(original_query, query);
}

#[cfg(test)]
mod tests {
    use crate::transformer::first_name::FirstNameTransformer;
    use crate::transformer::random::RandomTransformer;
    use crate::Source;
    use bson::{bson, doc, Document};
    use std::collections::HashMap;
    use std::vec;

    use crate::source::mongo::Mongo;
    use crate::transformer::transient::TransientTransformer;
    use crate::transformer::Transformer;

    use super::recursively_transform_document;

    fn get_mongo() -> Mongo<'static> {
        Mongo::new("localhost", 27017, "test", "root", "password")
    }

    fn get_invalid_mongo() -> Mongo<'static> {
        Mongo::new("localhost", 27017, "test", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_mongo();

        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_ok());

        let p = get_invalid_mongo();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_mongo();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        p.read(&transformers, |original_query, query| {
            assert!(original_query.data().len() > 0);
            assert!(query.data().len() > 0);
        })
        .unwrap();
    }

    #[test]
    fn recursive_document_transform() {
        let database_name = "test";
        let table_name = "users";
        let columns = vec!["no_nest", "info.ext.number", "info_arr.0.a", "info_arr.1.b"];
        let doc = doc! {
            "no_nest": 5,
            "info": {
                "ext": {
                    "number": 123456789000 as i64
                }
            },
            "info_arr" : [
                { "a": "SomeString" },
                { "b": 3.5 }
            ]
        };
        // Create a vec of all transformers
        let transformers_vec = Vec::from_iter(columns.iter().map(|&c| {
            let t: Box<dyn Transformer> = Box::new(RandomTransformer::new(
                database_name,
                table_name,
                &c.to_string(),
            ));
            t
        }));
        // Create a HashMap with Transformer by db_name.table_name.column_name
        let transformers = HashMap::from_iter(
            transformers_vec
                .iter()
                .map(|t| t.database_and_table_and_column_name())
                .zip(transformers_vec.iter()),
        );
        // Recursively transform the document
        let transformed_doc =
            recursively_transform_document("test.users".to_string(), doc, &transformers);

        println!("{:#?}", transformed_doc);

        // Assert transformed values are not equal to original values

        // no_nest
        assert_ne!(
            transformed_doc.get("no_nest").unwrap(),
            &bson::Bson::Int32(5)
        );
        // info.ext.number
        assert_ne!(
            transformed_doc
                .get_document("info")
                .unwrap()
                .get_document("ext")
                .unwrap()
                .get("number")
                .unwrap(),
            &bson::Bson::Int64(1234567890)
        );

        let arr = transformed_doc.get_array("info_arr").unwrap();
        // info_arr.0.a
        let doc = arr[0].as_document().unwrap();
        assert_ne!(
            doc.get("a").unwrap(),
            &bson::Bson::String("SomeString".to_string())
        );
        // info_arr.1.b
        let doc = arr[1].as_document().unwrap();
        assert_ne!(doc.get("b").unwrap(), &bson::Bson::Double(3.5));
    }
}
