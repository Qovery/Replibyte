use std::collections::{HashMap, HashSet};
use std::io::{BufReader, Error, ErrorKind, Read};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::source::Source;
use crate::transformer::Transformer;
use crate::types::{Column, OriginalQuery, Query};

use bson::{Bson, Document};
use dump_parser::mongodb::Archive;

pub struct MongoDB<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> MongoDB<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        MongoDB {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for MongoDB<'a> {
    fn init(&mut self) -> Result<(), Error> {
        Ok(())
    }
}

impl<'a> Source for MongoDB<'a> {
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

        read_and_transform(reader, transformers, query_callback)?;

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
    wildcard_keys: &HashSet<String>,
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
        Bson::Array(arr) => {
            let new_arr = arr
                .iter()
                .enumerate()
                .map(|(idx, bson)| {
                    let wildcard_key = format!("{}.$[]", key);
                    recursively_transform_bson(
                        if wildcard_keys.contains(&wildcard_key) {
                            wildcard_key
                        } else {
                            format!("{}.{}", key, idx)
                        },
                        bson.clone(),
                        transformers,
                        wildcard_keys,
                    )
                })
                .collect::<Vec<Bson>>();
            Bson::Array(new_arr)
        }
        Bson::Document(nested_doc) => Bson::Document(recursively_transform_document(
            key,
            nested_doc,
            transformers,
            wildcard_keys,
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
        // ALL OF THE NEXT TYPES ARE NOT TRANSFORMABLE (yet?)
        Bson::ObjectId(oid) => Bson::ObjectId(oid),
        Bson::Binary(bin) => Bson::Binary(bin),
        Bson::RegularExpression(regex) => Bson::RegularExpression(regex),
        Bson::Boolean(value) => Bson::Boolean(value),
        Bson::DateTime(value) => Bson::DateTime(value),
        Bson::Timestamp(value) => Bson::Timestamp(value),
        Bson::MinKey => Bson::MinKey,
        Bson::MaxKey => Bson::MaxKey,
        Bson::JavaScriptCode(jsc) => Bson::JavaScriptCode(jsc),
        Bson::JavaScriptCodeWithScope(jsc) => Bson::JavaScriptCodeWithScope(jsc),
        Bson::Symbol(symbol) => Bson::Symbol(symbol),
        Bson::Decimal128(decimal) => Bson::Decimal128(decimal),
        Bson::Undefined => Bson::Undefined,
        Bson::DbPointer(db_pointer) => Bson::DbPointer(db_pointer),
    }
}

pub fn recursively_transform_document(
    prefix: String,
    mut original_doc: Document,
    transformers: &HashMap<String, &Box<dyn Transformer + '_>>,
    wildcard_keys: &HashSet<String>,
) -> Document {
    for (key, bson) in original_doc.clone() {
        original_doc.insert(
            key.clone(),
            recursively_transform_bson(
                format!("{}.{}", prefix, key),
                bson,
                transformers,
                wildcard_keys,
            ),
        );
    }
    original_doc
}

pub(crate) fn find_all_keys_with_array_wildcard_op(
    transformers: &Vec<Box<dyn Transformer + '_>>,
) -> HashSet<String> {
    let mut wildcard_keys = HashSet::new();
    for transformer in transformers {
        let column_name = transformer.column_name();
        let delim = ".$[].";
        let mut iter = 0;
        while let Some(idx) = column_name[iter..].find(delim) {
            let offset = delim.len();
            iter += idx + offset;
            let key = column_name[..(iter - 1)].to_string();
            wildcard_keys.insert(format!("{}.{}", transformer.database_and_table_name(), key));
        }
        // try to find last delim
        let last_delim = ".$[]"; // no dot at the end
        if let Some(_) = column_name[iter..].find(last_delim) {
            let key = column_name.to_string();
            wildcard_keys.insert(format!("{}.{}", transformer.database_and_table_name(), key));
        }
    }
    wildcard_keys
}

/// consume reader and apply transformation on INSERT INTO queries if needed
pub fn read_and_transform<R: Read, F: FnMut(OriginalQuery, Query)>(
    reader: BufReader<R>,
    transformers: &Vec<Box<dyn Transformer + '_>>,
    mut query_callback: F,
) -> Result<(), Error> {
    // create a set of wildcards to be used in the transformation
    let wildcard_keys = find_all_keys_with_array_wildcard_op(transformers);
    // create a map variable with Transformer by column_name
    let mut transformer_by_db_and_table_and_column_name: HashMap<String, &Box<dyn Transformer>> =
        HashMap::with_capacity(transformers.len());

    for transformer in transformers {
        let _ = transformer_by_db_and_table_and_column_name.insert(
            transformer.database_and_table_and_column_name(),
            transformer,
        );
    }
    // init archive from reader
    let mut archive = Archive::from_reader(reader)?;

    let original_query = Query(archive.to_bytes()?);

    archive.alter_docs(|prefixed_docs| {
        for (prefix, doc) in prefixed_docs.to_owned() {
            let new_doc = recursively_transform_document(
                prefix.clone(), // prefix is <db_name>.<collection_name>
                doc,
                &transformer_by_db_and_table_and_column_name,
                &wildcard_keys,
            );
            prefixed_docs.insert(prefix, new_doc);
        }
    });

    let query = Query(archive.to_bytes()?);

    query_callback(original_query, query);
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::transformer::random::RandomTransformer;
    use crate::Source;
    use bson::{doc, Bson};
    use std::collections::{HashMap, HashSet};
    use std::vec;

    use crate::source::mongodb::{find_all_keys_with_array_wildcard_op, MongoDB};
    use crate::transformer::transient::TransientTransformer;
    use crate::transformer::Transformer;

    use super::recursively_transform_document;

    fn get_mongodb() -> MongoDB<'static> {
        MongoDB::new("localhost", 27017, "test", "root", "password")
    }

    fn get_invalid_mongodb() -> MongoDB<'static> {
        MongoDB::new("localhost", 27017, "test", "root", "wrongpassword")
    }

    #[test]
    fn connect() {
        let p = get_mongodb();

        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_ok());

        let p = get_invalid_mongodb();
        let t1: Box<dyn Transformer> = Box::new(TransientTransformer::default());
        let transformers = vec![t1];
        assert!(p.read(&transformers, |_, _| {}).is_err());
    }

    #[test]
    fn list_rows() {
        let p = get_mongodb();
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
        let transformed_doc = recursively_transform_document(
            "test.users".to_string(),
            doc,
            &transformers,
            &HashSet::new(),
        );

        // Assert transformed values are not equal to original values
        // no_nest
        assert_ne!(transformed_doc.get("no_nest").unwrap(), &Bson::Int32(5));
        // info.ext.number
        assert_ne!(
            transformed_doc
                .get_document("info")
                .unwrap()
                .get_document("ext")
                .unwrap()
                .get("number")
                .unwrap(),
            &Bson::Int64(1234567890)
        );

        let arr = transformed_doc.get_array("info_arr").unwrap();
        // info_arr.0.a
        let doc = arr[0].as_document().unwrap();
        assert_ne!(
            doc.get("a").unwrap(),
            &Bson::String("SomeString".to_string())
        );
        // info_arr.1.b
        let doc = arr[1].as_document().unwrap();
        assert_ne!(doc.get("b").unwrap(), &Bson::Double(3.5));
    }

    #[test]
    fn recursive_document_transform_with_wildcard_nested() {
        let database_name = "test";
        let table_name = "users";
        let column_name = "a.b.$[].c.0";
        let doc = doc! {
            "a": {
                "b" : [
                    {
                        "c" : [
                            1, // should be transformed
                            2  // shouldn't be transformed
                        ]
                    },
                    {
                        "c" : [
                            3, // should be transformed
                            4  // shouldn't be transformed
                        ]
                    }
                ]
            }
        };
        let t: Box<dyn Transformer> = Box::new(RandomTransformer::new(
            database_name,
            table_name,
            column_name.into(),
        ));
        let transformers_vec = vec![t];
        // create a set of wildcards to be used in the transformation
        let wildcard_keys = find_all_keys_with_array_wildcard_op(&transformers_vec);
        // create a map variable with Transformer by column_name
        let mut transformers: HashMap<String, &Box<dyn Transformer>> =
            HashMap::with_capacity(transformers_vec.len());

        for transformer in transformers_vec.iter() {
            let _ = transformers.insert(
                transformer.database_and_table_and_column_name(),
                transformer,
            );
        }
        // Recursively transform the document
        let transformed_doc = recursively_transform_document(
            "test.users".to_string(),
            doc,
            &transformers,
            &wildcard_keys,
        );

        // Assert transformed values are not equal to original values
        let arr = transformed_doc
            .get_document("a")
            .unwrap()
            .get_array("b")
            .unwrap();
        // 1, 2
        let mut doc = arr[0].as_document().unwrap();
        let mut inner_arr = doc.get_array("c").unwrap();
        assert_ne!(inner_arr[0], Bson::Int32(1));
        assert_eq!(inner_arr[1], Bson::Int32(2));
        // 3, 4
        doc = arr[1].as_document().unwrap();
        inner_arr = doc.get_array("c").unwrap();
        assert_ne!(inner_arr[0], Bson::Int32(3));
        assert_eq!(inner_arr[1], Bson::Int32(4));
    }
}
