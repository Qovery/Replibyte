use bson::Document;
use crc::crc64;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};

/// Four bytes that are always present at the beginning of the archive.
const MAGIC_BYTES: [u8; 4] = [0x6d, 0xe2, 0x99, 0x81];

/// Seperator bytes, found mostly between different data blocks.
const SEPERATOR_BYTES: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
/// Mongo archive header document.
///
/// Found immediately after the magic number in the archive, before any Metadata documents.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Header {
    pub concurrent_collections: i32,
    pub version: String,
    pub server_version: String,
    pub tool_version: String,
}
/// Mongo archive collection metadata document.
///
/// there is one Metadata document per collection that will be in the archive.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub db: String,
    pub collection: String,
    pub metadata: String,
    pub size: i32,
    pub r#type: String,
}
/// Mongo archive namespace document.
///
/// namespaces are found in the archive before any data blocks, and also after them,
/// and are used as headers or footers for data blocks.
///
/// if namespace.eof is true, then the namespace is a header, otherwise it is a footer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Namespace {
    pub db: String,
    pub collection: String,
    #[serde(rename = "EOF")]
    pub eof: bool,
    #[serde(rename = "CRC")]
    pub crc: i64,
}

// Prefixes are "<db_name>.<collection_name>"
pub type Prefix = String;
pub type Collection = Vec<Document>;
pub type PrefixedCollections = HashMap<Prefix, Collection>;
/// # Archive
/// reference: https://github.com/mongodb/mongo-tools-common/blob/v4.2/archive/archive.go
///
/// mongodump/mongorestore "archives" are binary files with the following structure:
///  ```
/// // +-----------------------+                                                       
/// // |      magic bytes      |                                                       
/// // +-----------------------+                                                       
/// // |      header Bson      |
/// // +-----------------------+
/// // |    metadata Bson 0    |
/// // +-----------------------+
/// // |    metadata Bson 1    |
/// // +-----------------------+
/// //            ...
/// // +-----------------------+
/// // |    metadata Bson x    |  - x is the number of collections in the database
/// // +-----------------------+
/// // |    seperator bytes    |
/// // +-----------------------+
/// // |======= BLOCK 0 =======|  - each block represents a single collection.
/// // |-----------------------|
/// // |    namespace Bson     |  - contains the collection name and an EOF marker (see reference)
/// // |-----------------------|
/// // |         data          |  - 0 or more of the actual documents in the collection.
/// // |-----------------------|
/// // |    seperator bytes    |
/// // +-----------------------+
/// //            ...
/// // +-----------------------+
/// // |======= BLOCK x =======|  - x is the number of collections in the database
/// // |-----------------------|
/// // |    namespace Bson     |
/// // |-----------------------|
/// // |         data          |  
/// // +-----------------------+
/// // |    seperator bytes    |
/// // +-----------------------+
/// ```
#[derive(Debug, Clone)]
pub struct Archive {
    header: Header,
    metadata_docs: Vec<Metadata>,
    namespace_docs: Vec<Namespace>,
    prefixed_collections: PrefixedCollections, // prefix is <db_name>.<collection_name>
}
impl Archive {
    pub fn from_reader<R: Read>(mut reader: BufReader<R>) -> Result<Archive, Error> {
        let mut buf: [u8; 4] = [0; 4];
        let mut num_blocks = 0;
        let mut vec_eofs = Vec::with_capacity(num_blocks * 2);
        let mut metadata_docs = vec![];
        let mut namespace_docs = vec![];
        let mut prefixed_collections = HashMap::new();

        // read magic bytes
        reader.read_exact(&mut buf)?;
        if buf != MAGIC_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Stream or file does not appear to be a mongodump archive",
            ));
        }

        // read namespace header
        let header: Header = bson::from_reader(&mut reader)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("{}", e)))?;

        // read metadata headers and seperator (seperator is read when while let fails for the 1st time)
        while let Ok(collection_metadata_doc) = bson::from_reader(&mut reader) {
            let metadata_doc = collection_metadata_doc;
            metadata_docs.push(metadata_doc);
            num_blocks += 1;
        }

        if num_blocks > 0 {
            // read blocks
            loop {
                // read namespace header
                let namespace_doc: Namespace = bson::from_reader(&mut reader).map_err(|err| {
                    Error::new(
                        ErrorKind::Other,
                        format!("Error reading block header: {}", err),
                    )
                })?;
                namespace_docs.push(namespace_doc.clone()); // TODO can we avoid cloning here?
                vec_eofs.push(namespace_doc.eof);
                // read block data
                let mut collection_docs = vec![];
                while let Ok(collection_doc) = Document::from_reader(&mut reader) {
                    collection_docs.push(collection_doc.clone());
                }
                if !namespace_doc.eof {
                    // if this namespace was a footer (eof == true), that would mean the collection just ended
                    prefixed_collections.insert(
                        format!("{}.{}", namespace_doc.db, namespace_doc.collection),
                        collection_docs,
                    );
                }
                // when we've seen as much EOFs as there are blocks, we're done.
                if vec_eofs.iter().filter(|&&eof| eof).count() == num_blocks {
                    break;
                }
            }
        }
        Ok(Archive {
            header,
            metadata_docs,
            namespace_docs,
            prefixed_collections,
        })
    }

    pub fn alter_docs<F>(&mut self, alter_fn: F)
    where
        F: FnOnce(&mut PrefixedCollections),
    {
        alter_fn(&mut self.prefixed_collections);
    }

    pub fn into_bytes(mut self) -> Result<Vec<u8>, Error> {
        let mut new_crc64_checksums: HashMap<Prefix, i64> = HashMap::new();
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC_BYTES);
        bson::to_document(&self.header)
            .unwrap()
            .to_writer(&mut buf)
            .map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("Error writing namespace header: {}", err),
                )
            })?;
        for metadata_doc in &self.metadata_docs {
            bson::to_document(&metadata_doc)
                .unwrap()
                .to_writer(&mut buf)
                .map_err(|err| {
                    Error::new(
                        ErrorKind::Other,
                        format!("Error writing metadata doc: {}", err),
                    )
                })?;
        }
        buf.extend_from_slice(&SEPERATOR_BYTES);
        for namespace_doc in &mut self.namespace_docs {
            if !namespace_doc.eof {
                // then this is a collection header
                bson::to_document(&namespace_doc)
                    .unwrap()
                    .to_writer(&mut buf)
                    .map_err(|err| {
                        Error::new(
                            ErrorKind::Other,
                            format!("Error writing block header: {}", err),
                        )
                    })?;
            }
            let prefix = format!("{}.{}", namespace_doc.db, namespace_doc.collection);
            if let Some(docs) = self.prefixed_collections.get(&prefix) {
                let mut collection_bytestream: Vec<u8> = Vec::new();
                for doc in docs {
                    doc.to_writer(&mut collection_bytestream).map_err(|err| {
                        Error::new(
                            ErrorKind::Other,
                            format!("Error writing prefixed doc: {}", err),
                        )
                    })?;
                }
                // revalidate crc64 checksum
                let crc64_checksum = crc64::checksum_ecma(&collection_bytestream);
                new_crc64_checksums.insert(prefix.clone(), crc64_checksum as i64);
                buf.extend_from_slice(&collection_bytestream);
                self.prefixed_collections.remove_entry(&prefix);
            } else {
                // then we've seen the prefix before, which means we've already written the docs
                // this also means this namespace is a footer (eof == true).
                // all that's left is to update the crc64 checksum
                if let Some(&crc64_checksum) = new_crc64_checksums.get(&prefix) {
                    namespace_doc.crc = crc64_checksum;
                    bson::to_document(&namespace_doc)
                        .unwrap()
                        .to_writer(&mut buf)
                        .map_err(|err| {
                            Error::new(
                                ErrorKind::Other,
                                format!("Error writing block header: {}", err),
                            )
                        })?;
                }
            }
            buf.extend_from_slice(&SEPERATOR_BYTES);
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::{mongodb::Archive, utils::decode_hex};
    use std::{fmt::Write, io::BufReader};

    #[test]
    fn mongo_archive_parsing() {
        // archive should contain a single collection "Users" in db "test2" with a single document: {name: "John", age: 42}
        let dump_str = "6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e32000003010000026462000600000074657374320002636f6c6c656374696f6e0006000000557365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223732306531616132326231373435643739663139373530626162323933303837222c22636f6c6c656374696f6e4e616d65223a225573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f46000012435243000000000000000000002e000000075f696400623f23928e7f1feed4d5e3e1026e616d6500050000004a6f686e0010616765002a00000000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f4600011243524300ff2a87dec3c86e6e00ffffffff";
        let hexdump = decode_hex(dump_str).unwrap();
        let reader = BufReader::new(hexdump.as_slice());
        let archive = Archive::from_reader(reader);
        assert!(archive.is_ok());
        let archive = archive.unwrap();
        assert!(archive.prefixed_collections.contains_key("test2.Users"));
        let decoded_collection = archive.prefixed_collections.get("test2.Users").unwrap();
        assert_eq!(
            decoded_collection.first().unwrap().get_str("name").unwrap(),
            "John"
        );
        assert_eq!(
            decoded_collection.first().unwrap().get_i32("age").unwrap(),
            42
        );
    }

    #[test]
    fn mongo_archive_to_bytes() {
        let dump_str = "6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e32000003010000026462000600000074657374320002636f6c6c656374696f6e0006000000557365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223732306531616132326231373435643739663139373530626162323933303837222c22636f6c6c656374696f6e4e616d65223a225573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f46000012435243000000000000000000002e000000075f696400623f23928e7f1feed4d5e3e1026e616d6500050000004a6f686e0010616765002a00000000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f4600011243524300ff2a87dec3c86e6e00ffffffff";
        let hexdump = decode_hex(dump_str).unwrap();
        let reader = BufReader::new(hexdump.as_slice());
        let archive = Archive::from_reader(reader).unwrap();
        let vec_bytes = archive.into_bytes().unwrap();
        let mut out = String::new();
        for byte in vec_bytes {
            write!(out, "{:02x}", byte).unwrap();
        }
        assert_eq!(out.as_str(), dump_str);
    }
}
