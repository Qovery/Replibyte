use bson::Document;
use std::collections::HashMap;
use std::io::{BufReader, Error, ErrorKind, Read};

// Prefixes are "<db_name>.<collection_name>"
pub type Prefix = String;
pub type PrefixedDocuments = HashMap<Prefix, Document>;
/// mongodump/mongorestore "archives" are binary files with the following structure:
/// ```
/// // +-----------------------+                                                       
/// // |      magic bytes      |                                                       
/// // +-----------------------+                                                       
/// // | namespace header Bson |
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
/// // |   block header Bson   |  - contains the collection name and an EOF marker (further explanation in reference below)
/// // |-----------------------|
/// // |         data          |  - 0 or more of the actual documents in the collection.
/// // |-----------------------|
/// // |    seperator bytes    |
/// // +-----------------------+
/// //            ...
/// // +-----------------------+
/// // |======= BLOCK x =======|  - x is the number of collections in the database
/// // |-----------------------|
/// // |   block header Bson   |
/// // |-----------------------|
/// // |         data          |  
/// // +-----------------------+
/// // |    seperator bytes    |
/// // +-----------------------+
/// ```
/// reference: https://github.com/mongodb/mongo-tools-common/blob/v4.2/archive/archive.go
#[derive(Debug)]
pub struct Archive {
    namespace_header: Document,
    metadata_docs: Vec<Document>,
    block_headers: Vec<Document>,
    prefixed_docs: PrefixedDocuments, // prefix is <db_name>.<collection_name>
}
impl Archive {
    const MAGIC_BYTES: [u8; 4] = [0x6d, 0xe2, 0x99, 0x81];
    const SEPERATOR_BYTES: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

    pub fn from_reader<R: Read>(mut reader: BufReader<R>) -> Result<Archive, Error> {
        let mut buf: [u8; 4] = [0; 4];
        let mut num_blocks = 0;
        let mut vec_eofs = Vec::with_capacity(num_blocks * 2);
        let mut metadata_docs = vec![];
        let mut block_headers = vec![];
        let mut prefixed_docs = HashMap::new();

        // read magic bytes
        reader.read_exact(&mut buf)?;
        if buf != Archive::MAGIC_BYTES {
            return Err(Error::new(
                ErrorKind::InvalidData,
                "Stream or file does not appear to be a mongodump archive",
            ));
        }

        // read namespace header
        let namespace_header = Document::from_reader(&mut reader)
            .map_err(|e| Error::new(ErrorKind::InvalidData, format!("{}", e)))?;

        // read metadata headers and seperator (seperator is read when while let fails for the 1st time)
        while let Ok(collection_metadata_doc) = Document::from_reader(&mut reader) {
            metadata_docs.push(collection_metadata_doc);
            num_blocks += 1;
        }

        // read blocks
        loop {
            // read block header
            let collection_header_doc = Document::from_reader(&mut reader).map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("Error reading block header: {}", err),
                )
            })?;
            block_headers.push(collection_header_doc.clone()); // TODO can we avoid cloning here?
            let db_name = collection_header_doc.get_str("db").unwrap();
            let coll_name = collection_header_doc.get_str("collection").unwrap();
            let eof = collection_header_doc.get_bool("EOF").unwrap();
            vec_eofs.push(eof);
            // read block data
            while let Ok(collection_doc) = Document::from_reader(&mut reader) {
                prefixed_docs.insert(format!("{}.{}", db_name, coll_name), collection_doc.clone());
            }
            // when we've seen as much EOFs as there are blocks, we're done.
            if vec_eofs.iter().filter(|&&eof| eof).count() == num_blocks {
                break;
            }
        }
        Ok(Archive {
            namespace_header,
            metadata_docs,
            block_headers,
            prefixed_docs,
        })
    }

    pub fn alter_docs<F>(&mut self, alter_fn: F)
    where
        F: FnOnce(&mut PrefixedDocuments),
    {
        alter_fn(&mut self.prefixed_docs);
    }

    pub fn to_bytes(&mut self) -> Result<Vec<u8>, Error> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&Archive::MAGIC_BYTES);
        self.namespace_header.to_writer(&mut buf).map_err(|err| {
            Error::new(
                ErrorKind::Other,
                format!("Error writing namespace header: {}", err),
            )
        })?;
        for metadata_doc in &self.metadata_docs {
            metadata_doc.to_writer(&mut buf).map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("Error writing metadata doc: {}", err),
                )
            })?;
        }
        buf.extend_from_slice(&Archive::SEPERATOR_BYTES);
        for block_header in &self.block_headers {
            block_header.to_writer(&mut buf).map_err(|err| {
                Error::new(
                    ErrorKind::Other,
                    format!("Error writing block header: {}", err),
                )
            })?;
            let prefix = format!(
                "{}.{}",
                block_header.get_str("db").unwrap(),
                block_header.get_str("collection").unwrap()
            );
            if let Some(doc) = self.prefixed_docs.get(&prefix) {
                doc.to_writer(&mut buf).map_err(|err| {
                    Error::new(
                        ErrorKind::Other,
                        format!("Error writing prefixed doc: {}", err),
                    )
                })?;
                self.prefixed_docs.remove_entry(&prefix);
            }
            buf.extend_from_slice(&Archive::SEPERATOR_BYTES);
        }

        Ok(buf)
    }
}

#[cfg(test)]
mod tests {
    use crate::mongo::Archive;
    use std::{fmt::Write, io::BufReader};

    fn decode_hex(s: &str) -> Result<Vec<u8>, std::num::ParseIntError> {
        (0..s.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
            .collect()
    }

    #[test]
    fn mongo_archive_parsing() {
        // archive should contain a single collection "Users" in db "test2" with a single document: {name: "John", age: 42}
        let dump_str = "6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e32000003010000026462000600000074657374320002636f6c6c656374696f6e0006000000557365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223732306531616132326231373435643739663139373530626162323933303837222c22636f6c6c656374696f6e4e616d65223a225573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f46000012435243000000000000000000002e000000075f696400623f23928e7f1feed4d5e3e1026e616d6500050000004a6f686e0010616765002a00000000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f4600011243524300ff2a87dec3c86e6e00ffffffff";
        let hexdump = decode_hex(dump_str).unwrap();
        let reader = BufReader::new(hexdump.as_slice());
        let archive = Archive::from_reader(reader);
        assert!(archive.is_ok());
        let archive = archive.unwrap();
        assert!(archive.prefixed_docs.contains_key("test2.Users"));
        let decoded_doc = archive.prefixed_docs.get("test2.Users").unwrap();
        assert_eq!(decoded_doc.get_str("name").unwrap(), "John");
        assert_eq!(decoded_doc.get_i32("age").unwrap(), 42);
    }

    #[test]
    fn mongo_archive_to_bytes() {
        let dump_str = "6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e32000003010000026462000600000074657374320002636f6c6c656374696f6e0006000000557365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223732306531616132326231373435643739663139373530626162323933303837222c22636f6c6c656374696f6e4e616d65223a225573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f46000012435243000000000000000000002e000000075f696400623f23928e7f1feed4d5e3e1026e616d6500050000004a6f686e0010616765002a00000000ffffffff3c000000026462000600000074657374320002636f6c6c656374696f6e000600000055736572730008454f4600011243524300ff2a87dec3c86e6e00ffffffff";
        let hexdump = decode_hex(dump_str).unwrap();
        let reader = BufReader::new(hexdump.as_slice());
        let mut archive = Archive::from_reader(reader).unwrap();
        let vec_bytes = archive.to_bytes().unwrap();
        let mut out = String::new();
        for byte in vec_bytes {
            write!(out, "{:02x}", byte).unwrap();
        }
        assert_eq!(out.as_str(), dump_str);
    }
}
