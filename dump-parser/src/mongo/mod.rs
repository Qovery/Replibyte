use bson::de::Error as BsonError;
use bson::{bson, Document};
use std::io::{BufReader, BufWriter, Cursor, Error, ErrorKind, Read, Write};
use std::str::Bytes;
use std::time::Instant;

// Prefixes are "<db_name>.<collection_name>"
pub type Prefix = String;
pub type PrefixedDocuments = Vec<(Prefix, Document)>;
pub struct Archive {
    chunks: Vec<Document>,
}
impl Archive {
    const MAGIC_BYTES: [u8; 4] = [0x81, 0x99, 0xe2, 0x6d];
    const SEPERATOR_BYTES: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];

    pub fn new() -> Self {
        Archive { chunks: vec![] }
    }

    pub fn parse<R: Read>(mut self, mut reader: BufReader<R>) -> Result<PrefixedDocuments, Error> {
        let mut rows = Vec::new();
        let mut buf: [u8; 4] = [0; 4];
        let mut num_blocks = 0;
        let mut vec_eofs = Vec::with_capacity(num_blocks * 2);

        // Read magic bytes
        reader.read_exact(&mut buf)?;

        // Read namespace header
        let _namespace_header_doc = Document::from_reader(&mut reader).unwrap();
        //self.chunks.push(namespace_header_doc);

        // Read metadata header
        while let Ok(_collection_metadata_doc) = Document::from_reader(&mut reader) {
            //self.chunks.push(collection_metadata_doc);
            num_blocks += 1;
        }

        // Read blocks
        loop {
            // Read block header
            let collection_header_doc = Document::from_reader(&mut reader).unwrap();
            let db_name = collection_header_doc.get_str("db").unwrap();
            let coll_name = collection_header_doc.get_str("collection").unwrap();
            let eof = collection_header_doc.get_bool("EOF").unwrap();
            //self.chunks.push(collection_header_doc);
            // println!("---COLLECTION HEADER---{:?}", collection_header_doc);
            vec_eofs.push(eof);
            // Read block data
            while let Ok(collection_doc) = Document::from_reader(&mut reader) {
                //println!("---COLLECTION--- {:#?}", collection_doc);
                //self.chunks.push(collection_doc);
                rows.push((format!("{}.{}", db_name, coll_name), collection_doc));
            }
            if vec_eofs.iter().filter(|&&eof| eof).count() == num_blocks {
                break;
            }
        }
        Ok(rows)
    }
    pub fn restore() -> Result<Vec<u8>, Error> {
        let buf = Vec::new();
        let mut writer = BufWriter::new(buf);
        writer.write(&Archive::MAGIC_BYTES)?;
        // TODO write metadata header and blocks here ...
        todo!();
    }
}

#[test]
fn test_mongo_dump_into_docs() {
    // TODO
}
