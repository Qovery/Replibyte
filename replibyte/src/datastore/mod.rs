use aes_gcm::aead::{Aead, NewAead};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use chrono::{Duration, Utc};
use serde_json::Value;
use std::io::{Error, ErrorKind, Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::cli::DumpDeleteArgs;
use crate::connector::Connector;
use crate::types::Bytes;
use crate::utils::get_replibyte_version;

pub mod local_disk;
pub mod s3;

const INDEX_FILE_NAME: &str = "metadata.json";

pub trait Datastore: Connector + Send + Sync {
    /// Getting Index file with all the dumps information
    fn index_file(&self) -> Result<IndexFile, Error>;
    fn raw_index_file(&self) -> Result<Value, Error>;
    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error>;
    fn write_raw_index_file(&self, raw_index_file: &Value) -> Result<(), Error>;
    fn write(&self, file_part: u16, data: Bytes) -> Result<(), Error>;
    fn read(
        &self,
        options: &ReadOptions,
        data_callback: &mut dyn FnMut(Bytes),
    ) -> Result<(), Error>;
    fn compression_enabled(&self) -> bool;
    fn set_compression(&mut self, enable: bool);
    fn encryption_key(&self) -> &Option<String>;
    fn set_encryption_key(&mut self, key: String);
    fn set_dump_name(&mut self, name: String);
    fn delete_by_name(&self, name: String) -> Result<(), Error>;

    fn delete(&self, args: &DumpDeleteArgs) -> Result<(), Error> {
        if let Some(dump_name) = &args.dump {
            return self.delete_by_name(dump_name.to_string());
        }

        if let Some(older_than) = &args.older_than {
            let days = match older_than.chars().nth_back(0) {
                Some('d') => {
                    // remove the last character which corresponds to the unit
                    let mut older_than = older_than.to_string();
                    older_than.pop();

                    match older_than.parse::<i64>() {
                        Ok(days) => days,
                        Err(err) => return Err(Error::new(
                            ErrorKind::Other,
                            format!("command error: {} - invalid `--older-than` format. Use `--older-than=14d`", err),
                        )),
                    }
                }
                _ => {
                    return Err(Error::new(
                        ErrorKind::Other,
                        "command error: invalid `--older-than` format. Use `--older-than=14d`",
                    ));
                }
            };

            return self.delete_older_than(days);
        }

        if let Some(keep_last) = args.keep_last {
            return self.delete_keep_last(keep_last);
        }

        Err(Error::new(
            ErrorKind::Other,
            "command error: parameters or options required",
        ))
    }

    fn delete_older_than(&self, days: i64) -> Result<(), Error> {
        let index_file = self.index_file()?;

        let threshold_date = Utc::now() - Duration::days(days);
        let threshold_date = threshold_date.timestamp_millis() as u128;

        let dumps_to_delete: Vec<Dump> = index_file
            .dumps
            .into_iter()
            .filter(|b| b.created_at.lt(&threshold_date))
            .collect();

        for dump in dumps_to_delete {
            let dump_name = dump.directory_name;
            self.delete_by_name(dump_name)?
        }

        Ok(())
    }

    fn delete_keep_last(&self, keep_last: usize) -> Result<(), Error> {
        let mut index_file = self.index_file()?;

        index_file
            .dumps
            .sort_by(|a, b| b.created_at.cmp(&a.created_at));

        if let Some(dumps) = index_file.dumps.get(keep_last..) {
            for dump in dumps {
                let dump_name = &dump.directory_name;
                self.delete_by_name(dump_name.to_string())?;
            }
        }

        Ok(())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct IndexFile {
    pub v: Option<String>,
    pub dumps: Vec<Dump>,
}

impl IndexFile {
    pub fn new() -> Self {
        Self {
            v: Some(get_replibyte_version().to_string()),
            dumps: vec![],
        }
    }

    pub fn find_dump(&mut self, options: &ReadOptions) -> Result<&Dump, Error> {
        match options {
            ReadOptions::Latest => {
                self.dumps.sort_by(|a, b| a.created_at.cmp(&b.created_at));

                match self.dumps.last() {
                    Some(dump) => Ok(dump),
                    None => return Err(Error::new(ErrorKind::Other, "No dumps available.")),
                }
            }
            ReadOptions::Dump { name } => {
                match self
                    .dumps
                    .iter()
                    .find(|dump| dump.directory_name.as_str() == name.as_str())
                {
                    Some(dump) => Ok(dump),
                    None => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("Can't find dump with name '{}'", name),
                        ));
                    }
                }
            }
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Dump {
    pub directory_name: String,
    pub size: usize,
    pub created_at: u128,
    pub compressed: bool,
    pub encrypted: bool,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum ReadOptions {
    Latest,
    Dump { name: String },
}

fn compress(data: Bytes) -> Result<Bytes, Error> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    let _ = enc.write_all(data.as_slice());
    enc.flush_finish()
}

fn decompress(data: Bytes) -> Result<Bytes, Error> {
    let mut dec = ZlibDecoder::new(data.as_slice());
    let mut decoded_data = Vec::new();
    let _ = dec.read_to_end(&mut decoded_data);
    Ok(decoded_data)
}

fn get_encryption_key_with_correct_length(key: &str) -> String {
    if key.len() >= 32 {
        return key[0..32].to_string();
    }

    let mut key_string = key.to_string();
    for _ in 0..(32 - key.len()) {
        key_string.push('x');
    }

    key_string
}

fn encrypt(data: Bytes, encryption_key: &str) -> Result<Bytes, Error> {
    let key = get_encryption_key_with_correct_length(encryption_key);
    let key = Key::from_slice(key.as_bytes());
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(b"unique nonce");

    let encrypted_data = match cipher.encrypt(nonce, data.as_slice()) {
        Ok(data) => data,
        Err(err) => return Err(Error::new(ErrorKind::Other, format!("{:?}", err))),
    };

    Ok(encrypted_data)
}

fn decrypt(encrypted_data: Bytes, encryption_key: &str) -> Result<Bytes, Error> {
    let key = get_encryption_key_with_correct_length(encryption_key);
    let key = Key::from_slice(key.as_bytes());
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(b"unique nonce");

    let data = match cipher.decrypt(nonce, encrypted_data.as_slice()) {
        Ok(data) => data,
        Err(err) => return Err(Error::new(ErrorKind::Other, format!("{:?}", err))),
    };

    Ok(data)
}

#[cfg(test)]
mod tests {
    use crate::datastore::{compress, decompress, decrypt, encrypt};

    #[test]
    fn test_compression() {
        let data = b"hello w0rld - this is a long sentence right?".to_vec();
        let compressed_data = compress(data.clone()).unwrap();
        assert_ne!(data, compressed_data);
        assert_eq!(decompress(compressed_data).unwrap(), data);
    }

    #[test]
    fn test_encryption_1() {
        let key = "this is my secret";
        let data = b"hello w0rld hello w0rld hello w0rld hello w0rld hello w0rld".to_vec();
        let encrypted_data = encrypt(data.clone(), key).unwrap();
        assert_ne!(encrypted_data, data);
        assert_eq!(decrypt(encrypted_data, key).unwrap(), data);
    }

    #[test]
    fn test_encryption_2() {
        let key = "this is my secret very very very long and greater than 32 chars";
        let data = b"hello w0rld hello w0rld hello w0rld hello w0rld hello w0rld".to_vec();
        let encrypted_data = encrypt(data.clone(), key).unwrap();
        assert_ne!(encrypted_data, data);
        assert_eq!(decrypt(encrypted_data, key).unwrap(), data);
    }
}
