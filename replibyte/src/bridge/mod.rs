use aes_gcm::aead::{Aead, NewAead};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use std::io::{Error, ErrorKind, Read, Write};

use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use flate2::Compression;
use serde::{Deserialize, Serialize};

use crate::connector::Connector;
use crate::types::Bytes;

pub mod s3;

pub trait Bridge: Connector + Send + Sync {
    /// Getting Index file with all the backups information
    fn index_file(&self) -> Result<IndexFile, Error>;
    fn write_index_file(&self, index_file: &IndexFile) -> Result<(), Error>;
    fn write(&self, file_part: u16, data: Bytes) -> Result<(), Error>;
    fn read<F>(&self, options: &ReadOptions, data_callback: F) -> Result<(), Error>
    where
        F: FnMut(Bytes);
    fn set_compression(&mut self, enable: bool);
    fn set_encryption_key(&mut self, key: String);
    fn set_backup_name(&mut self, key: String);
    fn delete(&self, backup_name: &str) -> Result<(), Error>;
}

#[derive(Serialize, Deserialize)]
pub struct IndexFile {
    pub backups: Vec<Backup>,
}

impl IndexFile {
    pub fn find_backup(&mut self, options: &ReadOptions) -> Result<&Backup, Error> {
        match options {
            ReadOptions::Latest => {
                self.backups.sort_by(|a, b| a.created_at.cmp(&b.created_at));

                match self.backups.last() {
                    Some(backup) => Ok(backup),
                    None => return Err(Error::new(ErrorKind::Other, "No backups available.")),
                }
            }
            ReadOptions::Backup { name } => {
                match self
                    .backups
                    .iter()
                    .find(|backup| backup.directory_name.as_str() == name.as_str())
                {
                    Some(backup) => Ok(backup),
                    None => {
                        return Err(Error::new(
                            ErrorKind::Other,
                            format!("Can't find backup with name '{}'", name),
                        ));
                    }
                }
            }
        }
    }
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq)]
pub struct Backup {
    pub directory_name: String,
    pub size: usize,
    pub created_at: u128,
    pub compressed: bool,
    pub encrypted: bool,
}

#[derive(Serialize, Deserialize, Ord, PartialOrd, Eq, PartialEq, Clone)]
pub enum ReadOptions {
    Latest,
    Backup { name: String },
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
    use crate::bridge::{compress, decompress, decrypt, encrypt};

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
