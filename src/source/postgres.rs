use std::borrow::Borrow;
use std::fs;
use std::io::{Error, ErrorKind};

use native_tls::{Certificate, TlsConnector};
use postgres::tls::MakeTlsConnect;
use postgres::{Client, NoTls, Socket};
use postgres_native_tls::MakeTlsConnector;

use crate::connector::Connector;
use crate::database::Database;
use crate::source::Source;

struct PostgresError(postgres::error::Error);

impl From<PostgresError> for Error {
    fn from(err: PostgresError) -> Self {
        Error::new(ErrorKind::Other, err.0.to_string())
    }
}

pub struct Postgres<'a> {
    connection_uri: &'a str,
    enable_tls: bool,
    client: Option<Client>,
}

impl<'a> Postgres<'a> {
    pub fn new(connection_uri: &'a str, enable_tls: bool) -> Self {
        Postgres {
            connection_uri,
            enable_tls,
            client: None,
        }
    }
}

impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        self.connect()
    }
}

impl<'a> Source for Postgres<'a> {}

impl<'a> Database for Postgres<'a> {
    fn connect(&mut self) -> Result<(), Error> {
        let client = match self.enable_tls {
            true => Client::connect(self.connection_uri, get_tls()?),
            false => Client::connect(self.connection_uri, NoTls),
        }
        .map_err(|err| Error::from(PostgresError(err)))?;

        self.client = Some(client);

        Ok(())
    }
}

pub fn get_tls() -> Result<MakeTlsConnector, Error> {
    // FIXME ?
    let cert = fs::read("database_cert.pem")?;

    let cert = match Certificate::from_pem(&cert) {
        Ok(cert) => cert,
        Err(err) => return Err(Error::new(ErrorKind::Other, err.to_string())),
    };

    let connector = match TlsConnector::builder().add_root_certificate(cert).build() {
        Ok(connector) => connector,
        Err(err) => return Err(Error::new(ErrorKind::Other, err.to_string())),
    };

    Ok(MakeTlsConnector::new(connector))
}
