use crate::{RandomTransformer, Transformer};
use serde;
use serde::{Deserialize, Serialize};
use std::env::VarError;
use std::io::{Error, ErrorKind};
use std::net::Ipv4Addr;
use uriparse::{Scheme, URIReference, URIReferenceError};

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Config {
    pub bind: Ipv4Addr,
    pub port: u16,
    pub source: SourceConfig,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct SourceConfig {
    pub connection_uri: String,
    pub transformers: Vec<TransformerConfig>,
}

impl SourceConfig {
    pub fn connection_uri(&self) -> Result<ConnectionUri, Error> {
        parse_connection_uri(self.connection_uri.as_str())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct TransformerConfig {
    pub table: String,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct ColumnConfig {
    pub name: String,
    pub transformer: TransformerTypeConfig,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum TransformerTypeConfig {
    #[serde(rename = "random")]
    Random,
    #[serde(rename = "random-date")]
    RandomDate,
}

impl TransformerTypeConfig {
    pub fn transformer(&self, table_name: &str, column_name: &str) -> Box<dyn Transformer> {
        let transformer: Box<dyn Transformer> = match self {
            TransformerTypeConfig::Random => {
                Box::new(RandomTransformer::new(table_name, column_name))
            }
            TransformerTypeConfig::RandomDate => todo!(),
        };

        transformer
    }
}

type Host = String;
type Port = u16;
type Username = String;
type Password = String;
type Database = String;

pub enum ConnectionUri {
    Postgres(Host, Port, Username, Password, Database),
    Mysql(Host, Port, Username, Password, Database),
}

fn get_host(uri_ref: &URIReference) -> Result<String, Error> {
    match uri_ref.host() {
        Some(host) => Ok(host.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <host> property from connection uri",
        )),
    }
}

fn get_port(uri_ref: &URIReference, default_port: u16) -> Result<u16, Error> {
    match uri_ref.port() {
        Some(port) if port < 1 => Err(Error::new(
            ErrorKind::Other,
            "<port> from connection uri can't be lower than 0",
        )),
        Some(port) => Ok(port),
        None => Ok(default_port),
    }
}

fn get_username(uri_ref: &URIReference) -> Result<String, Error> {
    match uri_ref.username() {
        Some(username) => Ok(username.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <username> property from connection uri",
        )),
    }
}

fn get_password(uri_ref: &URIReference) -> Result<String, Error> {
    match uri_ref.password() {
        Some(password) => Ok(password.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <password> property from connection uri",
        )),
    }
}

fn get_database(uri_ref: &URIReference, default: Option<&str>) -> Result<String, Error> {
    let path = uri_ref.path().to_string();
    let database = path.split("/").take(1).collect::<Vec<&str>>();

    if database.is_empty() {
        return match default {
            Some(default) => Ok(default.to_string()),
            None => Err(Error::new(
                ErrorKind::Other,
                "missing <database> property from connection uri",
            )),
        };
    }

    let database = match database.get(0) {
        Some(database) => *database,
        None => {
            return match default {
                Some(default) => Ok(default.to_string()),
                None => Err(Error::new(
                    ErrorKind::Other,
                    "missing <database> property from connection uri",
                )),
            };
        }
    };

    Ok(database.to_string())
}

fn parse_connection_uri(uri: &str) -> Result<ConnectionUri, Error> {
    let uri = substitute_env_var(uri)?;

    let uri_ref = match URIReference::try_from(uri.as_str()) {
        Ok(uri_ref) => uri_ref,
        Err(err) => return Err(Error::new(ErrorKind::Other, format!("{:?}", err))),
    };

    let connection_uri = match uri_ref.scheme() {
        Some(err) if err.as_str().to_lowercase() == "postgres" => ConnectionUri::Postgres(
            get_host(&uri_ref)?,
            get_port(&uri_ref, 5432)?,
            get_username(&uri_ref)?,
            get_password(&uri_ref)?,
            get_database(&uri_ref, Some("public"))?,
        ),
        Some(err) if err.as_str().to_lowercase() == "mysql" => ConnectionUri::Postgres(
            get_host(&uri_ref)?,
            get_port(&uri_ref, 3306)?,
            get_username(&uri_ref)?,
            get_password(&uri_ref)?,
            get_database(&uri_ref, None)?,
        ),
        Some(err) => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("'{}' not supported", err.as_str()),
            ));
        }
        None => {
            return Err(Error::new(ErrorKind::Other, "missing URI scheme"));
        }
    };

    Ok(connection_uri)
}

/// take as input $KEY_ENV_VAR and convert it into a real value if the env var does exist
/// otherwise return an Error
fn substitute_env_var(env_var: &str) -> Result<String, Error> {
    match env_var {
        "" => Ok(String::new()),
        env_var if env_var.starts_with("$") && env_var.len() > 1 => {
            let key = &env_var[1..env_var.len()];
            match std::env::var(key) {
                Ok(value) => Ok(value),
                Err(_) => Err(Error::new(
                    ErrorKind::Other,
                    format!("environment variable '{}' is missing", key),
                )),
            }
        }
        x => Ok(x.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{parse_connection_uri, substitute_env_var, SourceConfig};

    #[test]
    fn substitute_env_variables() {
        assert!(substitute_env_var("$DOES_NOT_EXIST").is_err());
        assert_eq!(substitute_env_var("").unwrap(), "".to_string());
        assert_eq!(substitute_env_var("toto").unwrap(), "toto".to_string());

        std::env::set_var("THIS_IS_A_TEST", "here is my value");
        assert_eq!(
            substitute_env_var("$THIS_IS_A_TEST").unwrap(),
            "here is my value"
        );
    }

    #[test]
    fn parse_postgres_connection_uri() {
        assert!(parse_connection_uri("postgres://root:password@localhost:5432/root").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost:5432").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost").is_ok());
        assert!(parse_connection_uri("postgres://root:password").is_err());
    }
}
