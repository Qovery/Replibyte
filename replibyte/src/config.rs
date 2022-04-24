use crate::transformer::credit_card::CreditCardTransformer;
use crate::transformer::custom_wasm::{CustomWasmTransformer, CustomWasmTransformerOptions};
use crate::transformer::email::EmailTransformer;
use crate::transformer::first_name::FirstNameTransformer;
use crate::transformer::keep_first_char::KeepFirstCharTransformer;
use crate::transformer::phone_number::PhoneNumberTransformer;
use crate::transformer::random::RandomTransformer;
use crate::transformer::redacted::{RedactedTransformer, RedactedTransformerOptions};
use crate::transformer::transient::TransientTransformer;
use crate::transformer::Transformer;
use serde;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use url::Url;

const DEFAULT_MONGODB_AUTH_DB: &str = "admin";

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct Config {
    // pub bind: Ipv4Addr,
    // pub port: u16,
    pub source: Option<SourceConfig>,
    pub bridge: BridgeConfig,
    pub destination: Option<DestinationConfig>,
}

pub enum ConnectorConfig<'a> {
    Source(&'a SourceConfig),
    Destination(&'a DestinationConfig),
}

impl Config {
    pub fn connector(&self) -> Result<ConnectorConfig, Error> {
        if let Some(source) = &self.source {
            return Ok(ConnectorConfig::Source(source));
        }

        if let Some(destination) = &self.destination {
            return Ok(ConnectorConfig::Destination(destination));
        }

        Err(Error::new(
            ErrorKind::Other,
            "<source> or <destination> is mandatory",
        ))
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct BridgeConfig {
    // At the moment we do support only S3 as B,
    // in a near future we'll need to make it generic
    pub bucket: String,
    pub region: String,
    pub access_key_id: String,
    pub secret_access_key: String,
    pub endpoint: Option<Endpoint>,
}

impl BridgeConfig {
    /// decode and return the bucket value
    pub fn bucket(&self) -> Result<String, Error> {
        substitute_env_var(self.bucket.as_str())
    }

    /// decode and return the region value
    pub fn region(&self) -> Result<String, Error> {
        substitute_env_var(self.region.as_str())
    }

    /// decode and return the access_key_id value
    pub fn access_key_id(&self) -> Result<String, Error> {
        substitute_env_var(self.access_key_id.as_str())
    }

    /// decode and return the secret_access_key value
    pub fn secret_access_key(&self) -> Result<String, Error> {
        substitute_env_var(self.secret_access_key.as_str())
    }

    /// decode and return the endpoint value
    pub fn endpoint(&self) -> Result<Endpoint, Error> {
        if let Some(endpoint) = &self.endpoint {
            match endpoint {
                Endpoint::Custom(url) => match substitute_env_var(url.as_str()) {
                    Ok(substituted_url) => Ok(Endpoint::Custom(substituted_url)),
                    Err(err) => Err(err),
                },
                _ => Ok(Endpoint::Default),
            }
        } else {
            Ok(Endpoint::Default)
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SourceConfig {
    pub connection_uri: String,
    pub compression: Option<bool>,
    pub encryption_key: Option<String>,
    pub transformers: Vec<TransformerConfig>,
    pub skip: Option<Vec<SkipConfig>>,
    pub database_subset: Option<DatabaseSubsetConfig>,
}

impl SourceConfig {
    pub fn connection_uri(&self) -> Result<ConnectionUri, Error> {
        parse_connection_uri(self.connection_uri.as_str())
    }

    pub fn encryption_key(&self) -> Result<Option<String>, Error> {
        match &self.encryption_key {
            Some(key) => substitute_env_var(key.as_str()).map(|x| Some(x)),
            None => Ok(None),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DestinationConfig {
    pub connection_uri: String,
}

impl DestinationConfig {
    pub fn connection_uri(&self) -> Result<ConnectionUri, Error> {
        parse_connection_uri(self.connection_uri.as_str())
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct SkipConfig {
    pub database: String,
    pub table: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct DatabaseSubsetConfig {
    pub database: String,
    pub table: String,
    #[serde(flatten)]
    pub strategy: DatabaseSubsetConfigStrategy,
    // copy the entire table - not affected by the subset algorithm
    pub passthrough_tables: Option<Vec<String>>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "strategy_name", content = "strategy_options")]
pub enum DatabaseSubsetConfigStrategy {
    Random(DatabaseSubsetConfigStrategyRandom),
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub struct DatabaseSubsetConfigStrategyRandom {
    pub percent: u8,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct TransformerConfig {
    pub database: String,
    pub table: String,
    pub columns: Vec<ColumnConfig>,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub struct ColumnConfig {
    pub name: String,

    #[serde(flatten)]
    pub transformer: TransformerTypeConfig,
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
#[serde(rename_all = "kebab-case")]
#[serde(tag = "transformer_name", content = "transformer_options")]
pub enum TransformerTypeConfig {
    Random,
    RandomDate,
    FirstName,
    Email,
    KeepFirstChar,
    PhoneNumber,
    CreditCard,
    Redacted(Option<RedactedTransformerOptions>),
    Transient,
    CustomWasm(CustomWasmTransformerOptions),
}

impl TransformerTypeConfig {
    pub fn transformer(
        &self,
        database_name: &str,
        table_name: &str,
        column_name: &str,
    ) -> Box<dyn Transformer> {
        let transformer: Box<dyn Transformer> = match self {
            TransformerTypeConfig::Random => Box::new(RandomTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::FirstName => Box::new(FirstNameTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::Email => Box::new(EmailTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::KeepFirstChar => Box::new(KeepFirstCharTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::PhoneNumber => Box::new(PhoneNumberTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::RandomDate => todo!(),
            TransformerTypeConfig::CreditCard => Box::new(CreditCardTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::Redacted(options) => {
                let options = match options {
                    Some(options) => *options,
                    None => RedactedTransformerOptions::default(),
                };
                Box::new(RedactedTransformer::new(
                    database_name,
                    table_name,
                    column_name,
                    options,
                ))
            }
            TransformerTypeConfig::Transient => Box::new(TransientTransformer::new(
                database_name,
                table_name,
                column_name,
            )),
            TransformerTypeConfig::CustomWasm(options) => {
                let wasm_bytes = match std::fs::read(options.path.clone()) {
                    Ok(bytes) => bytes,
                    Err(err) => {
                        // The user probably provided a wrong path to the wasm file
                        panic!("Failed to read wasm file: {}", err);
                    }
                };
                let wasm_transformer =
                    CustomWasmTransformer::new(database_name, table_name, column_name, wasm_bytes);
                match wasm_transformer {
                    Ok(transformer) => Box::new(transformer),
                    Err(err) => {
                        // The wasm which the user provided is invalid
                        panic!("Failed to load custom wasm transformer: {}", err);
                    }
                }
            }
        };

        transformer
    }
}

type Host = String;
type Port = u16;
type Username = String;
type Password = String;
type Database = String;
type AuthenticationDatabase = String;

#[derive(Debug, PartialEq, Clone)]
pub enum ConnectionUri {
    Postgres(Host, Port, Username, Password, Database),
    Mysql(Host, Port, Username, Password, Database),
    MongoDB(
        Host,
        Port,
        Username,
        Password,
        Database,
        AuthenticationDatabase,
    ),
}

fn get_host(url: &Url) -> Result<String, Error> {
    match url.host() {
        Some(host) => Ok(host.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <host> property from connection uri",
        )),
    }
}

fn get_port(url: &Url, default_port: u16) -> Result<u16, Error> {
    match url.port() {
        Some(port) if port < 1 => Err(Error::new(
            ErrorKind::Other,
            "<port> from connection uri can't be lower than 0",
        )),
        Some(port) => Ok(port),
        None => Ok(default_port),
    }
}

fn get_username(url: &Url) -> Result<String, Error> {
    match url.username() {
        username if username != "" => Ok(username.to_string()),
        _ => Err(Error::new(
            ErrorKind::Other,
            "missing <username> property from connection uri",
        )),
    }
}

fn get_password(url: &Url) -> Result<String, Error> {
    match url.password() {
        Some(password) => Ok(password.to_string()),
        None => Err(Error::new(
            ErrorKind::Other,
            "missing <password> property from connection uri",
        )),
    }
}

fn get_database(url: &Url, default: Option<&str>) -> Result<String, Error> {
    let path = url.path().to_string();
    let database = path.split("/").collect::<Vec<&str>>();

    if database.is_empty() {
        return match default {
            Some(default) => Ok(default.to_string()),
            None => Err(Error::new(
                ErrorKind::Other,
                "missing <database> property from connection uri",
            )),
        };
    }

    let database = match database.get(1) {
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

fn get_mongodb_authentication_db(url: &Url) -> String {
    let hash_query: HashMap<String, String> = url.query_pairs().into_owned().collect();

    let authentication_database = match hash_query.get("authSource") {
        Some(auth_source) => auth_source.to_string(),
        None => DEFAULT_MONGODB_AUTH_DB.to_string(),
    };

    authentication_database
}

fn parse_connection_uri(uri: &str) -> Result<ConnectionUri, Error> {
    let uri = substitute_env_var(uri)?;

    let url = match Url::parse(uri.as_str()) {
        Ok(url) => url,
        Err(err) => return Err(Error::new(ErrorKind::Other, format!("{:?}", err))),
    };

    let connection_uri = match url.scheme() {
        scheme if scheme.to_lowercase() == "postgres" => ConnectionUri::Postgres(
            get_host(&url)?,
            get_port(&url, 5432)?,
            get_username(&url)?,
            get_password(&url)?,
            get_database(&url, Some("public"))?,
        ),
        scheme if scheme.to_lowercase() == "mysql" => ConnectionUri::Mysql(
            get_host(&url)?,
            get_port(&url, 3306)?,
            get_username(&url)?,
            get_password(&url)?,
            get_database(&url, None)?,
        ),
        scheme if scheme.to_lowercase() == "mongodb" || scheme.to_lowercase() == "mongodb+srv" => {
            ConnectionUri::MongoDB(
                get_host(&url)?,
                get_port(&url, 27017)?,
                get_username(&url)?,
                get_password(&url)?,
                get_database(&url, Some(DEFAULT_MONGODB_AUTH_DB))?,
                get_mongodb_authentication_db(&url),
            )
        }
        scheme => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("'{}' not supported", scheme),
            ));
        }
    };

    Ok(connection_uri)
}

#[derive(Debug, PartialEq, Serialize, Deserialize, Clone)]
pub enum Endpoint {
    #[serde(rename = "custom")]
    Custom(String),
    #[serde(rename = "default")]
    Default,
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
    use crate::config::{parse_connection_uri, substitute_env_var, ConnectionUri};

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
        assert!(parse_connection_uri("postgres://root:password@localhost:5432/db").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost:5432").is_ok());
        assert!(parse_connection_uri("postgres://root:password@localhost").is_ok());
        assert!(parse_connection_uri("postgres://root:password").is_err());
    }

    #[test]
    fn parse_mysql_connection_uri() {
        assert!(parse_connection_uri("mysql://root:password@localhost:3306/db").is_ok());
        assert!(parse_connection_uri("mysql://root:password@localhost/db").is_ok());
        assert!(parse_connection_uri("mysql://root:password@localhost").is_err());
        assert!(parse_connection_uri("mysql://root:password").is_err());
    }

    #[test]
    fn parse_mysql_connection_uri_with_db() {
        assert_eq!(
            parse_connection_uri("mysql://root:password@localhost:3306/db").unwrap(),
            ConnectionUri::Mysql(
                "localhost".to_string(),
                3306,
                "root".to_string(),
                "password".to_string(),
                "db".to_string()
            ),
        );

        assert_eq!(
            parse_connection_uri("mysql://root:password@localhost/db").unwrap(),
            ConnectionUri::Mysql(
                "localhost".to_string(),
                3306,
                "root".to_string(),
                "password".to_string(),
                "db".to_string()
            ),
        );
    }

    #[test]
    fn parse_postgres_connection_uri_with_db() {
        assert_eq!(
            parse_connection_uri("postgres://root:password@localhost:5432/db").unwrap(),
            ConnectionUri::Postgres(
                "localhost".to_string(),
                5432,
                "root".to_string(),
                "password".to_string(),
                "db".to_string(),
            ),
        )
    }

    #[test]
    fn parse_mongodb_connection_uri() {
        assert!(parse_connection_uri("mongodb://root:password").is_err());
        assert!(parse_connection_uri("mongodb://root:password@localhost:27017").is_ok());
        assert!(parse_connection_uri("mongodb://root:password@localhost:27017/db").is_ok());
        assert!(parse_connection_uri("mongodb://root:password@localhost").is_ok());
        assert!(parse_connection_uri("mongodb+srv://root:password@server.example.com/").is_ok());
    }

    #[test]
    fn parse_mongodb_connection_uri_with_db() {
        let connection_uri = parse_connection_uri(
            "mongodb+srv://root:password@server.example.com/my_db?authSource=other_db",
        )
        .unwrap();

        assert_eq!(
            connection_uri,
            ConnectionUri::MongoDB(
                "server.example.com".to_string(),
                27017,
                "root".to_string(),
                "password".to_string(),
                "my_db".to_string(),
                "other_db".to_string(),
            )
        )
    }
}
