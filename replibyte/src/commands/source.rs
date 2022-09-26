use std::io::{Error, ErrorKind};

use crate::config::{Config, ConnectionUri};
use crate::source::Explain;
use crate::source::mongodb::MongoDB;
use crate::source::mysql::Mysql;
use crate::source::postgres::Postgres;

/// show the database schema
pub fn schema(config: Config) -> anyhow::Result<()> {
    match config.source {
        Some(source) => {
            match source.connection_uri()? {
                ConnectionUri::Postgres(host, port, username, password, database) => {
                    let postgres = Postgres::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                    );

                    postgres.schema()?;

                    Ok(())
                }
                ConnectionUri::Mysql(host, port, username, password, database) => {
                    let mysql = Mysql::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                    );

                    mysql.schema()?;

                    Ok(())
                }
                ConnectionUri::MongoDB(uri, database) => {
                    let mongodb = MongoDB::new(uri.as_str(), database.as_str());

                    mongodb.schema()?;

                    Ok(())
                }
            }
        }
        None => {
            Err(anyhow::Error::from(Error::new(
                ErrorKind::Other,
                "missing <source> object in the configuration file",
            )))
        }
    }
}
