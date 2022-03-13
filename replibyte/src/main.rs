use crate::bridge::s3::S3;
use crate::config::{Config, ConnectionUri};
use std::fs::File;
use std::io::Error;

use crate::source::postgres::Postgres;
use crate::source::Source;
use crate::tasks::{FullBackupTask, Task};
use crate::transformer::{NoTransformer, RandomTransformer, Transformer};

mod bridge;
mod config;
mod connector;
mod database;
mod destination;
mod source;
mod tasks;
pub mod transformer;
mod types;

fn main() -> Result<(), Error> {
    // TODO implement CLI

    let file = File::open("examples/source-postgres.yaml")?; // FIXME
    let config: Config = match serde_yaml::from_reader(file) {
        Ok(config) => config,
        Err(err) => panic!("{:?}", err),
    };

    // TODO match source or destination type
    // TODO match transformers by name

    let connection_uri = config.source.connection_uri()?;

    let t1: Box<dyn Transformer> = Box::new(NoTransformer::default());
    let t2: Box<dyn Transformer> = Box::new(RandomTransformer::new("fake", "fake_column"));
    let transformers = vec![t1, t2];

    let bridge = S3::new();

    match connection_uri {
        ConnectionUri::Postgres(host, port, username, password, database) => {
            let postgres = Postgres::new(
                host.as_str(),
                port,
                database.as_str(),
                username.as_str(),
                password.as_str(),
            );

            let mut task = FullBackupTask::new(postgres, &transformers, bridge);
            task.run()
        }
        ConnectionUri::Mysql(host, port, username, password, database) => {
            todo!()
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_from_postgres() {}
}
