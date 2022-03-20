use std::fs::File;
use std::path::PathBuf;

use clap::Parser;

use crate::bridge::s3::S3;
use crate::config::{Config, ConnectionUri, ConnectorConfig};
use crate::destination::postgres::Postgres as DestinationPostgres;
use crate::source::postgres::Postgres as SourcePostgres;
use crate::source::Source;
use crate::tasks::full_backup::FullBackupTask;
use crate::tasks::full_restore::FullRestoreTask;
use crate::tasks::Task;

mod bridge;
mod config;
mod connector;
mod destination;
mod runtime;
mod source;
mod tasks;
mod transformer;
mod types;
mod utils;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, parse(from_os_str), value_name = "configuration file")]
    config: PathBuf,
    // TODO: List available transformers
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = Args::parse();

    // ! TODO: Fix this line.
    let file = File::open(args.config)?;
    let config: Config = serde_yaml::from_reader(file)?;

    let bridge = S3::new(
        config.bridge.bucket()?,
        config.bridge.region()?,
        config.bridge.access_key_id()?,
        config.bridge.secret_access_key()?,
    );

    match config.connector()? {
        ConnectorConfig::Source(source) => {
            // Match the transformers from the config
            let transformers = source
                .transformers
                .iter()
                .flat_map(|transformer| {
                    transformer.columns.iter().map(|column| {
                        column.transformer.transformer(
                            transformer.database.as_str(),
                            transformer.table.as_str(),
                            column.name.as_str(),
                        )
                    })
                })
                .collect::<Vec<_>>();

            match source.connection_uri()? {
                ConnectionUri::Postgres(host, port, username, password, database) => {
                    let postgres = SourcePostgres::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                    );

                    let task = FullBackupTask::new(postgres, &transformers, bridge);
                    task.run()?
                }
                ConnectionUri::Mysql(host, port, username, password, database) => {
                    todo!()
                }
            }
        }
        ConnectorConfig::Destination(destination) => match destination.connection_uri()? {
            ConnectionUri::Postgres(host, port, username, password, database) => {
                let postgres = DestinationPostgres::new(
                    host.as_str(),
                    port,
                    database.as_str(),
                    username.as_str(),
                    password.as_str(),
                    true,
                );

                let task = FullRestoreTask::new(postgres, bridge);
                task.run()?
            }
            ConnectionUri::Mysql(host, port, username, password, database) => {
                todo!()
            }
        },
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_from_postgres() {}
}
