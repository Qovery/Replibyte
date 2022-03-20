#[macro_use]
extern crate prettytable;

use std::fs::File;
use std::io::{Error, ErrorKind};
use std::time::Duration;

use clap::Parser;
use timeago::Formatter;

use utils::to_human_readable_unit;

use crate::bridge::s3::S3;
use crate::bridge::{Bridge, DownloadOptions};
use crate::cli::{BackupCommand, SubCommand, CLI};
use crate::config::{Config, ConnectionUri};
use crate::destination::postgres::Postgres as DestinationPostgres;
use crate::source::postgres::Postgres as SourcePostgres;
use crate::source::Source;
use crate::tasks::full_backup::FullBackupTask;
use crate::tasks::full_restore::FullRestoreTask;
use crate::tasks::Task;
use crate::utils::{epoch_millis, table};

mod bridge;
mod cli;
mod config;
mod connector;
mod destination;
mod runtime;
mod source;
mod tasks;
mod transformer;
mod types;
mod utils;

fn list_backups(s3: &S3) -> Result<(), Error> {
    let mut index_file = s3.index_file()?;
    index_file.backups.sort_by(|a, b| a.cmp(b).reverse());

    let mut table = table();
    table.set_titles(row!["name", "size", "when"]);
    let formatter = Formatter::new();
    let now = epoch_millis();

    for backup in index_file.backups {
        table.add_row(row![
            backup.directory_name.as_str(),
            to_human_readable_unit(backup.size),
            formatter.convert(Duration::from_millis((now - backup.created_at) as u64)),
        ]);
    }

    table.printstd();

    Ok(())
}

fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = CLI::parse();

    let file = File::open(args.config)?;
    let config: Config = serde_yaml::from_reader(file)?;

    let bridge = S3::new(
        config.bridge.bucket()?,
        config.bridge.region()?,
        config.bridge.access_key_id()?,
        config.bridge.secret_access_key()?,
    );

    let sub_commands: &SubCommand = &args.sub_commands;
    match sub_commands {
        SubCommand::Backup(cmd) => match cmd {
            BackupCommand::List => {
                let _ = list_backups(&bridge)?;
            }
            BackupCommand::Launch => match config.source {
                Some(source) => {
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

                    println!("Backup successful!")
                }
                None => {
                    return Err(anyhow::Error::from(Error::new(
                        ErrorKind::Other,
                        "missing <source> object in the configuration file",
                    )));
                }
            },
        },
        SubCommand::Restore(cmd) => match config.destination {
            Some(destination) => {
                match destination.connection_uri()? {
                    ConnectionUri::Postgres(host, port, username, password, database) => {
                        let postgres = DestinationPostgres::new(
                            host.as_str(),
                            port,
                            database.as_str(),
                            username.as_str(),
                            password.as_str(),
                            true,
                        );

                        let options = match cmd.value.as_str() {
                            "latest" => DownloadOptions::Latest,
                            v => DownloadOptions::Backup {
                                name: v.to_string(),
                            },
                        };

                        let task = FullRestoreTask::new(postgres, bridge, options);
                        task.run()?
                    }
                    ConnectionUri::Mysql(host, port, username, password, database) => {
                        todo!()
                    }
                }

                println!("Restore successful!")
            }
            None => {
                return Err(anyhow::Error::from(Error::new(
                    ErrorKind::Other,
                    "missing <destination> object in the configuration file",
                )));
            }
        },
    };

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_from_postgres() {}
}
