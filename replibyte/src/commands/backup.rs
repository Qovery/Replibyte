use std::fs::File;
use std::io::{stdin, BufReader, Error, ErrorKind, Read};
use std::time::Duration;

use timeago::Formatter;

use crate::cli::{DumpCreateArgs, DumpDeleteArgs};
use crate::config::{Config, ConnectionUri};
use crate::datastore::Datastore;

use crate::source::mongodb::MongoDB;
use crate::source::mysql::Mysql;
use crate::source::mysql_stdin::MysqlStdin;
use crate::source::postgres::Postgres;
use crate::source::postgres_stdin::PostgresStdin;
use crate::source::SourceOptions;
use crate::tasks::full_backup::FullBackupTask;
use crate::tasks::Task;
use crate::utils::{epoch_millis, table, to_human_readable_unit};

/// Display all backups
pub fn list(datastore: &mut Box<dyn Datastore>) -> Result<(), Error> {
    let _ = datastore.init()?;
    let mut index_file = datastore.index_file()?;

    if index_file.backups.is_empty() {
        println!("<empty> no backups available\n");
        return Ok(());
    }

    index_file.backups.sort_by(|a, b| a.cmp(b).reverse());

    let mut table = table();
    table.set_titles(row!["name", "size", "when", "compressed", "encrypted"]);
    let formatter = Formatter::new();
    let now = epoch_millis();

    for backup in index_file.backups {
        table.add_row(row![
            backup.directory_name.as_str(),
            to_human_readable_unit(backup.size),
            formatter.convert(Duration::from_millis((now - backup.created_at) as u64)),
            backup.compressed,
            backup.encrypted,
        ]);
    }

    let _ = table.printstd();

    Ok(())
}

// Run a new backup
pub fn run<F>(
    args: &DumpCreateArgs,
    mut datastore: Box<dyn Datastore>,
    config: Config,
    progress_callback: F,
) -> anyhow::Result<()>
where
    F: Fn(usize, usize) -> (),
{
    if let Some(encryption_key) = config.encryption_key()? {
        datastore.set_encryption_key(encryption_key)
    }

    match config.source {
        Some(source) => {
            // Configure datastore options (compression is enabled by default)
            datastore.set_compression(source.compression.unwrap_or(true));

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

            let empty_config = vec![];
            let skip_config = match &source.skip {
                Some(config) => config,
                None => &empty_config,
            };

            let options = SourceOptions {
                transformers: &transformers,
                skip_config: &skip_config,
                database_subset: &source.database_subset,
            };

            match args.source_type.as_ref().map(|x| x.as_str()) {
                None => match source.connection_uri()? {
                    ConnectionUri::Postgres(host, port, username, password, database) => {
                        let postgres = Postgres::new(
                            host.as_str(),
                            port,
                            database.as_str(),
                            username.as_str(),
                            password.as_str(),
                        );

                        let task = FullBackupTask::new(postgres, datastore, options);
                        task.run(progress_callback)?
                    }
                    ConnectionUri::Mysql(host, port, username, password, database) => {
                        let mysql = Mysql::new(
                            host.as_str(),
                            port,
                            database.as_str(),
                            username.as_str(),
                            password.as_str(),
                        );

                        let task = FullBackupTask::new(mysql, datastore, options);
                        task.run(progress_callback)?
                    }
                    ConnectionUri::MongoDB(
                        host,
                        port,
                        username,
                        password,
                        database,
                        authentication_db,
                    ) => {
                        let mongodb = MongoDB::new(
                            host.as_str(),
                            port,
                            database.as_str(),
                            username.as_str(),
                            password.as_str(),
                            authentication_db.as_str(),
                        );

                        let task = FullBackupTask::new(mongodb, datastore, options);
                        task.run(progress_callback)?
                    }
                },
                // some user use "postgres" and "postgresql" both are valid
                Some(v) if v == "postgres" || v == "postgresql" => {
                    if args.file.is_some() {
                        let dump_file = File::open(args.file.as_ref().unwrap())?;
                        let mut stdin = stdin(); // FIXME
                        let reader = BufReader::new(dump_file);
                        let _ = stdin.read_to_end(&mut reader.buffer().to_vec())?;
                    }

                    let postgres = PostgresStdin::default();
                    let task = FullBackupTask::new(postgres, datastore, options);
                    task.run(progress_callback)?
                }
                Some(v) if v == "mysql" => {
                    if args.file.is_some() {
                        let dump_file = File::open(args.file.as_ref().unwrap())?;
                        let mut stdin = stdin(); // FIXME
                        let reader = BufReader::new(dump_file);
                        let _ = stdin.read_to_end(&mut reader.buffer().to_vec())?;
                    }

                    let mysql = MysqlStdin::default();
                    let task = FullBackupTask::new(mysql, datastore, options);
                    task.run(progress_callback)?
                }
                Some(v) => {
                    return Err(anyhow::Error::from(Error::new(
                        ErrorKind::Other,
                        format!("source type '{}' not recognized", v),
                    )));
                }
            }

            println!("Backup successful!");
            Ok(())
        }
        None => {
            return Err(anyhow::Error::from(Error::new(
                ErrorKind::Other,
                "missing <source> object in the configuration file",
            )));
        }
    }
}

pub fn delete(datastore: Box<dyn Datastore>, args: &DumpDeleteArgs) -> anyhow::Result<()> {
    let _ = datastore.delete(args)?;
    println!("Backup deleted!");
    Ok(())
}
