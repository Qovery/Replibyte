use std::io::{Error, ErrorKind};
use std::sync::mpsc;

use crate::bridge::s3::S3;
use crate::bridge::{Bridge, ReadOptions};
use crate::cli::{RestoreArgs, RestoreLocalArgs};
use crate::config::{Config, ConnectionUri};
use crate::destination::mongodb::MongoDB;
use crate::destination::mongodb_docker::{MongoDBDocker, DEFAULT_MONGO_CONTAINER_PORT};
use crate::destination::mysql::Mysql;
use crate::destination::mysql_docker::{
    MysqlDocker, DEFAULT_MYSQL_CONTAINER_PORT, DEFAULT_MYSQL_IMAGE_TAG,
};
use crate::destination::postgres::Postgres;
use crate::destination::postgres_docker::{
    PostgresDocker, DEFAULT_POSTGRES_CONTAINER_PORT, DEFAULT_POSTGRES_DB,
    DEFAULT_POSTGRES_IMAGE_TAG, DEFAULT_POSTGRES_PASSWORD, DEFAULT_POSTGRES_USER,
};
use crate::destination::postgres_stdout::PostgresStdout;
use crate::tasks::full_restore::FullRestoreTask;
use crate::tasks::Task;

/// Restore a backup in a local container
pub fn local<F, B>(
    args: &RestoreLocalArgs,
    mut bridge: B,
    config: Config,
    progress_callback: F,
) -> anyhow::Result<()>
where
    F: Fn(usize, usize) -> (),
    B: Bridge + 'static,
{
    if let Some(encryption_key) = config.encryption_key()? {
        bridge.set_encryption_key(encryption_key);
    }

    let options = match args.value.as_str() {
        "latest" => ReadOptions::Latest,
        v => ReadOptions::Backup {
            name: v.to_string(),
        },
    };

    if args.image == "postgres".to_string() || args.image == "postgresql".to_string() {
        let port = args.port.unwrap_or(DEFAULT_POSTGRES_CONTAINER_PORT);
        let tag = match &args.tag {
            Some(tag) => tag,
            None => DEFAULT_POSTGRES_IMAGE_TAG,
        };

        let mut postgres = PostgresDocker::new(tag.to_string(), port);
        let task = FullRestoreTask::new(&mut postgres, bridge, options);
        let _ = task.run(progress_callback)?;

        print_connection_string_and_wait(
            "To connect to your Postgres database, use the following connection string:",
            &format!(
                "postgres://{}:{}@localhost:{}/{}",
                DEFAULT_POSTGRES_USER, DEFAULT_POSTGRES_PASSWORD, port, DEFAULT_POSTGRES_DB
            ),
        );

        match postgres.container {
            Some(container) => {
                if args.remove {
                    match container.rm() {
                        Ok(_) => {
                            println!("Container removed!");
                            return Ok(());
                        }
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                }

                match container.stop() {
                    Ok(_) => {
                        println!("container stopped!");
                        return Ok(());
                    }
                    Err(err) => return Err(anyhow::Error::from(err)),
                }
            }
            None => {
                return Err(anyhow::Error::from(Error::new(
                    ErrorKind::Other,
                    "command error: unable to retrieve container ID",
                )))
            }
        }
    }

    if args.image == "mongodb".to_string() {
        let port = args.port.unwrap_or(DEFAULT_MONGO_CONTAINER_PORT);
        let tag = match &args.tag {
            Some(tag) => tag,
            None => crate::destination::mongodb_docker::DEFAULT_MONGO_IMAGE_TAG,
        };

        let mut mongodb = MongoDBDocker::new(tag.to_string(), port);
        let task = FullRestoreTask::new(&mut mongodb, bridge, options);
        let _ = task.run(progress_callback)?;

        print_connection_string_and_wait(
            "To connect to your MongoDB database, use the following connection string:",
            &format!("mongodb://root:password@localhost:{}/root", port),
        );

        match mongodb.container {
            Some(container) => {
                if args.remove {
                    match container.rm() {
                        Ok(_) => {
                            println!("Container removed!");
                            return Ok(());
                        }
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                }

                match container.stop() {
                    Ok(_) => {
                        println!("container stopped!");
                        return Ok(());
                    }
                    Err(err) => return Err(anyhow::Error::from(err)),
                }
            }
            None => {
                return Err(anyhow::Error::from(Error::new(
                    ErrorKind::Other,
                    "command error: unable to retrieve container ID",
                )))
            }
        }
    }

    if args.image == "mysql".to_string() {
        let port = args.port.unwrap_or(DEFAULT_MYSQL_CONTAINER_PORT);
        let tag = match &args.tag {
            Some(tag) => tag,
            None => DEFAULT_MYSQL_IMAGE_TAG,
        };

        let mut mysql = MysqlDocker::new(tag.to_string(), port);
        let task = FullRestoreTask::new(&mut mysql, bridge, options);
        let _ = task.run(progress_callback)?;

        print_connection_string_and_wait(
            "To connect to your MySQL database, use the following connection string:",
            &format!("mysql://root:password@127.0.0.1:{}/root", port),
        );

        match mysql.container {
            Some(container) => {
                if args.remove {
                    match container.rm() {
                        Ok(_) => {
                            println!("Container removed!");
                            return Ok(());
                        }
                        Err(err) => return Err(anyhow::Error::from(err)),
                    }
                }

                match container.stop() {
                    Ok(_) => {
                        println!("container stopped!");
                        return Ok(());
                    }
                    Err(err) => return Err(anyhow::Error::from(err)),
                }
            }
            None => {
                return Err(anyhow::Error::from(Error::new(
                    ErrorKind::Other,
                    "command error: unable to retrieve container ID",
                )))
            }
        }
    }

    Ok(())
}

/// Restore a backup in the configured destination
pub fn remote<F, B>(
    args: &RestoreArgs,
    mut bridge: B,
    config: Config,
    progress_callback: F,
) -> anyhow::Result<()>
where
    F: Fn(usize, usize) -> (),
    B: Bridge + 'static,
{
    if let Some(encryption_key) = config.encryption_key()? {
        bridge.set_encryption_key(encryption_key);
    }

    match config.destination {
        Some(destination) => {
            let options = match args.value.as_str() {
                "latest" => ReadOptions::Latest,
                v => ReadOptions::Backup {
                    name: v.to_string(),
                },
            };

            if args.output {
                let mut postgres = PostgresStdout::default();
                let task = FullRestoreTask::new(&mut postgres, bridge, options);
                let _ = task.run(|_, _| {})?; // do not display the progress bar
                return Ok(());
            }

            match destination.connection_uri()? {
                ConnectionUri::Postgres(host, port, username, password, database) => {
                    let mut postgres = Postgres::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                        true,
                    );

                    let task = FullRestoreTask::new(&mut postgres, bridge, options);
                    task.run(progress_callback)?
                }
                ConnectionUri::Mysql(host, port, username, password, database) => {
                    let mut mysql = Mysql::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                    );
                    let task = FullRestoreTask::new(&mut mysql, bridge, options);
                    task.run(progress_callback)?;
                }
                ConnectionUri::MongoDB(
                    host,
                    port,
                    username,
                    password,
                    database,
                    authentication_db,
                ) => {
                    let mut mongodb = MongoDB::new(
                        host.as_str(),
                        port,
                        database.as_str(),
                        username.as_str(),
                        password.as_str(),
                        authentication_db.as_str(),
                    );

                    let task = FullRestoreTask::new(&mut mongodb, bridge, options);
                    task.run(progress_callback)?
                }
            }

            println!("Restore successful!");
            Ok(())
        }
        None => {
            return Err(anyhow::Error::from(Error::new(
                ErrorKind::Other,
                "missing <destination> object in the configuration file",
            )));
        }
    }
}

fn wait_until_ctrlc(msg: &str) {
    let (tx, rx) = mpsc::channel();
    ctrlc::set_handler(move || tx.send(()).expect("cound not send signal on channel"))
        .expect("Error setting Ctrl-C handler");
    println!("{}", msg);
    rx.recv().expect("Could not receive from channel.");
}

fn print_connection_string_and_wait(msg: &str, connection_string: &str) {
    println!("{}", msg);
    println!("> {}", connection_string);
    wait_until_ctrlc("Waiting for Ctrl-C to stop the container");
}
