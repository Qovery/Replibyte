use replibyte::bridge::s3::S3;
use replibyte::config::{Config, ConnectionUri};
use replibyte::source::postgres::Postgres;
use replibyte::tasks::{FullBackupTask, Task};
use std::fs::File;
use std::io::Error;
use std::path::PathBuf;
use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    #[clap(short, long, parse(from_os_str), value_name = "configuration file")]
    config: PathBuf,
    // TODO list available transformers
}

fn main() -> Result<(), Error> {
    let args = Args::parse();

    let file = File::open(args.config)?; // FIXME
    let config: Config = match serde_yaml::from_reader(file) {
        Ok(config) => config,
        Err(err) => panic!("{:?}", err),
    };

    // match transformers from conf
    let transformers = config
        .source
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

    let bridge = S3::new();

    match config.source.connection_uri()? {
        ConnectionUri::Postgres(host, port, username, password, database) => {
            let postgres = Postgres::new(
                host.as_str(),
                port,
                database.as_str(),
                username.as_str(),
                password.as_str(),
            );

            let task = FullBackupTask::new(postgres, &transformers, bridge);
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
