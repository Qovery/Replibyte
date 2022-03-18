use replibyte::bridge::s3::S3;
use replibyte::config::{Config, ConnectionUri};
use replibyte::source::mysql::Mysql;
use std::fs::File;
use std::path::Path;

#[test]
fn test_source_connection_uri() {
    let input = Path::new("../examples/source-mysql.yaml");
    let file = File::open(input).expect("file not found");

    let config: Config = serde_yaml::from_reader(file).expect("unexpected structure of source");

    let _transformers = config
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

    let _bridge = S3::new();

    match config.source.connection_uri().expect("invalid URI scheme") {
        ConnectionUri::Mysql(host, port, username, password, database) => {
            let _mysql = Mysql::new(
                host.as_str(),
                port,
                database.as_str(),
                username.as_str(),
                password.as_str(),
            );
            todo!();
        }
        _ => panic!("not mysql connection URI"),
    }
}
