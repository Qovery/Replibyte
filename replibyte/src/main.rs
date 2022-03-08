use crate::bridge::s3::S3;
use std::io::Error;

use crate::source::postgres::Postgres;
use crate::source::transformer::Transformer;
use crate::source::Source;
use crate::tasks::{FullBackupTask, Task};

mod bridge;
mod connector;
mod database;
mod destination;
mod source;
mod tasks;
mod types;

fn main() -> Result<(), Error> {
    let mut source = Postgres::new("postgres://root:password@localhost:5432", false);
    source.set_transformer(Transformer::None);

    let bridge = S3::new();

    let mut task = FullBackupTask::new(source, bridge);
    task.run()
}
