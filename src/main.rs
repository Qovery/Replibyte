use crate::bridge::s3::S3;
use crate::database::Database;
use std::io::{Error, ErrorKind};

use crate::source::postgres::Postgres;
use crate::source::Source;
use crate::tasks::{FullBackupTask, Task};
use crate::transform::Transformer;

mod bridge;
mod connector;
mod database;
mod destination;
mod source;
mod tasks;
mod transform;

fn main() -> Result<(), Error> {
    let source = Postgres::new("postgres://root:password@localhost:5432", false);
    let bridge = S3 {};

    let mut task = FullBackupTask::new(source, bridge, Transformer::None);
    task.run()
}
