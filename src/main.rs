use crate::database::Database;
use std::io::{Error, ErrorKind};

use crate::source::postgres::Postgres;
use crate::source::Source;
use crate::tasks::FullBackupTask;

mod bridge;
mod connector;
mod database;
mod destination;
mod source;
mod tasks;
mod transform;

fn main() -> Result<(), Error> {
    let postgres = Postgres::new("postgres://root:password@localhost:5432", false);

    // Task::new(postgres)

    Ok(())
}
