use crate::bridge::s3::S3;
use std::io::Error;

use crate::source::postgres::Postgres;
use crate::source::Source;
use crate::tasks::{FullBackupTask, Task};
use crate::transformer::{NoTransformer, RandomTransformer, Transformer};

mod bridge;
mod connector;
mod database;
mod destination;
mod source;
mod tasks;
pub mod transformer;
mod types;

fn main() -> Result<(), Error> {
    // TODO parse and check yaml configuration file
    // TODO match source or destination type
    // TODO match transformers by name

    let source = Postgres::new("localhost", 5432, "root", "root", "password");

    let t1: Box<dyn Transformer> = Box::new(NoTransformer::default());
    let t2: Box<dyn Transformer> = Box::new(RandomTransformer::new("fake", "fake_column"));
    let transformers = vec![t1, t2];

    let bridge = S3::new();

    let mut task = FullBackupTask::new(source, &transformers, bridge);
    task.run()
}

#[cfg(test)]
mod tests {
    #[test]
    fn read_from_postgres() {}
}
