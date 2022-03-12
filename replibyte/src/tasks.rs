use crate::bridge::Bridge;
use crate::transformer::Transformer;
use crate::Source;
use std::io::Error;

pub trait Task {
    fn run(&mut self) -> Result<(), Error>;
}

/// FullBackupTask is a wrapping struct to execute the synchronization between a *Source* and a *Bridge*
pub struct FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge,
{
    source: S,
    transformers: &'a Vec<Box<dyn Transformer>>,
    bridge: B,
}

impl<'a, S, B> FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge,
{
    pub fn new(source: S, transformers: &'a Vec<Box<dyn Transformer>>, bridge: B) -> Self {
        FullBackupTask {
            source,
            transformers,
            bridge,
        }
    }
}

impl<'a, S, B> Task for FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge,
{
    fn run(&mut self) -> Result<(), Error> {
        // initialize the source
        let _ = self.source.init()?;

        // start
        // TODO initialize the bridge

        let _ = self
            .source
            .stream_rows(self.transformers, |original_row, row| {
                // TODO
                println!("{}", row.table_name.as_str());
            });

        // business execution
        // TODO find source last checkpoint
        // TODO find destination last checkpoint from bridge

        // final
        // TODO close source
        // TODO close destination
        // TODO close bridge

        Ok(())
    }
}
