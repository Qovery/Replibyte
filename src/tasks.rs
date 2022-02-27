use crate::bridge::Bridge;
use crate::destination::Destination;
use crate::transform::Transformer;
use crate::Source;
use std::io::Error;

pub trait Task {
    fn run(&mut self) -> Result<(), Error>;
}

/// FullBackupTask is a wrapping struct to execute the synchronization between a *Source* and a *Bridge*
pub struct FullBackupTask<S, B>
where
    S: Source,
    B: Bridge,
{
    source: S,
    bridge: B,
    transformer: Transformer,
}

impl<S, B> FullBackupTask<S, B>
where
    S: Source,
    B: Bridge,
{
    pub fn new(source: S, bridge: B, transformer: Transformer) -> Self {
        FullBackupTask {
            source,
            bridge,
            transformer,
        }
    }
}

impl<S, B> Task for FullBackupTask<S, B>
where
    S: Source,
    B: Bridge,
{
    fn run(&mut self) -> Result<(), Error> {
        // initialize the source
        let _ = self.source.init()?;

        // start
        // TODO initialize the destination
        // TODO initialize the bridge

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
