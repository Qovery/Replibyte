use std::io::Error;
use std::sync::mpsc;
use std::thread;

use crate::bridge::Bridge;
use crate::destination::Destination;
use crate::tasks::{Message, Task};
use crate::types::Bytes;

/// FullRestoreTask is a wrapping struct to execute the synchronization between a *Bridge* and a *Source*.
pub struct FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    destination: D,
    bridge: B,
}

impl<D, B> FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    pub fn new(destination: D, bridge: B) -> Self {
        FullRestoreTask {
            destination,
            bridge,
        }
    }
}

impl<D, B> Task for FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    fn run(mut self) -> Result<(), Error> {
        // initialize the destination
        let _ = self.destination.init()?;

        // initialize the bridge
        let _ = self.bridge.init()?;

        let (tx, rx) = mpsc::sync_channel::<Message<Bytes>>(1);
        let bridge = self.bridge;

        let join_handle = thread::spawn(move || {
            // managing Bridge (S3) download here
            let bridge = bridge;

            let _ = match bridge.download(|data| {
                let _ = tx.send(Message::Data(data));
            }) {
                Ok(_) => {}
                Err(err) => panic!("{:?}", err),
            };

            let _ = tx.send(Message::EOF);
        });

        loop {
            let data = match rx.recv() {
                Ok(Message::Data(data)) => data,
                Ok(Message::EOF) => break,
                Err(err) => panic!("{:?}", err), // FIXME what should I do here?
            };

            let _ = self.destination.insert(data)?;
        }

        // wait for end of download execution
        let _ = join_handle.join(); // FIXME catch result here

        Ok(())
    }
}
