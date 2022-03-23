use std::io::Error;
use std::sync::mpsc;
use std::thread;

use crate::bridge::{Bridge, ReadOptions};
use crate::destination::Destination;
use crate::tasks::{MaxBytes, Message, Task, TransferredBytes};
use crate::types::Bytes;

/// FullRestoreTask is a wrapping struct to execute the synchronization between a *Bridge* and a *Source*.
pub struct FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    destination: D,
    bridge: B,
    read_options: ReadOptions,
}

impl<D, B> FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    pub fn new(destination: D, bridge: B, read_options: ReadOptions) -> Self {
        FullRestoreTask {
            destination,
            bridge,
            read_options,
        }
    }
}

impl<D, B> Task for FullRestoreTask<D, B>
where
    D: Destination,
    B: Bridge + 'static,
{
    fn run<F: FnMut(TransferredBytes, MaxBytes)>(
        mut self,
        mut progress_callback: F,
    ) -> Result<(), Error> {
        // initialize the destination
        let _ = self.destination.init()?;

        // initialize the bridge
        let _ = self.bridge.init()?;

        // bound to 1 to avoid eating too much memory if we download the dump faster than we ingest it
        let (tx, rx) = mpsc::sync_channel::<Message<Bytes>>(1);
        let bridge = self.bridge;

        let mut index_file = bridge.index_file()?;
        let backup = index_file.find_backup(&self.read_options)?;

        // init progress
        progress_callback(0, backup.size);

        let read_options = self.read_options.clone();

        let join_handle = thread::spawn(move || {
            // managing Bridge (S3) download here
            let bridge = bridge;
            let read_options = read_options;

            let _ = match bridge.read(&read_options, |data| {
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

            progress_callback(data.len(), backup.size);

            let _ = self.destination.write(data)?;
        }

        // wait for end of download execution
        let _ = join_handle.join(); // FIXME catch result here

        progress_callback(backup.size, backup.size);

        Ok(())
    }
}
