use std::io::Error;
use std::sync::mpsc;
use std::thread;

use crate::datastore::{Datastore, ReadOptions};
use crate::destination::Destination;
use crate::tasks::{MaxBytes, Message, Task, TransferredBytes};
use crate::types::Bytes;

/// FullRestoreTask is a wrapping struct to execute the synchronization between a *Datastore* and a *Source*.
pub struct FullRestoreTask<'a, D>
where
    D: Destination,
{
    destination: &'a mut D,
    datastore: Box<dyn Datastore>,
    read_options: ReadOptions,
}

impl<'a, D> FullRestoreTask<'a, D>
where
    D: Destination,
{
    pub fn new(
        destination: &'a mut D,
        datastore: Box<dyn Datastore>,
        read_options: ReadOptions,
    ) -> Self {
        FullRestoreTask {
            destination,
            datastore,
            read_options,
        }
    }
}

impl<'a, D> Task for FullRestoreTask<'a, D>
where
    D: Destination,
{
    fn run<F: FnMut(TransferredBytes, MaxBytes)>(
        self,
        mut progress_callback: F,
    ) -> Result<(), Error> {
        // initialize the destination
        let _ = self.destination.init()?;

        // bound to 1 to avoid eating too much memory if we download the dump faster than we ingest it
        let (tx, rx) = mpsc::sync_channel::<Message<Bytes>>(1);
        let datastore = self.datastore;

        let mut index_file = datastore.index_file()?;
        let dump = index_file.find_dump(&self.read_options)?;

        // init progress
        progress_callback(0, dump.size);

        let read_options = self.read_options.clone();

        let join_handle = thread::spawn(move || {
            // managing Datastore (S3) download here
            let datastore = datastore;
            let read_options = read_options;

            let _ = match datastore.read(&read_options, &mut |data| {
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

            progress_callback(data.len(), dump.size);

            let _ = self.destination.write(data)?;
        }

        // wait for end of download execution
        let _ = join_handle.join(); // FIXME catch result here

        progress_callback(dump.size, dump.size);

        Ok(())
    }
}
