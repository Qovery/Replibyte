use std::io::Error;
use std::sync::mpsc;
use std::thread;

use crate::bridge::Bridge;
use crate::source::SourceOptions;
use crate::tasks::{MaxBytes, Message, Task, TransferredBytes};
use crate::types::{to_bytes, Queries};
use crate::Source;

type DataMessage = (u16, Queries);

/// FullBackupTask is a wrapping struct to execute the synchronization between a *Source* and a *Bridge*
pub struct FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge + 'static,
{
    source: S,
    bridge: B,
    options: SourceOptions<'a>,
}

impl<'a, S, B> FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge + 'static,
{
    pub fn new(source: S, bridge: B, options: SourceOptions<'a>) -> Self {
        FullBackupTask {
            source,
            bridge,
            options,
        }
    }
}

impl<'a, S, B> Task for FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge + 'static,
{
    fn run<F: FnMut(TransferredBytes, MaxBytes)>(
        mut self,
        mut progress_callback: F,
    ) -> Result<(), Error> {
        // initialize the source
        let _ = self.source.init()?;

        // initialize the bridge
        let _ = self.bridge.init()?;

        let (tx, rx) = mpsc::sync_channel::<Message<DataMessage>>(1);
        let bridge = self.bridge;

        let join_handle = thread::spawn(move || {
            // managing Bridge (S3) upload here
            let bridge = bridge;

            loop {
                let (chunk_part, queries) = match rx.recv() {
                    Ok(Message::Data((chunk_part, queries))) => (chunk_part, queries),
                    Ok(Message::EOF) => break,
                    Err(err) => panic!("{:?}", err), // FIXME what should I do here?
                };

                let _ = match bridge.write(chunk_part, to_bytes(queries)) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("{:?}", err);
                    } // FIXME what should we do?
                };
            }
        });

        // buffer of 100MB in memory to use and re-use to upload data into bridge
        let buffer_size = 100 * 1024 * 1024;
        let mut queries = vec![];
        let mut consumed_buffer_size = 0usize;
        let mut total_transferred_bytes = 0usize;
        let mut chunk_part = 0u16;

        // init progress
        progress_callback(
            total_transferred_bytes,
            buffer_size * (chunk_part as usize + 1),
        );

        let _ = self.source.read(self.options, |original_query, query| {
            if consumed_buffer_size + query.data().len() > buffer_size {
                chunk_part += 1;
                consumed_buffer_size = 0;
                // TODO .clone() - look if we do not consume more mem

                let message = Message::Data((chunk_part, queries.clone()));

                let _ = tx.send(message); // FIXME catch SendError?
                let _ = queries.clear();
            }

            consumed_buffer_size += query.data().len();
            total_transferred_bytes += query.data().len();
            progress_callback(
                total_transferred_bytes,
                buffer_size * (chunk_part as usize + 1),
            );
            queries.push(query);
        });

        progress_callback(total_transferred_bytes, total_transferred_bytes);

        chunk_part += 1;
        let _ = tx.send(Message::Data((chunk_part, queries)));
        let _ = tx.send(Message::EOF);
        // wait for end of upload execution
        let _ = join_handle.join(); // FIXME catch result here

        Ok(())
    }
}
