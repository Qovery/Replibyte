use std::io::Error;
use std::sync::mpsc;
use std::thread;

use crate::bridge::Bridge;
use crate::tasks::{Message, Task};
use crate::transformer::Transformer;
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
    transformers: &'a Vec<Box<dyn Transformer>>,
    bridge: B,
}

impl<'a, S, B> FullBackupTask<'a, S, B>
where
    S: Source,
    B: Bridge + 'static,
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
    B: Bridge + 'static,
{
    fn run(mut self) -> Result<(), Error> {
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

                let _ = match bridge.upload(chunk_part, to_bytes(queries)) {
                    Ok(_) => {}
                    Err(err) => {
                        panic!("{:?}", err);
                    } // FIXME what should we do?
                };
            }
        });

        // buffer of 50MB in memory to use and re-use to upload data into bridge
        let buffer_size = 50 * 1024 * 1024;
        let mut queries = vec![];
        let mut consumed_buffer_size = 0usize;
        let mut chunk_part = 0u16;

        let _ = self
            .source
            .read(self.transformers, |original_query, query| {
                if consumed_buffer_size + query.data().len() > buffer_size {
                    chunk_part += 1;
                    consumed_buffer_size = 0;
                    // TODO .clone() - look if we do not consume more mem

                    let message = Message::Data((chunk_part, queries.clone()));

                    let _ = tx.send(message); // FIXME catch SendError?
                    let _ = queries.clear();
                }

                consumed_buffer_size += query.data().len();
                queries.push(query);
            });

        chunk_part += 1;
        let _ = tx.send(Message::Data((chunk_part, queries)));
        let _ = tx.send(Message::EOF);
        // wait for end of upload execution
        let _ = join_handle.join(); // FIXME catch result here

        Ok(())
    }
}
