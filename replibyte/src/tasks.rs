use crate::bridge::Bridge;
use crate::transformer::Transformer;
use crate::source::Source;
use crate::types::Queries;
use std::io::Error;
use std::sync::mpsc;
use std::thread;

pub trait Task {
    fn run(self) -> Result<(), Error>;
}

/// inter-thread message for Source and Bridge
#[derive(Debug, Clone)]
enum Message {
    Data { chunk_part: u16, queries: Queries },
    EOF,
}

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

        // start
        // TODO initialize the bridge

        let (tx, rx) = mpsc::sync_channel::<Message>(1);
        let bridge = self.bridge;

        let join_handle = thread::spawn(move || {
            // managing Bridge (S3) upload here
            let bridge = bridge;

            // TODO get the value of rx and upload to bridge
            loop {
                let (chunk_part, queries) = match rx.recv() {
                    Ok(Message::Data {
                        chunk_part,
                        queries,
                    }) => (chunk_part, queries),
                    Ok(Message::EOF) => break,
                    Err(err) => panic!("{:?}", err), // FIXME what should I do here?
                };

                let _ = match bridge.upload(chunk_part, &queries) {
                    Ok(_) => {}
                    Err(err) => {} // FIXME what should we do?
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
            .stream_dump_queries(self.transformers, |original_query, query| {
                if consumed_buffer_size + query.data().len() > buffer_size {
                    chunk_part += 1;
                    consumed_buffer_size = 0;
                    // TODO .clone() - look if we do not consume more mem

                    let message = Message::Data {
                        chunk_part,
                        queries: queries.clone(),
                    };

                    let _ = tx.send(message); // FIXME catch SendError?
                    let _ = queries.clear();
                }

                consumed_buffer_size += query.data().len();
                queries.push(query);
            });

        chunk_part += 1;
        let _ = tx.send(Message::Data {
            chunk_part,
            queries,
        });

        let _ = tx.send(Message::EOF);
        // wait for end of upload execution
        let _ = join_handle.join(); // FIXME catch result here

        Ok(())
    }
}
