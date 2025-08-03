use std::io::{Error, ErrorKind};
use std::sync::mpsc;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::datastore::Datastore;
use crate::source::SourceOptions;
use crate::tasks::{MaxBytes, Message, Task, TransferredBytes};
use crate::types::{to_bytes, Queries, Query, OptimizedQuery};
use crate::Source;
use crate::utils::advanced_pool::get_thread_local_vec;
use crate::io::RingBuffer;

type StreamingDataMessage = (u16, Vec<OptimizedQuery>);

/// Memory-optimized streaming dump task that minimizes peak memory usage
pub struct StreamingDumpTask<'a, S>
where
    S: Source,
{
    source: S,
    datastore: Box<dyn Datastore>,
    options: SourceOptions<'a>,
    max_memory_mb: usize,
}

impl<'a, S> StreamingDumpTask<'a, S>
where
    S: Source,
{
    pub fn new(source: S, datastore: Box<dyn Datastore>, options: SourceOptions<'a>) -> Self {
        Self {
            source,
            datastore,
            options,
            max_memory_mb: 200, // Default to 200MB max memory usage
        }
    }
    
    pub fn with_memory_limit(mut self, max_memory_mb: usize) -> Self {
        self.max_memory_mb = max_memory_mb;
        self
    }
}

impl<'a, S> Task for StreamingDumpTask<'a, S>
where
    S: Source,
{
    fn run<F: FnMut(TransferredBytes, MaxBytes)>(
        mut self,
        mut progress_callback: F,
    ) -> Result<(), Error> {
        // Initialize the source
        let _ = self.source.init()?;

        // Adaptive buffer size based on memory limit
        let buffer_size = (self.max_memory_mb * 1024 * 1024) / 4; // Use 1/4 of memory limit per buffer
        let num_buffers = 4; // Keep 4 buffers in flight
        
        let (tx, rx) = mpsc::sync_channel::<Message<StreamingDataMessage>>(num_buffers);
        let datastore = self.datastore;
        
        // Shared counters for progress tracking
        let total_bytes = Arc::new(AtomicUsize::new(0));
        let processed_bytes = Arc::new(AtomicUsize::new(0));
        
        let total_bytes_clone = total_bytes.clone();
        let processed_bytes_clone = processed_bytes.clone();

        // Upload thread with batching
        let join_handle = thread::spawn(move || -> Result<(), Error> {
            let mut batch_buffer = Vec::with_capacity(buffer_size);
            let mut current_chunk = 0u16;
            
            loop {
                match rx.recv() {
                    Ok(Message::Data((chunk_part, optimized_queries))) => {
                        // Convert optimized queries to bytes efficiently
                        for query in optimized_queries {
                            batch_buffer.extend_from_slice(query.as_slice());
                            batch_buffer.push(b'\n');
                            
                            // Upload when batch buffer is full
                            if batch_buffer.len() >= buffer_size {
                                current_chunk += 1;
                                if let Err(e) = datastore.write(current_chunk, batch_buffer.clone()) {
                                    return Err(Error::new(ErrorKind::Other, format!("{}", e)));
                                }
                                processed_bytes_clone.fetch_add(batch_buffer.len(), Ordering::Relaxed);
                                batch_buffer.clear();
                            }
                        }
                    }
                    Ok(Message::EOF) => {
                        // Upload remaining data
                        if !batch_buffer.is_empty() {
                            current_chunk += 1;
                            if let Err(e) = datastore.write(current_chunk, batch_buffer) {
                                return Err(Error::new(ErrorKind::Other, format!("{}", e)));
                            }
                        }
                        break;
                    }
                    Err(err) => return Err(Error::new(ErrorKind::Other, format!("{}", err))),
                }
            }

            Ok(())
        });

        // Streaming read with memory pressure monitoring
        let mut queries_batch = Vec::with_capacity(1000);
        let mut current_batch_size = 0usize;
        let mut chunk_part = 0u16;
        let mut total_transferred_bytes = 0usize;

        // Progress callback setup
        progress_callback(0, buffer_size);

        let _ = self.source.read(self.options, |_original_query, query| {
            let query_size = query.data().len();
            
            // Create optimized query from regular query
            let optimized_query = OptimizedQuery::from_slice(query.data());
            
            // Check if adding this query would exceed buffer limit
            if current_batch_size + query_size > buffer_size && !queries_batch.is_empty() {
                // Send current batch
                chunk_part += 1;
                let message = Message::Data((chunk_part, std::mem::take(&mut queries_batch)));
                let _ = tx.send(message);
                
                // Reset for next batch
                queries_batch = Vec::with_capacity(1000);
                current_batch_size = 0;
                
                // Update progress
                total_transferred_bytes += current_batch_size;
                total_bytes.store(total_transferred_bytes + buffer_size, Ordering::Relaxed);
                progress_callback(total_transferred_bytes, total_transferred_bytes + buffer_size);
            }

            current_batch_size += query_size;
            total_transferred_bytes += query_size;
            queries_batch.push(optimized_query);
            
            // Update progress more frequently for large operations
            if queries_batch.len() % 100 == 0 {
                progress_callback(total_transferred_bytes, total_transferred_bytes + buffer_size);
            }
        })?;

        // Send final batch if not empty
        if !queries_batch.is_empty() {
            chunk_part += 1;
            let _ = tx.send(Message::Data((chunk_part, queries_batch)));
        }
        
        let _ = tx.send(Message::EOF);
        
        // Final progress update
        progress_callback(total_transferred_bytes, total_transferred_bytes);
        
        // Wait for upload completion
        join_handle.join().unwrap()?;

        Ok(())
    }
}

/// Streaming processor that processes data in fixed-size chunks to maintain constant memory usage
pub struct ConstantMemoryProcessor<R, W> {
    reader: R,
    writer: W,
    chunk_size: usize,
    ring_buffer: RingBuffer,
}

impl<R: std::io::Read, W: std::io::Write> ConstantMemoryProcessor<R, W> {
    pub fn new(reader: R, writer: W, chunk_size: usize) -> Self {
        Self {
            reader,
            writer,
            chunk_size,
            ring_buffer: RingBuffer::new(chunk_size * 2), // Double buffering
        }
    }
    
    /// Process data with constant memory usage regardless of input size
    pub fn process_constant_memory<F>(mut self, mut processor: F) -> std::io::Result<u64>
    where
        F: FnMut(&[u8]) -> Vec<u8>,
    {
        let mut total_processed = 0u64;
        let mut read_buffer = vec![0u8; self.chunk_size];
        let mut temp_buffer = vec![0u8; self.chunk_size];
        
        loop {
            // Read chunk
            match self.reader.read(&mut read_buffer)? {
                0 => break, // EOF
                n => {
                    let input_data = &read_buffer[..n];
                    
                    // Process chunk
                    let processed_data = processor(input_data);
                    
                    // Write processed data through ring buffer to handle size changes
                    let mut offset = 0;
                    while offset < processed_data.len() {
                        let written = self.ring_buffer.write(&processed_data[offset..]);
                        offset += written;
                        
                        // Flush ring buffer when it gets full
                        while self.ring_buffer.available_read() > 0 {
                            let read_count = self.ring_buffer.read(&mut temp_buffer);
                            if read_count > 0 {
                                self.writer.write_all(&temp_buffer[..read_count])?;
                                total_processed += read_count as u64;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }
        
        // Flush remaining data from ring buffer
        while !self.ring_buffer.is_empty() {
            let read_count = self.ring_buffer.read(&mut temp_buffer);
            if read_count > 0 {
                self.writer.write_all(&temp_buffer[..read_count])?;
                total_processed += read_count as u64;
            } else {
                break;
            }
        }
        
        self.writer.flush()?;
        Ok(total_processed)
    }
}

/// Memory-aware chunk processor that adapts chunk sizes based on available memory
pub struct AdaptiveChunkProcessor {
    base_chunk_size: usize,
    max_chunk_size: usize,
    memory_threshold: usize,
}

impl AdaptiveChunkProcessor {
    pub fn new(base_chunk_size: usize, max_chunk_size: usize, memory_threshold_mb: usize) -> Self {
        Self {
            base_chunk_size,
            max_chunk_size,
            memory_threshold: memory_threshold_mb * 1024 * 1024,
        }
    }
    
    /// Get optimal chunk size based on current memory usage
    pub fn get_optimal_chunk_size(&self) -> usize {
        // In a real implementation, you would check actual memory usage
        // For now, we'll use a simple heuristic
        let current_memory_usage = self.estimate_memory_usage();
        
        if current_memory_usage > self.memory_threshold {
            // Reduce chunk size under memory pressure
            std::cmp::max(self.base_chunk_size / 2, 1024)
        } else if current_memory_usage < self.memory_threshold / 2 {
            // Increase chunk size when memory is abundant
            std::cmp::min(self.max_chunk_size, self.base_chunk_size * 2)
        } else {
            self.base_chunk_size
        }
    }
    
    fn estimate_memory_usage(&self) -> usize {
        // Placeholder for actual memory usage detection
        // In practice, you could use system APIs or memory profilers
        self.base_chunk_size * 4 // Rough estimate
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_constant_memory_processor() {
        let input = b"hello world ".repeat(1000);
        let reader = Cursor::new(&input);
        let mut output = Vec::new();
        
        let processor = ConstantMemoryProcessor::new(reader, &mut output, 64);
        
        let processed = processor.process_constant_memory(|data| {
            data.iter().map(|&b| b.to_ascii_uppercase()).collect()
        }).unwrap();
        
        assert_eq!(processed, input.len() as u64);
        
        let expected: Vec<u8> = input.iter().map(|&b| b.to_ascii_uppercase()).collect();
        assert_eq!(output, expected);
    }
    
    #[test]
    fn test_adaptive_chunk_processor() {
        let processor = AdaptiveChunkProcessor::new(1024, 8192, 100);
        let chunk_size = processor.get_optimal_chunk_size();
        assert!(chunk_size >= 1024);
        assert!(chunk_size <= 8192);
    }
}