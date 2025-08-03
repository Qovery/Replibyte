use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::sync::mpsc;
use std::thread;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::utils::advanced_pool::{AlignedBuffer, get_thread_local_vec, PooledObject};

/// High-performance buffered reader with configurable buffer sizes
pub struct OptimizedBufReader<R> {
    inner: R,
    buffer: AlignedBuffer,
    pos: usize,
    cap: usize,
}

impl<R: Read> OptimizedBufReader<R> {
    pub fn new(inner: R) -> Self {
        Self::with_capacity(64 * 1024, inner)
    }
    
    pub fn with_capacity(capacity: usize, inner: R) -> Self {
        Self {
            inner,
            buffer: AlignedBuffer::new(capacity),
            pos: 0,
            cap: 0,
        }
    }
    
    fn fill_buffer(&mut self) -> io::Result<usize> {
        if self.pos >= self.cap {
            self.pos = 0;
            self.cap = 0;
        }
        
        if self.pos < self.cap {
            return Ok(self.cap - self.pos);
        }
        
        // Read into aligned buffer for better performance
        let buffer_slice = unsafe {
            std::slice::from_raw_parts_mut(
                self.buffer.as_mut_slice().as_mut_ptr(),
                self.buffer.capacity()
            )
        };
        
        match self.inner.read(buffer_slice) {
            Ok(0) => Ok(0),
            Ok(n) => {
                self.cap = n;
                self.pos = 0;
                Ok(n)
            }
            Err(e) => Err(e),
        }
    }
}

impl<R: Read> Read for OptimizedBufReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.pos >= self.cap && buf.len() >= self.buffer.capacity() {
            // Large read, bypass buffer
            return self.inner.read(buf);
        }
        
        let available = self.cap - self.pos;
        if available == 0 {
            self.fill_buffer()?;
            if self.cap == 0 {
                return Ok(0);
            }
        }
        
        let to_copy = std::cmp::min(buf.len(), self.cap - self.pos);
        buf[..to_copy].copy_from_slice(&self.buffer.as_slice()[self.pos..self.pos + to_copy]);
        self.pos += to_copy;
        
        Ok(to_copy)
    }
}

impl<R: Read> BufRead for OptimizedBufReader<R> {
    fn fill_buf(&mut self) -> io::Result<&[u8]> {
        if self.pos >= self.cap {
            self.fill_buffer()?;
        }
        Ok(&self.buffer.as_slice()[self.pos..self.cap])
    }
    
    fn consume(&mut self, amt: usize) {
        self.pos = std::cmp::min(self.pos + amt, self.cap);
    }
}

/// Asynchronous I/O processor for overlapping reads and processing
pub struct AsyncIOProcessor<R, W> {
    reader: R,
    writer: W,
    buffer_size: usize,
    num_buffers: usize,
}

impl<R: Read + Send + 'static, W: Write + Send + 'static> AsyncIOProcessor<R, W> {
    pub fn new(reader: R, writer: W, buffer_size: usize, num_buffers: usize) -> Self {
        Self {
            reader,
            writer,
            buffer_size,
            num_buffers,
        }
    }
    
    /// Process data with overlapping I/O operations
    pub fn process<F>(self, mut processor: F) -> io::Result<u64>
    where
        F: FnMut(&[u8]) -> Vec<u8> + Send + 'static,
    {
        let (read_tx, read_rx) = mpsc::sync_channel::<Vec<u8>>(self.num_buffers);
        let (write_tx, write_rx) = mpsc::sync_channel::<Vec<u8>>(self.num_buffers);
        
        let bytes_read = Arc::new(AtomicUsize::new(0));
        let bytes_written = Arc::new(AtomicUsize::new(0));
        
        // Reader thread
        let buffer_size = self.buffer_size;
        let bytes_read_clone = bytes_read.clone();
        let reader_handle = thread::spawn(move || -> io::Result<()> {
            let mut reader = self.reader;
            loop {
                let mut buffer = vec![0u8; buffer_size];
                
                match reader.read(&mut buffer) {
                    Ok(0) => break, // EOF
                    Ok(n) => {
                        buffer.truncate(n);
                        bytes_read_clone.fetch_add(n, Ordering::Relaxed);
                        if read_tx.send(buffer).is_err() {
                            break; // Channel closed
                        }
                    }
                    Err(e) => return Err(e),
                }
            }
            Ok(())
        });
        
        // Processor thread
        let bytes_written_clone = bytes_written.clone();
        let processor_handle = thread::spawn(move || -> io::Result<()> {
            while let Ok(input_buffer) = read_rx.recv() {
                let processed_data = processor(&input_buffer);
                bytes_written_clone.fetch_add(processed_data.len(), Ordering::Relaxed);
                
                if write_tx.send(processed_data).is_err() {
                    break; // Channel closed
                }
            }
            Ok(())
        });
        
        // Writer thread
        let writer_handle = thread::spawn(move || -> io::Result<()> {
            let mut writer = self.writer;
            while let Ok(data) = write_rx.recv() {
                writer.write_all(&data)?;
            }
            writer.flush()?;
            Ok(())
        });
        
        // Wait for all threads to complete
        reader_handle.join().unwrap()?;
        
        processor_handle.join().unwrap()?;
        
        writer_handle.join().unwrap()?;
        
        Ok(bytes_read.load(Ordering::Relaxed) as u64)
    }
}

/// Ring buffer for zero-copy circular buffer operations
pub struct RingBuffer {
    buffer: AlignedBuffer,
    read_pos: usize,
    write_pos: usize,
    size: usize,
}

impl RingBuffer {
    pub fn new(capacity: usize) -> Self {
        // Ensure capacity is power of 2 for efficient modulo operations
        let size = capacity.next_power_of_two();
        Self {
            buffer: AlignedBuffer::new(size),
            read_pos: 0,
            write_pos: 0,
            size,
        }
    }
    
    pub fn write(&mut self, data: &[u8]) -> usize {
        let available = self.available_write();
        let to_write = std::cmp::min(data.len(), available);
        
        if to_write == 0 {
            return 0;
        }
        
        let end_pos = (self.write_pos + to_write) & (self.size - 1);
        
        if end_pos > self.write_pos {
            // Simple case: no wrap around
            unsafe {
                let buffer_slice = std::slice::from_raw_parts_mut(
                    self.buffer.as_mut_slice().as_mut_ptr().add(self.write_pos),
                    to_write,
                );
                buffer_slice.copy_from_slice(&data[..to_write]);
            }
        } else {
            // Wrap around case
            let first_part = self.size - self.write_pos;
            let second_part = to_write - first_part;
            
            unsafe {
                let buffer_ptr = self.buffer.as_mut_slice().as_mut_ptr();
                
                // First part
                let first_slice = std::slice::from_raw_parts_mut(
                    buffer_ptr.add(self.write_pos),
                    first_part,
                );
                first_slice.copy_from_slice(&data[..first_part]);
                
                // Second part
                let second_slice = std::slice::from_raw_parts_mut(
                    buffer_ptr,
                    second_part,
                );
                second_slice.copy_from_slice(&data[first_part..to_write]);
            }
        }
        
        self.write_pos = end_pos;
        to_write
    }
    
    pub fn read(&mut self, buf: &mut [u8]) -> usize {
        let available = self.available_read();
        let to_read = std::cmp::min(buf.len(), available);
        
        if to_read == 0 {
            return 0;
        }
        
        let end_pos = (self.read_pos + to_read) & (self.size - 1);
        
        if end_pos > self.read_pos {
            // Simple case: no wrap around
            buf[..to_read].copy_from_slice(
                &self.buffer.as_slice()[self.read_pos..self.read_pos + to_read]
            );
        } else {
            // Wrap around case
            let first_part = self.size - self.read_pos;
            let second_part = to_read - first_part;
            
            buf[..first_part].copy_from_slice(
                &self.buffer.as_slice()[self.read_pos..self.size]
            );
            buf[first_part..to_read].copy_from_slice(
                &self.buffer.as_slice()[..second_part]
            );
        }
        
        self.read_pos = end_pos;
        to_read
    }
    
    pub fn available_read(&self) -> usize {
        (self.write_pos - self.read_pos) & (self.size - 1)
    }
    
    pub fn available_write(&self) -> usize {
        self.size - 1 - self.available_read()
    }
    
    pub fn is_empty(&self) -> bool {
        self.read_pos == self.write_pos
    }
    
    pub fn is_full(&self) -> bool {
        self.available_write() == 0
    }
}

/// Memory-mapped I/O for extremely large files
pub struct MmapIO {
    #[cfg(unix)]
    mmap: memmap2::Mmap,
    offset: AtomicUsize,
}

#[cfg(unix)]
impl MmapIO {
    pub fn new(file: std::fs::File) -> io::Result<Self> {
        let mmap = unsafe { memmap2::Mmap::map(&file)? };
        Ok(Self {
            mmap,
            offset: AtomicUsize::new(0),
        })
    }
    
    pub fn read_chunk(&self, size: usize) -> Option<&[u8]> {
        let current_offset = self.offset.load(Ordering::Acquire);
        if current_offset >= self.mmap.len() {
            return None;
        }
        
        let end = std::cmp::min(current_offset + size, self.mmap.len());
        let chunk = &self.mmap[current_offset..end];
        
        // Try to update offset atomically
        if self.offset.compare_exchange_weak(
            current_offset,
            end,
            Ordering::Release,
            Ordering::Relaxed
        ).is_ok() {
            Some(chunk)
        } else {
            // Another thread updated the offset, retry
            self.read_chunk(size)
        }
    }
    
    pub fn remaining(&self) -> usize {
        let current_offset = self.offset.load(Ordering::Acquire);
        self.mmap.len().saturating_sub(current_offset)
    }
    
    pub fn total_size(&self) -> usize {
        self.mmap.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_optimized_buf_reader() {
        let data = b"Hello, World! This is a test.";
        let cursor = Cursor::new(data);
        let mut reader = OptimizedBufReader::with_capacity(8, cursor);
        
        let mut buffer = [0u8; 5];
        let n = reader.read(&mut buffer).unwrap();
        assert_eq!(n, 5);
        assert_eq!(&buffer, b"Hello");
    }
    
    #[test]
    fn test_ring_buffer() {
        let mut ring = RingBuffer::new(8);
        
        // Write some data
        let written = ring.write(b"Hello");
        assert_eq!(written, 5);
        assert_eq!(ring.available_read(), 5);
        
        // Read some data
        let mut buffer = [0u8; 3];
        let read = ring.read(&mut buffer);
        assert_eq!(read, 3);
        assert_eq!(&buffer, b"Hel");
        assert_eq!(ring.available_read(), 2);
        
        // Write more data (should wrap around)
        let written = ring.write(b"World!");
        assert_eq!(written, 6);
        
        // Read all remaining data
        let mut buffer = [0u8; 10];
        let read = ring.read(&mut buffer);
        assert_eq!(read, 8);
        assert_eq!(&buffer[..8], b"loWorld!");
    }
    
    #[test]
    fn test_async_io_processor() {
        let input = b"abcdefghijklmnopqrstuvwxyz".repeat(1000);
        let reader = Cursor::new(input.clone());
        let mut writer = Vec::new();
        
        let processor = AsyncIOProcessor::new(reader, &mut writer, 1024, 4);
        
        let bytes_processed = processor.process(|data| {
            // Simple transformation: convert to uppercase
            data.iter().map(|&b| b.to_ascii_uppercase()).collect()
        }).unwrap();
        
        assert_eq!(bytes_processed, input.len() as u64);
        
        let expected: Vec<u8> = input.iter().map(|&b| b.to_ascii_uppercase()).collect();
        assert_eq!(writer, expected);
    }
}