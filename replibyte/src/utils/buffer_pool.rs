use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

/// A simple buffer pool to reuse Vec<u8> allocations and reduce memory pressure
pub struct BufferPool {
    buffers: Arc<Mutex<VecDeque<Vec<u8>>>>,
    max_size: usize,
    buffer_capacity: usize,
}

impl BufferPool {
    pub fn new(max_size: usize, buffer_capacity: usize) -> Self {
        Self {
            buffers: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            buffer_capacity,
        }
    }

    pub fn get_buffer(&self) -> Vec<u8> {
        if let Ok(mut buffers) = self.buffers.lock() {
            if let Some(mut buffer) = buffers.pop_front() {
                buffer.clear();
                return buffer;
            }
        }

        // If no buffer available, create a new one
        Vec::with_capacity(self.buffer_capacity)
    }

    pub fn return_buffer(&self, buffer: Vec<u8>) {
        if let Ok(mut buffers) = self.buffers.lock() {
            if buffers.len() < self.max_size && buffer.capacity() >= self.buffer_capacity / 2 {
                buffers.push_back(buffer);
            }
            // If pool is full or buffer is too small, just drop it
        }
    }
}

/// A buffer that automatically returns itself to the pool when dropped
pub struct PooledBuffer {
    buffer: Option<Vec<u8>>,
    pool: Arc<BufferPool>,
}

impl PooledBuffer {
    pub fn new(pool: Arc<BufferPool>) -> Self {
        let buffer = pool.get_buffer();
        Self {
            buffer: Some(buffer),
            pool,
        }
    }

    pub fn as_mut(&mut self) -> &mut Vec<u8> {
        self.buffer.as_mut().unwrap()
    }

    pub fn as_ref(&self) -> &Vec<u8> {
        self.buffer.as_ref().unwrap()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(buffer) = self.buffer.take() {
            self.pool.return_buffer(buffer);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic_operations() {
        let pool = BufferPool::new(5, 1024);

        // Get a buffer
        let buffer1 = pool.get_buffer();
        assert_eq!(buffer1.capacity(), 1024);

        // Return the buffer
        pool.return_buffer(buffer1);

        // Get another buffer (should reuse the returned one)
        let buffer2 = pool.get_buffer();
        assert!(buffer2.capacity() >= 1024);
    }

    #[test]
    fn test_pooled_buffer() {
        let pool = Arc::new(BufferPool::new(5, 1024));

        {
            let mut pooled = PooledBuffer::new(pool.clone());
            pooled.as_mut().extend_from_slice(b"test data");
            assert_eq!(pooled.as_ref().len(), 9);
        } // Buffer should be returned to pool here

        // Get a new buffer - should reuse the one from above
        let buffer = pool.get_buffer();
        assert_eq!(buffer.len(), 0); // Should be cleared
        assert!(buffer.capacity() >= 1024);
    }
}
