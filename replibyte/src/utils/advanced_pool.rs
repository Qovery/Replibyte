use std::sync::atomic::{AtomicUsize, Ordering};

/// Lock-free object pool for high-performance memory reuse
pub struct LockFreePool<T> {
    storage: Vec<std::sync::atomic::AtomicPtr<T>>,
    head: AtomicUsize,
    capacity: usize,
    constructor: Box<dyn Fn() -> T + Send + Sync>,
}

unsafe impl<T> Send for LockFreePool<T> {}
unsafe impl<T> Sync for LockFreePool<T> {}

impl<T> LockFreePool<T> {
    pub fn new<F>(capacity: usize, constructor: F) -> Self
    where
        F: Fn() -> T + Send + Sync + 'static,
    {
        let mut storage = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            storage.push(std::sync::atomic::AtomicPtr::new(std::ptr::null_mut()));
        }
        
        Self {
            storage,
            head: AtomicUsize::new(0),
            capacity,
            constructor: Box::new(constructor),
        }
    }
    
    pub fn get(&self) -> PooledObject<T> {
        // Try to get from pool first
        for _ in 0..self.capacity {
            let current = self.head.load(Ordering::Acquire);
            let next = (current + 1) % self.capacity;
            
            if self.head.compare_exchange_weak(
                current,
                next,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                let ptr = self.storage[current].swap(std::ptr::null_mut(), Ordering::Acquire);
                if !ptr.is_null() {
                    let obj = unsafe { Box::from_raw(ptr) };
                    return PooledObject::new(*obj, Some(self));
                }
            }
        }
        
        // Pool is empty, create new object
        PooledObject::new((self.constructor)(), Some(self))
    }
    
    fn try_return(&self, obj: T) -> bool {
        let boxed = Box::into_raw(Box::new(obj));
        
        for _ in 0..self.capacity {
            let current = self.head.load(Ordering::Acquire);
            let expected = std::ptr::null_mut();
            
            if self.storage[current].compare_exchange_weak(
                expected,
                boxed,
                Ordering::Release,
                Ordering::Relaxed
            ).is_ok() {
                return true;
            }
        }
        
        // Pool is full, drop the object
        unsafe { Box::from_raw(boxed) };
        false
    }
}

impl<T> Drop for LockFreePool<T> {
    fn drop(&mut self) {
        for atomic_ptr in &self.storage {
            let ptr = atomic_ptr.load(Ordering::Acquire);
            if !ptr.is_null() {
                unsafe { Box::from_raw(ptr) };
            }
        }
    }
}

/// RAII wrapper that returns objects to pool on drop
pub struct PooledObject<T> {
    object: Option<T>,
    pool: Option<*const LockFreePool<T>>,
}

impl<T> PooledObject<T> {
    fn new(object: T, pool: Option<&LockFreePool<T>>) -> Self {
        Self {
            object: Some(object),
            pool: pool.map(|p| p as *const _),
        }
    }
    
    pub fn get(&self) -> &T {
        self.object.as_ref().unwrap()
    }
    
    pub fn get_mut(&mut self) -> &mut T {
        self.object.as_mut().unwrap()
    }
}

impl<T> Drop for PooledObject<T> {
    fn drop(&mut self) {
        if let (Some(object), Some(pool_ptr)) = (self.object.take(), self.pool) {
            unsafe {
                let pool = &*pool_ptr;
                pool.try_return(object);
            }
        }
    }
}

/// High-performance memory pool for Vec<u8> with size classes
pub struct VecPool {
    small_pool: LockFreePool<Vec<u8>>,  // < 1KB
    medium_pool: LockFreePool<Vec<u8>>, // 1KB - 64KB
    large_pool: LockFreePool<Vec<u8>>,  // > 64KB
}

impl VecPool {
    pub fn new() -> Self {
        Self {
            small_pool: LockFreePool::new(100, || Vec::with_capacity(1024)),
            medium_pool: LockFreePool::new(50, || Vec::with_capacity(32 * 1024)),
            large_pool: LockFreePool::new(10, || Vec::with_capacity(256 * 1024)),
        }
    }
    
    pub fn get_vec(&self, min_capacity: usize) -> PooledObject<Vec<u8>> {
        if min_capacity <= 1024 {
            let mut vec = self.small_pool.get();
            vec.get_mut().clear();
            let current_capacity = vec.get().capacity();
            if current_capacity < min_capacity {
                vec.get_mut().reserve(min_capacity - current_capacity);
            }
            vec
        } else if min_capacity <= 64 * 1024 {
            let mut vec = self.medium_pool.get();
            vec.get_mut().clear();
            let current_capacity = vec.get().capacity();
            if current_capacity < min_capacity {
                vec.get_mut().reserve(min_capacity - current_capacity);
            }
            vec
        } else {
            let mut vec = self.large_pool.get();
            vec.get_mut().clear();
            let current_capacity = vec.get().capacity();
            if current_capacity < min_capacity {
                vec.get_mut().reserve(min_capacity - current_capacity);
            }
            vec
        }
    }
}

/// Thread-local storage for even faster access patterns
thread_local! {
    static VEC_POOL: VecPool = VecPool::new();
}

pub fn get_thread_local_vec(min_capacity: usize) -> PooledObject<Vec<u8>> {
    VEC_POOL.with(|pool| pool.get_vec(min_capacity))
}

/// Memory-aligned buffer for SIMD operations
#[repr(align(32))] // AVX2 alignment
pub struct AlignedBuffer {
    data: Vec<u8>,
}

impl AlignedBuffer {
    pub fn new(capacity: usize) -> Self {
        let mut data = Vec::with_capacity(capacity + 31);
        // Ensure alignment
        let ptr = data.as_ptr() as usize;
        let aligned_ptr = (ptr + 31) & !31;
        let offset = aligned_ptr - ptr;
        
        unsafe {
            data.set_len(offset);
            data.reserve_exact(capacity);
        }
        
        Self { data }
    }
    
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }
    
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }
    
    pub fn capacity(&self) -> usize {
        self.data.capacity()
    }
    
    pub fn len(&self) -> usize {
        self.data.len()
    }
    
    pub fn clear(&mut self) {
        self.data.clear();
    }
    
    pub fn extend_from_slice(&mut self, data: &[u8]) {
        self.data.extend_from_slice(data);
    }
}

/// Pool specifically for SQL query objects
pub struct QueryObjectPool {
    pool: LockFreePool<QueryObject>,
}

#[derive(Debug)]
pub struct QueryObject {
    pub data: Vec<u8>,
    pub table_name: String,
    pub columns: Vec<String>,
    pub values: Vec<String>,
}

impl QueryObject {
    fn new() -> Self {
        Self {
            data: Vec::with_capacity(1024),
            table_name: String::with_capacity(64),
            columns: Vec::with_capacity(16),
            values: Vec::with_capacity(16),
        }
    }
    
    pub fn reset(&mut self) {
        self.data.clear();
        self.table_name.clear();
        self.columns.clear();
        self.values.clear();
    }
}

impl QueryObjectPool {
    pub fn new(capacity: usize) -> Self {
        Self {
            pool: LockFreePool::new(capacity, QueryObject::new),
        }
    }
    
    pub fn get(&self) -> PooledObject<QueryObject> {
        let mut obj = self.pool.get();
        obj.get_mut().reset();
        obj
    }
}

// Global pools for common use cases - using std::sync::Once instead of lazy_static
use std::sync::Once;

static INIT_POOLS: Once = Once::new();
static mut GLOBAL_VEC_POOL: Option<VecPool> = None;
static mut GLOBAL_QUERY_POOL: Option<QueryObjectPool> = None;

fn init_global_pools() {
    unsafe {
        INIT_POOLS.call_once(|| {
            GLOBAL_VEC_POOL = Some(VecPool::new());
            GLOBAL_QUERY_POOL = Some(QueryObjectPool::new(1000));
        });
    }
}

pub fn get_global_vec_pool() -> &'static VecPool {
    init_global_pools();
    unsafe { GLOBAL_VEC_POOL.as_ref().unwrap() }
}

pub fn get_global_query_pool() -> &'static QueryObjectPool {
    init_global_pools();
    unsafe { GLOBAL_QUERY_POOL.as_ref().unwrap() }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_free_pool() {
        let pool = LockFreePool::new(5, || Vec::<u8>::with_capacity(100));
        
        let mut objects = Vec::new();
        for _ in 0..3 {
            objects.push(pool.get());
        }
        
        // Objects should be independent
        objects[0].get_mut().push(1);
        objects[1].get_mut().push(2);
        objects[2].get_mut().push(3);
        
        assert_eq!(objects[0].get()[0], 1);
        assert_eq!(objects[1].get()[0], 2);
        assert_eq!(objects[2].get()[0], 3);
    }
    
    #[test]
    fn test_vec_pool_size_classes() {
        let pool = VecPool::new();
        
        let small = pool.get_vec(512);
        let medium = pool.get_vec(16 * 1024);
        let large = pool.get_vec(128 * 1024);
        
        assert!(small.get().capacity() >= 512);
        assert!(medium.get().capacity() >= 16 * 1024);
        assert!(large.get().capacity() >= 128 * 1024);
    }
    
    #[test]
    fn test_aligned_buffer() {
        let mut buffer = AlignedBuffer::new(1024);
        let ptr = buffer.as_slice().as_ptr() as usize;
        assert_eq!(ptr % 32, 0); // Should be 32-byte aligned
        
        buffer.extend_from_slice(b"test data");
        assert_eq!(buffer.len(), 9);
    }
    
    #[test]
    fn test_query_object_pool() {
        let pool = QueryObjectPool::new(10);
        
        let mut obj = pool.get();
        obj.get_mut().table_name = "users".to_string();
        obj.get_mut().columns.push("id".to_string());
        obj.get_mut().values.push("1".to_string());
        
        assert_eq!(obj.get().table_name, "users");
        assert_eq!(obj.get().columns.len(), 1);
    }
}