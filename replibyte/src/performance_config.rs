use serde::{Deserialize, Serialize};

/// Performance configuration options for RepliByte
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceConfig {
    /// Buffer size for chunking data (in bytes). Default: 100MB
    pub buffer_size: usize,

    /// Number of queries to pre-allocate in vectors. Default: 1000
    pub query_vector_capacity: usize,

    /// Channel buffer size for inter-thread communication. Default: 10
    pub channel_buffer_size: usize,

    /// Progress bar update interval (in milliseconds). Default: 10ms
    pub progress_update_interval: u64,

    /// Buffer pool settings
    pub buffer_pool: BufferPoolConfig,

    /// Parser settings
    pub parser: ParserConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferPoolConfig {
    /// Maximum number of buffers to keep in the pool. Default: 10
    pub max_buffers: usize,

    /// Default capacity for pooled buffers (in bytes). Default: 8192
    pub buffer_capacity: usize,

    /// Whether to enable buffer pooling. Default: true
    pub enabled: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParserConfig {
    /// Initial capacity for SQL statement vectors. Default: based on query length / 100
    pub statement_vector_capacity: Option<usize>,

    /// Initial capacity for parser stack. Default: 16
    pub parser_stack_capacity: usize,

    /// Line buffer capacity (in bytes). Default: 1024
    pub line_buffer_capacity: usize,

    /// Main buffer capacity (in bytes). Default: 8192
    pub main_buffer_capacity: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            buffer_size: 100 * 1024 * 1024, // 100MB
            query_vector_capacity: 1000,
            channel_buffer_size: 10,
            progress_update_interval: 10,
            buffer_pool: BufferPoolConfig::default(),
            parser: ParserConfig::default(),
        }
    }
}

impl Default for BufferPoolConfig {
    fn default() -> Self {
        Self {
            max_buffers: 10,
            buffer_capacity: 8192,
            enabled: true,
        }
    }
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            statement_vector_capacity: None, // Will be calculated based on query length
            parser_stack_capacity: 16,
            line_buffer_capacity: 1024,
            main_buffer_capacity: 8192,
        }
    }
}

impl PerformanceConfig {
    /// Load performance configuration from environment variables
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(buffer_size) = std::env::var("REPLIBYTE_BUFFER_SIZE") {
            if let Ok(size) = buffer_size.parse::<usize>() {
                config.buffer_size = size;
            }
        }

        if let Ok(capacity) = std::env::var("REPLIBYTE_QUERY_CAPACITY") {
            if let Ok(cap) = capacity.parse::<usize>() {
                config.query_vector_capacity = cap;
            }
        }

        if let Ok(channel_size) = std::env::var("REPLIBYTE_CHANNEL_BUFFER") {
            if let Ok(size) = channel_size.parse::<usize>() {
                config.channel_buffer_size = size;
            }
        }

        config
    }

    /// Validate configuration values
    pub fn validate(&self) -> Result<(), String> {
        if self.buffer_size < 1024 * 1024 {
            return Err("Buffer size must be at least 1MB".to_string());
        }

        if self.query_vector_capacity == 0 {
            return Err("Query vector capacity must be greater than 0".to_string());
        }

        if self.channel_buffer_size == 0 {
            return Err("Channel buffer size must be greater than 0".to_string());
        }

        Ok(())
    }
}

/// Global performance configuration instance
static mut PERFORMANCE_CONFIG: Option<PerformanceConfig> = None;
static INIT: std::sync::Once = std::sync::Once::new();

/// Initialize global performance configuration
pub fn init_performance_config(config: PerformanceConfig) {
    unsafe {
        INIT.call_once(|| {
            PERFORMANCE_CONFIG = Some(config);
        });
    }
}

/// Get global performance configuration
pub fn get_performance_config() -> &'static PerformanceConfig {
    unsafe {
        INIT.call_once(|| {
            PERFORMANCE_CONFIG = Some(PerformanceConfig::from_env());
        });
        PERFORMANCE_CONFIG.as_ref().unwrap()
    }
}
