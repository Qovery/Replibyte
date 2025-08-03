use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// High-performance profiler with minimal overhead
pub struct Profiler {
    metrics: Arc<Mutex<HashMap<String, ProfileMetric>>>,
    enabled: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileMetric {
    pub total_time: Duration,
    pub call_count: u64,
    pub min_time: Duration,
    pub max_time: Duration,
    pub memory_allocated: u64,
    pub memory_peak: u64,
}

impl ProfileMetric {
    fn new() -> Self {
        Self {
            total_time: Duration::ZERO,
            call_count: 0,
            min_time: Duration::MAX,
            max_time: Duration::ZERO,
            memory_allocated: 0,
            memory_peak: 0,
        }
    }

    fn update(&mut self, duration: Duration, memory_used: u64) {
        self.total_time += duration;
        self.call_count += 1;
        self.min_time = self.min_time.min(duration);
        self.max_time = self.max_time.max(duration);
        self.memory_allocated += memory_used;
        self.memory_peak = self.memory_peak.max(memory_used);
    }

    pub fn average_time(&self) -> Duration {
        if self.call_count > 0 {
            self.total_time / self.call_count as u32
        } else {
            Duration::ZERO
        }
    }

    pub fn average_memory(&self) -> u64 {
        if self.call_count > 0 {
            self.memory_allocated / self.call_count
        } else {
            0
        }
    }
}

impl Profiler {
    pub fn new(enabled: bool) -> Self {
        Self {
            metrics: Arc::new(Mutex::new(HashMap::new())),
            enabled,
        }
    }

    pub fn profile<F, R>(&self, name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        if !self.enabled {
            return f();
        }

        let start_time = Instant::now();
        let start_memory = get_current_memory_usage();

        let result = f();

        let duration = start_time.elapsed();
        let end_memory = get_current_memory_usage();
        let memory_used = end_memory.saturating_sub(start_memory);

        if let Ok(mut metrics) = self.metrics.lock() {
            let metric = metrics
                .entry(name.to_string())
                .or_insert_with(ProfileMetric::new);
            metric.update(duration, memory_used);
        }

        result
    }

    pub fn get_metrics(&self) -> HashMap<String, ProfileMetric> {
        self.metrics.lock().unwrap().clone()
    }

    pub fn print_report(&self) {
        if !self.enabled {
            println!("Profiling is disabled");
            return;
        }

        let metrics = self.get_metrics();

        println!("\n=== Performance Profile Report ===");
        println!(
            "{:<30} {:>10} {:>12} {:>12} {:>12} {:>12} {:>12}",
            "Function", "Calls", "Total (ms)", "Avg (ms)", "Min (ms)", "Max (ms)", "Avg Mem (KB)"
        );
        println!("{:-<110}", "");

        let mut sorted_metrics: Vec<_> = metrics.iter().collect();
        sorted_metrics.sort_by(|a, b| b.1.total_time.cmp(&a.1.total_time));

        for (name, metric) in sorted_metrics {
            println!(
                "{:<30} {:>10} {:>12.2} {:>12.2} {:>12.2} {:>12.2} {:>12.2}",
                name,
                metric.call_count,
                metric.total_time.as_millis() as f64,
                metric.average_time().as_micros() as f64 / 1000.0,
                metric.min_time.as_micros() as f64 / 1000.0,
                metric.max_time.as_micros() as f64 / 1000.0,
                metric.average_memory() as f64 / 1024.0
            );
        }
        println!();
    }

    pub fn reset(&self) {
        if let Ok(mut metrics) = self.metrics.lock() {
            metrics.clear();
        }
    }
}

/// Global profiler instance
static mut GLOBAL_PROFILER: Option<Profiler> = None;
static PROFILER_INIT: std::sync::Once = std::sync::Once::new();

pub fn init_profiler(enabled: bool) {
    unsafe {
        PROFILER_INIT.call_once(|| {
            GLOBAL_PROFILER = Some(Profiler::new(enabled));
        });
    }
}

pub fn get_profiler() -> &'static Profiler {
    unsafe {
        PROFILER_INIT.call_once(|| {
            GLOBAL_PROFILER = Some(Profiler::new(false));
        });
        GLOBAL_PROFILER.as_ref().unwrap()
    }
}

/// Macro for easy profiling
#[macro_export]
macro_rules! profile {
    ($name:expr, $code:block) => {
        crate::profiling::get_profiler().profile($name, || $code)
    };
}

/// Memory tracking utilities
static MEMORY_USAGE: AtomicU64 = AtomicU64::new(0);
static PEAK_MEMORY: AtomicU64 = AtomicU64::new(0);
static ALLOCATION_COUNT: AtomicU64 = AtomicU64::new(0);

pub fn track_allocation(size: u64) {
    let current = MEMORY_USAGE.fetch_add(size, Ordering::Relaxed) + size;

    // Update peak memory usage
    let mut peak = PEAK_MEMORY.load(Ordering::Relaxed);
    while current > peak {
        match PEAK_MEMORY.compare_exchange_weak(peak, current, Ordering::Relaxed, Ordering::Relaxed)
        {
            Ok(_) => break,
            Err(new_peak) => peak = new_peak,
        }
    }

    ALLOCATION_COUNT.fetch_add(1, Ordering::Relaxed);
}

pub fn track_deallocation(size: u64) {
    MEMORY_USAGE.fetch_sub(size, Ordering::Relaxed);
}

pub fn get_current_memory_usage() -> u64 {
    MEMORY_USAGE.load(Ordering::Relaxed)
}

pub fn get_peak_memory_usage() -> u64 {
    PEAK_MEMORY.load(Ordering::Relaxed)
}

pub fn get_allocation_count() -> u64 {
    ALLOCATION_COUNT.load(Ordering::Relaxed)
}

pub fn reset_memory_tracking() {
    MEMORY_USAGE.store(0, Ordering::Relaxed);
    PEAK_MEMORY.store(0, Ordering::Relaxed);
    ALLOCATION_COUNT.store(0, Ordering::Relaxed);
}

/// Hot path detector that identifies frequently called functions
pub struct HotPathDetector {
    call_counts: Arc<Mutex<HashMap<String, AtomicUsize>>>,
    threshold: usize,
}

impl HotPathDetector {
    pub fn new(threshold: usize) -> Self {
        Self {
            call_counts: Arc::new(Mutex::new(HashMap::new())),
            threshold,
        }
    }

    pub fn record_call(&self, function_name: &str) {
        if let Ok(mut counts) = self.call_counts.lock() {
            let counter = counts
                .entry(function_name.to_string())
                .or_insert_with(|| AtomicUsize::new(0));
            let new_count = counter.fetch_add(1, Ordering::Relaxed) + 1;

            if new_count == self.threshold {
                println!(
                    "HOT PATH DETECTED: {} called {} times",
                    function_name, new_count
                );
            }
        }
    }

    pub fn get_hot_paths(&self) -> Vec<(String, usize)> {
        if let Ok(counts) = self.call_counts.lock() {
            let mut hot_paths: Vec<_> = counts
                .iter()
                .map(|(name, counter)| (name.clone(), counter.load(Ordering::Relaxed)))
                .filter(|(_, count)| *count >= self.threshold)
                .collect();
            hot_paths.sort_by(|a, b| b.1.cmp(&a.1));
            hot_paths
        } else {
            Vec::new()
        }
    }
}

/// Performance monitor that tracks system resources
pub struct PerformanceMonitor {
    start_time: Instant,
    samples: Arc<Mutex<Vec<PerformanceSample>>>,
    sampling_interval: Duration,
}

#[derive(Debug, Clone)]
pub struct PerformanceSample {
    pub timestamp: Duration,
    pub memory_usage: u64,
    pub cpu_usage: f64,
    pub io_operations: u64,
}

impl PerformanceMonitor {
    pub fn new(sampling_interval: Duration) -> Self {
        Self {
            start_time: Instant::now(),
            samples: Arc::new(Mutex::new(Vec::new())),
            sampling_interval,
        }
    }

    pub fn start_monitoring(&self) -> std::thread::JoinHandle<()> {
        let samples = self.samples.clone();
        let start_time = self.start_time;
        let interval = self.sampling_interval;

        std::thread::spawn(move || {
            loop {
                let sample = PerformanceSample {
                    timestamp: start_time.elapsed(),
                    memory_usage: get_current_memory_usage(),
                    cpu_usage: get_cpu_usage(),
                    io_operations: get_io_operations(),
                };

                if let Ok(mut samples) = samples.lock() {
                    samples.push(sample);

                    // Keep only last 1000 samples to prevent unbounded growth
                    if samples.len() > 1000 {
                        samples.remove(0);
                    }
                }

                std::thread::sleep(interval);
            }
        })
    }

    pub fn get_samples(&self) -> Vec<PerformanceSample> {
        self.samples.lock().unwrap().clone()
    }

    pub fn get_memory_trend(&self) -> (u64, u64, f64) {
        let samples = self.get_samples();
        if samples.is_empty() {
            return (0, 0, 0.0);
        }

        let min_memory = samples.iter().map(|s| s.memory_usage).min().unwrap_or(0);
        let max_memory = samples.iter().map(|s| s.memory_usage).max().unwrap_or(0);
        let avg_memory =
            samples.iter().map(|s| s.memory_usage).sum::<u64>() as f64 / samples.len() as f64;

        (min_memory, max_memory, avg_memory)
    }
}

// Platform-specific implementations
#[cfg(target_os = "linux")]
fn get_cpu_usage() -> f64 {
    // Implementation would read from /proc/stat
    0.0 // Placeholder
}

#[cfg(not(target_os = "linux"))]
fn get_cpu_usage() -> f64 {
    0.0 // Placeholder for other platforms
}

#[cfg(target_os = "linux")]
fn get_io_operations() -> u64 {
    // Implementation would read from /proc/self/io
    0 // Placeholder
}

#[cfg(not(target_os = "linux"))]
fn get_io_operations() -> u64 {
    0 // Placeholder for other platforms
}

/// Benchmark utilities for performance testing
pub struct BenchmarkRunner {
    iterations: usize,
    warmup_iterations: usize,
}

impl BenchmarkRunner {
    pub fn new(iterations: usize, warmup_iterations: usize) -> Self {
        Self {
            iterations,
            warmup_iterations,
        }
    }

    pub fn benchmark<F>(&self, name: &str, mut f: F) -> BenchmarkResult
    where
        F: FnMut(),
    {
        // Warmup
        for _ in 0..self.warmup_iterations {
            f();
        }

        // Actual benchmark
        let mut times = Vec::with_capacity(self.iterations);
        let start_memory = get_current_memory_usage();

        for _ in 0..self.iterations {
            let start = Instant::now();
            f();
            times.push(start.elapsed());
        }

        let end_memory = get_current_memory_usage();
        let memory_used = end_memory.saturating_sub(start_memory);

        BenchmarkResult::new(name.to_string(), times, memory_used)
    }
}

#[derive(Debug)]
pub struct BenchmarkResult {
    pub name: String,
    pub iterations: usize,
    pub total_time: Duration,
    pub min_time: Duration,
    pub max_time: Duration,
    pub mean_time: Duration,
    pub memory_used: u64,
}

impl BenchmarkResult {
    fn new(name: String, times: Vec<Duration>, memory_used: u64) -> Self {
        let total_time = times.iter().sum();
        let min_time = *times.iter().min().unwrap();
        let max_time = *times.iter().max().unwrap();
        let mean_time = total_time / times.len() as u32;

        Self {
            name,
            iterations: times.len(),
            total_time,
            min_time,
            max_time,
            mean_time,
            memory_used,
        }
    }

    pub fn print_results(&self) {
        println!("Benchmark: {}", self.name);
        println!("  Iterations: {}", self.iterations);
        println!(
            "  Total time: {:.2}ms",
            self.total_time.as_micros() as f64 / 1000.0
        );
        println!(
            "  Mean time:  {:.2}μs",
            self.mean_time.as_nanos() as f64 / 1000.0
        );
        println!(
            "  Min time:   {:.2}μs",
            self.min_time.as_nanos() as f64 / 1000.0
        );
        println!(
            "  Max time:   {:.2}μs",
            self.max_time.as_nanos() as f64 / 1000.0
        );
        println!("  Memory:     {:.2}KB", self.memory_used as f64 / 1024.0);
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_profiler() {
        let profiler = Profiler::new(true);

        let result = profiler.profile("test_function", || {
            thread::sleep(Duration::from_millis(10));
            42
        });

        assert_eq!(result, 42);

        let metrics = profiler.get_metrics();
        assert!(metrics.contains_key("test_function"));

        let metric = &metrics["test_function"];
        assert_eq!(metric.call_count, 1);
        assert!(metric.total_time >= Duration::from_millis(10));
    }

    #[test]
    fn test_memory_tracking() {
        reset_memory_tracking();

        track_allocation(1000);
        assert_eq!(get_current_memory_usage(), 1000);
        assert_eq!(get_peak_memory_usage(), 1000);

        track_allocation(500);
        assert_eq!(get_current_memory_usage(), 1500);
        assert_eq!(get_peak_memory_usage(), 1500);

        track_deallocation(200);
        assert_eq!(get_current_memory_usage(), 1300);
        assert_eq!(get_peak_memory_usage(), 1500); // Peak should remain
    }

    #[test]
    fn test_hot_path_detector() {
        let detector = HotPathDetector::new(3);

        detector.record_call("function_a");
        detector.record_call("function_a");
        detector.record_call("function_b");
        detector.record_call("function_a");

        let hot_paths = detector.get_hot_paths();
        assert_eq!(hot_paths.len(), 1);
        assert_eq!(hot_paths[0].0, "function_a");
        assert_eq!(hot_paths[0].1, 3);
    }

    #[test]
    fn test_benchmark_runner() {
        let runner = BenchmarkRunner::new(10, 2);

        let result = runner.benchmark("sleep_test", || {
            thread::sleep(Duration::from_micros(100));
        });

        result.print_results();
        assert_eq!(result.iterations, 10);
        assert!(result.mean_time >= Duration::from_micros(100));
    }
}
