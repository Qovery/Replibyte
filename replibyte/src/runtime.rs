use lazy_static::lazy_static;
use std::future::Future;
use std::sync::{Mutex, MutexGuard};
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    pub static ref TOKIO_RUNTIME: Mutex<Runtime> = Mutex::new({
        Builder::new_current_thread()
            .thread_name("tokio-blocking")
            .enable_all()
            .build()
            .unwrap()
    });
}

pub fn block_on<F: Future>(future: F, runtime: MutexGuard<Runtime>) -> F::Output {
    runtime.block_on(future)
}

#[cfg(test)]
mod tests {
    use aws_types::SdkConfig;
    use crate::runtime::block_on;

    #[test]
    fn test_block_on() {
        let runtime = std::sync::Mutex::new(
            tokio::runtime::Builder::new_current_thread()
                .thread_name("tokio-test")
                .enable_all()
                .build()
                .unwrap()
        );
        let config_future = aws_config::load_from_env();
        let result = block_on(config_future, runtime.lock().unwrap());
        assert_eq!(result.app_name(), None);
    }
}
