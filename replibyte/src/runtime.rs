use lazy_static::lazy_static;
use std::future::Future;
use std::sync::Mutex;
use tokio::runtime::{Builder, Runtime};

lazy_static! {
    static ref TOKIO_RUNTIME: Mutex<Runtime> = Mutex::new({
        Builder::new_current_thread()
            .thread_name("tokio-blocking")
            .enable_all()
            .build()
            .unwrap()
    });
}

pub fn block_on<F: Future>(future: F) -> F::Output {
    TOKIO_RUNTIME.lock().unwrap().block_on(future)
}
