pub mod full_backup;
pub mod full_restore;

use std::io::Error;

pub trait Task {
    fn run(self) -> Result<(), Error>;
}

/// inter-thread message for Source/Destination and Bridge
#[derive(Debug, Clone)]
enum Message<T> {
    Data(T),
    EOF,
}
