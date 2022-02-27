use std::io::Error;

pub trait Connector {
    fn init(&mut self) -> Result<(), Error>;
}
