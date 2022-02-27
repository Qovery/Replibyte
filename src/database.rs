use std::io::Error;

pub trait Database {
    fn connect(&mut self) -> Result<(), Error>;
}
