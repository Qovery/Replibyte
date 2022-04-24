use std::io::{Error, ErrorKind, Write};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::binary_exists;

pub struct Mysql<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
}

impl<'a> Mysql<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
    ) -> Self {
        Mysql {
            host,
            port,
            database,
            username,
            password,
        }
    }
}

impl<'a> Connector for Mysql<'a> {
    fn init(&mut self) -> Result<(), Error> {
        let _ = binary_exists("mysql")?;

        // test MySQL connection
        let exit_status = Command::new("mysql")
            .args([
                "-h",
                self.host,
                "-P",
                self.port.to_string().as_str(),
                "-u",
                self.username,
                &format!("-p{}", self.password),
                "-e",
                "SELECT 1;",
            ])
            .stdout(Stdio::piped())
            .spawn()?
            .wait()?;

        if !exit_status.success() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("command error: {:?}", exit_status.to_string()),
            ));
        }

        Ok(())
    }
}

impl<'a> Destination for Mysql<'a> {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let mut process = Command::new("mysql")
            .args([
                "-h",
                self.host,
                "-P",
                self.port.to_string().as_str(),
                "-u",
                self.username,
                &format!("-p{}", self.password),
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;

        let _ = process.stdin.take().unwrap().write_all(data.as_slice());

        let exit_status = process.wait()?;
        if !exit_status.success() {
            return Err(Error::new(
                ErrorKind::Other,
                format!("command error: {:?}", exit_status.to_string()),
            ));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::connector::Connector;
    use crate::destination::mysql::Mysql;
    use crate::destination::Destination;

    fn get_mysql() -> Mysql<'static> {
        Mysql::new("127.0.0.1", 3306, "root", "root", "password")
    }

    fn get_invalid_mysql() -> Mysql<'static> {
        Mysql::new("127.0.0.1", 3306, "root", "root", "wrong_password")
    }

    #[test]
    fn connect() {
        let mut m = get_mysql();
        let _ = m.init().expect("can't init mysql");
        assert!(m.write(b"SELECT 1;".to_vec()).is_ok());

        let mut m = get_invalid_mysql();
        assert!(m.init().is_err());
        assert!(m.write(b"SELECT 1".to_vec()).is_err());
    }

    #[test]
    fn test_inserts() {}
}
