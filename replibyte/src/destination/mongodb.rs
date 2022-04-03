use std::io::{Error, ErrorKind, Write};
use std::process::{Command, Stdio};

use crate::connector::Connector;
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::binary_exists;

pub struct MongoDB<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
    authentication_database: &'a str,
}

impl<'a> MongoDB<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
        authentication_database: &'a str,
    ) -> Self {
        MongoDB {
            host,
            port,
            database,
            username,
            password,
            authentication_database,
        }
    }
}

impl<'a> Connector for MongoDB<'a> {
    fn init(&mut self) -> Result<(), Error> {
        let _ = binary_exists("mongo")?;
        let _ = binary_exists("mongorestore")?;
        let _ = check_connection_status(self)?;

        Ok(())
    }
}

impl<'a> Destination for MongoDB<'a> {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let s_port = self.port.to_string();

        let mut process = Command::new("mongorestore")
            .args([
                "-h",
                self.host,
                "--port",
                s_port.as_str(),
                "--authenticationDatabase",
                self.authentication_database,
                "-u",
                self.username,
                "-p",
                self.password,
                format!("--nsFrom='{}.*'", self.database).as_str(),
                format!("--nsTo='{}.*'", self.database).as_str(),
                "--archive",
            ])
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .spawn()?;

        let _ = process
            .stdin
            .take()
            .unwrap()
            .write_all(&data[..data.len() - 1]); // remove trailing null terminator, or else mongorestore will fail

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

fn check_connection_status(db: &MongoDB) -> Result<(), Error> {
    let s_port = db.port.to_string();

    let mut echo_process = Command::new("echo")
        .arg(r#"'db.runCommand("ping").ok'"#)
        .stdout(Stdio::piped())
        .spawn()?;

    let mut mongo_process = Command::new("mongo")
        .args([
            "--host",
            db.host,
            "--port",
            s_port.as_str(),
            "--authenticationDatabase",
            db.authentication_database,
            "-u",
            db.username,
            "-p",
            db.password,
            "--quiet",
        ])
        .stdin(echo_process.stdout.take().unwrap())
        .stdout(Stdio::inherit())
        .spawn()?;

    let exit_status = mongo_process.wait()?;
    if !exit_status.success() {
        return Err(Error::new(
            ErrorKind::Other,
            format!("command error: {:?}", exit_status.to_string()),
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use dump_parser::utils::decode_hex;

    use crate::connector::Connector;
    use crate::destination::mongodb::MongoDB;
    use crate::destination::Destination;

    fn get_mongodb() -> MongoDB<'static> {
        MongoDB::new("localhost", 27018, "test", "root", "password", "admin")
    }

    fn get_invalid_mongodb() -> MongoDB<'static> {
        MongoDB::new("localhost", 27018, "test", "root", "wrongpassword", "admin")
    }

    #[test]
    fn connect() {
        let mut p = get_mongodb();
        let _ = p.init().expect("can't init mongodb");
        let bytes = decode_hex("6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e320000020100000264620005000000746573740002636f6c6c656374696f6e0006000000757365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223464363734323637316333613463663938316439386164373831343735333234222c22636f6c6c656374696f6e4e616d65223a227573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3b0000000264620005000000746573740002636f6c6c656374696f6e000600000075736572730008454f4600001243524300000000000000000000ffffffff3b0000000264620005000000746573740002636f6c6c656374696f6e000600000075736572730008454f4600011243524300000000000000000000ffffffff00").unwrap();
        assert!(p.write(bytes.to_vec()).is_ok());

        let mut p = get_invalid_mongodb();
        assert!(p.init().is_err());
        assert!(p.write(bytes.to_vec()).is_err());
    }
    //TODO add more tests
}
