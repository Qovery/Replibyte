use crate::connector::Connector;
use crate::destination::Destination;
use std::io::{Error, ErrorKind};
use std::process::{Command, Stdio};

pub struct Postgres<'a> {
    host: &'a str,
    port: u16,
    database: &'a str,
    username: &'a str,
    password: &'a str,
    wipe_database: bool,
}

impl<'a> Postgres<'a> {
    pub fn new(
        host: &'a str,
        port: u16,
        database: &'a str,
        username: &'a str,
        password: &'a str,
        wipe_database: bool,
    ) -> Self {
        Postgres {
            host,
            port,
            database,
            username,
            password,
            wipe_database,
        }
    }
}

impl<'a> Connector for Postgres<'a> {
    fn init(&mut self) -> Result<(), Error> {
        // TODO check psql binary available

        if self.wipe_database {
            let s_port = self.port.to_string();
            let wipe_db_query = wipe_database_query(self.username);

            let exit_status = Command::new("psql")
                .env("PGPASSWORD", self.password)
                .args([
                    "-h",
                    self.host,
                    "-p",
                    s_port.as_str(),
                    "-d",
                    self.database,
                    "-U",
                    self.username,
                    "-c",
                    wipe_db_query.as_str(),
                ])
                .spawn()?
                .wait()?;

            if !exit_status.success() {
                return Err(Error::new(
                    ErrorKind::Other,
                    format!("command error: {:?}", exit_status.to_string()),
                ));
            }
        }

        Ok(())
    }
}

impl<'a> Destination for Postgres<'a> {
    fn insert(&self, data: Vec<u8>) -> Result<(), Error> {
        let s_port = self.port.to_string();
        let queries = std::str::from_utf8(data.as_slice()).unwrap();

        let mut process = Command::new("psql")
            .env("PGPASSWORD", self.password)
            .args([
                "-h",
                self.host,
                "-p",
                s_port.as_str(),
                "-d",
                self.database,
                "-U",
                self.username,
                "-c",
                queries,
            ])
            .stdin(Stdio::piped())
            .spawn()?;

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

fn wipe_database_query(username: &str) -> String {
    format!(
        "\
    DROP SCHEMA public CASCADE; \
    CREATE SCHEMA public; \
    GRANT ALL ON SCHEMA public TO {}; \
    GRANT ALL ON SCHEMA public TO public;\
    ",
        username
    )
}

#[cfg(test)]
mod tests {
    use crate::connector::Connector;
    use crate::destination::postgres::Postgres;
    use crate::destination::Destination;

    fn get_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5453, "root", "root", "password", true)
    }

    fn get_invalid_postgres() -> Postgres<'static> {
        Postgres::new("localhost", 5453, "root", "root", "wrongpassword", true)
    }

    #[test]
    fn connect() {
        let mut p = get_postgres();
        let _ = p.init().expect("can't init postgres");
        assert!(p.insert(b"SELECT 1".to_vec()).is_ok());

        let mut p = get_invalid_postgres();
        assert!(p.init().is_err());
        assert!(p.insert(b"SELECT 1".to_vec()).is_err());
    }

    #[test]
    fn test_inserts() {}
}
