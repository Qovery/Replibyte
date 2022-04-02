use crate::connector::Connector;
use crate::destination::docker::{
    daemon_is_running, Container, ContainerOptions, Image, DOCKER_BINARY_NAME,
};
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::binary_exists;
use std::io::{Error, ErrorKind, Write};

const DEFAULT_POSTGRES_IMAGE: &str = "postgres";
pub const DEFAULT_POSTGRES_IMAGE_TAG: &str = "13";
pub const DEFAULT_POSTGRES_CONTAINER_PORT: u16 = 5432;
const DEFAULT_POSTGRES_USER: &str = "postgres";
const DEFAULT_POSTGRES_PASSWORD: &str = "password";
const DEFAULT_POSTGRES_DB: &str = "postgres";

pub struct PostgresDocker {
    pub image: Image,
    pub options: ContainerOptions,
    pub container: Option<Container>,
}

impl PostgresDocker {
    pub fn new(tag: String, port: u16) -> Self {
        Self {
            image: Image {
                name: DEFAULT_POSTGRES_IMAGE.to_string(),
                tag,
            },
            options: ContainerOptions {
                host_port: port,
                container_port: DEFAULT_POSTGRES_CONTAINER_PORT,
            },
            container: None,
        }
    }
}

impl Connector for PostgresDocker {
    fn init(&mut self) -> Result<(), Error> {
        let _ = binary_exists(DOCKER_BINARY_NAME)?;
        let _ = daemon_is_running()?;

        let password_env = format!("POSTGRES_PASSWORD={}", DEFAULT_POSTGRES_PASSWORD);
        let user_env = format!("POSTGRES_USER={}", DEFAULT_POSTGRES_USER);
        let container = Container::new(
            &self.image,
            &self.options,
            vec!["-e", password_env.as_str(), "-e", user_env.as_str()],
        )?;

        self.container = Some(container);
        Ok(())
    }
}

impl Destination for PostgresDocker {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let cmd = format!(
            "PGPASSWORD={} psql --username {} {}",
            DEFAULT_POSTGRES_PASSWORD, DEFAULT_POSTGRES_USER, DEFAULT_POSTGRES_DB
        );

        match &self.container {
            Some(container) => {
                let mut container_exec = container.exec(&cmd)?;
                let _ = container_exec
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(data.as_slice());

                let exit_status = container_exec.wait()?;
                if !exit_status.success() {
                    return Err(Error::new(
                        ErrorKind::Other,
                        format!("command error: {:?}", exit_status.to_string()),
                    ));
                }

                Ok(())
            }
            None => Err(Error::new(
                ErrorKind::Other,
                "command error: cannot retrieve container",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::PostgresDocker;
    use crate::connector::Connector;
    use crate::destination::Destination;

    fn get_postgres() -> PostgresDocker {
        PostgresDocker::new("13".to_string(), 5454)
    }

    fn get_invalid_postgres() -> PostgresDocker {
        PostgresDocker::new("bad_tag".to_string(), 5454)
    }

    #[test]
    fn connect() {
        let mut p = get_postgres();
        let _ = p.init().expect("can't init postgres");
        assert!(p.write(b"SELECT 1".to_vec()).is_ok());

        // cleanup container
        let _ = p.container.unwrap().rm();

        let mut p = get_invalid_postgres();
        assert!(p.init().is_err());
        assert!(p.write(b"SELECT 1".to_vec()).is_err());
    }
}
