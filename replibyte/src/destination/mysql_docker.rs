use crate::connector::Connector;
use crate::destination::docker::{
    daemon_is_running, Container, ContainerOptions, Image, DOCKER_BINARY_NAME,
};
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::binary_exists;
use std::io::{Error, ErrorKind, Write};

const DEFAULT_MYSQL_IMAGE: &str = "mysql";
pub const DEFAULT_MYSQL_IMAGE_TAG: &str = "8";
pub const DEFAULT_MYSQL_CONTAINER_PORT: u16 = 3306;
const DEFAULT_MYSQL_PASSWORD: &str = "password";

pub struct MysqlDocker {
    pub image: Image,
    pub options: ContainerOptions,
    pub container: Option<Container>,
}

impl MysqlDocker {
    pub fn new(tag: String, port: u16) -> Self {
        Self {
            image: Image {
                name: DEFAULT_MYSQL_IMAGE.to_string(),
                tag,
            },
            options: ContainerOptions {
                host_port: port,
                container_port: DEFAULT_MYSQL_CONTAINER_PORT,
            },
            container: None,
        }
    }
}

impl Connector for MysqlDocker {
    fn init(&mut self) -> Result<(), Error> {
        binary_exists(DOCKER_BINARY_NAME)?;
        daemon_is_running()?;

        let password_env = format!("MYSQL_ROOT_PASSWORD={}", DEFAULT_MYSQL_PASSWORD);
        let container = Container::new(
            &self.image,
            &self.options,
            vec!["-e", password_env.as_str()],
            Some(vec![
                "mysqld",
                "--default-authentication-plugin=mysql_native_password",
            ]),
        )?;

        self.container = Some(container);
        Ok(())
    }
}

impl Destination for MysqlDocker {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        match &self.container {
            Some(container) => {
                let mut container_exec =
                    container.exec("exec mysql -uroot -p\"$MYSQL_ROOT_PASSWORD\"")?;
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
    use super::MysqlDocker;
    use crate::connector::Connector;
    use crate::destination::Destination;

    fn get_mysql() -> MysqlDocker {
        MysqlDocker::new("8".to_string(), 3308)
    }

    fn get_invalid_mysql() -> MysqlDocker {
        MysqlDocker::new("bad_tag".to_string(), 3308)
    }

    #[test]
    fn connect() {
        let mut p = get_mysql();
        p.init().expect("can't init mysql");
        assert!(p.write(b"SELECT 1".to_vec()).is_ok());

        // cleanup container
        let _ = p.container.unwrap().rm();

        let mut p = get_invalid_mysql();
        assert!(p.init().is_err());
        assert!(p.write(b"SELECT 1".to_vec()).is_err());
    }
}
