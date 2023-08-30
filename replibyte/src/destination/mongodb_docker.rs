use crate::connector::Connector;
use crate::destination::docker::{
    daemon_is_running, Container, ContainerOptions, Image, DOCKER_BINARY_NAME,
};
use crate::destination::Destination;
use crate::types::Bytes;
use crate::utils::binary_exists;
use std::io::{Error, ErrorKind, Write};

const DEFAULT_MONGO_IMAGE: &str = "mongo";
pub const DEFAULT_MONGO_IMAGE_TAG: &str = "5";
pub const DEFAULT_MONGO_CONTAINER_PORT: u16 = 27017;
const DEFAULT_MONGO_USER: &str = "root";
const DEFAULT_MONGO_PASSWORD: &str = "password";

pub struct MongoDBDocker {
    pub image: Image,
    pub options: ContainerOptions,
    pub container: Option<Container>,
}

impl MongoDBDocker {
    pub fn new(tag: String, port: u16) -> Self {
        Self {
            image: Image {
                name: DEFAULT_MONGO_IMAGE.to_string(),
                tag,
            },
            options: ContainerOptions {
                host_port: port,
                container_port: DEFAULT_MONGO_CONTAINER_PORT,
            },
            container: None,
        }
    }
}

impl Connector for MongoDBDocker {
    fn init(&mut self) -> Result<(), Error> {
        binary_exists(DOCKER_BINARY_NAME)?;
        daemon_is_running()?;

        let password_env = format!("MONGO_INITDB_ROOT_USERNAME={}", DEFAULT_MONGO_USER);
        let user_env = format!("MONGO_INITDB_ROOT_PASSWORD={}", DEFAULT_MONGO_PASSWORD);
        let container = Container::new(
            &self.image,
            &self.options,
            vec!["-e", password_env.as_str(), "-e", user_env.as_str()],
            None,
        )?;

        self.container = Some(container);
        Ok(())
    }
}

impl Destination for MongoDBDocker {
    fn write(&self, data: Bytes) -> Result<(), Error> {
        let cmd = format!(
            "mongorestore --authenticationDatabase admin -u {} -p {} --archive",
            DEFAULT_MONGO_USER, DEFAULT_MONGO_PASSWORD,
        );

        match &self.container {
            Some(container) => {
                let mut container_exec = container.exec(&cmd)?;
                let _ = container_exec
                    .stdin
                    .take()
                    .unwrap()
                    .write_all(&data[..data.len() - 1]); // remove trailing null terminator, or else mongorestore will fail

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
    use dump_parser::utils::decode_hex;

    use crate::connector::Connector;
    use crate::destination::mongodb_docker::MongoDBDocker;
    use crate::destination::Destination;

    fn get_mongodb() -> MongoDBDocker {
        MongoDBDocker::new("5".to_string(), 27021)
    }

    fn get_invalid_mongodb() -> MongoDBDocker {
        MongoDBDocker::new("bad_tag".to_string(), 27021)
    }

    #[test]
    fn connect() {
        let mut p = get_mongodb();
        p.init().expect("can't init mongodb");
        let bytes = decode_hex("6de299816600000010636f6e63757272656e745f636f6c6c656374696f6e7300040000000276657273696f6e0004000000302e3100027365727665725f76657273696f6e0006000000352e302e360002746f6f6c5f76657273696f6e00080000003130302e352e320000020100000264620005000000746573740002636f6c6c656374696f6e0006000000757365727300026d6574616461746100ad0000007b22696e6465786573223a5b7b2276223a7b22246e756d626572496e74223a2232227d2c226b6579223a7b225f6964223a7b22246e756d626572496e74223a2231227d7d2c226e616d65223a225f69645f227d5d2c2275756964223a223464363734323637316333613463663938316439386164373831343735333234222c22636f6c6c656374696f6e4e616d65223a227573657273222c2274797065223a22636f6c6c656374696f6e227d001073697a6500000000000274797065000b000000636f6c6c656374696f6e0000ffffffff3b0000000264620005000000746573740002636f6c6c656374696f6e000600000075736572730008454f4600001243524300000000000000000000ffffffff3b0000000264620005000000746573740002636f6c6c656374696f6e000600000075736572730008454f4600011243524300000000000000000000ffffffff00").unwrap();
        assert!(p.write(bytes.to_vec()).is_ok());

        // cleanup container
        let _ = p.container.unwrap().rm();

        let mut p = get_invalid_mongodb();
        assert!(p.init().is_err());
        assert!(p.write(bytes.to_vec()).is_err());
    }
}
