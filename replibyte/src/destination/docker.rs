use std::io::{Error, ErrorKind};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::Duration;

pub const DOCKER_BINARY_NAME: &str = "docker";

pub struct Image {
    pub name: String,
    pub tag: String,
}

pub struct ContainerOptions {
    pub host_port: u16,
    pub container_port: u16,
}

pub struct Container {
    pub id: String,
}

impl Container {
    pub fn new(
        image: &Image,
        options: &ContainerOptions,
        args: Vec<&str>,
        command: Option<Vec<&str>>,
    ) -> Result<Container, Error> {
        let port_mapping = format!("{}:{}", options.host_port, options.container_port);
        let image_version = format!("{}:{}", image.name, image.tag);
        let mut run_args = vec!["run", "-p", port_mapping.as_str()];

        for arg in args {
            run_args.push(arg);
        }

        run_args.push("-d");
        run_args.push(image_version.as_str());

        if let Some(command) = command {
            for arg in command {
                run_args.push(arg);
            }
        }

        let output = Command::new(DOCKER_BINARY_NAME).args(run_args).output()?;

        // FIX: this is a workaround to wait until the container is up
        thread::sleep(Duration::from_millis(20_000));

        match output.status.success() {
            true => match String::from_utf8(output.stdout) {
                Ok(container_id) => Ok(Container { id: container_id }),
                Err(err) => Err(Error::new(ErrorKind::Other, format!("{}", err))),
            },
            false => match String::from_utf8(output.stderr) {
                Ok(stderr) => Err(Error::new(ErrorKind::Other, stderr)),
                Err(err) => Err(Error::new(ErrorKind::Other, format!("{}", err))),
            },
        }
    }

    pub fn stop(&self) -> Result<(), Error> {
        let _process = Command::new(DOCKER_BINARY_NAME)
            .args(["stop", &self.id[..12]])
            .stdout(Stdio::null())
            .spawn()?;

        Ok(())
    }

    pub fn rm(&self) -> Result<(), Error> {
        let _process = Command::new(DOCKER_BINARY_NAME)
            .args(["rm", "-f", &self.id[..12]])
            .stdout(Stdio::null())
            .spawn()?;

        // TODO: should I drop the struct?
        drop(&self);

        Ok(())
    }

    pub fn exec(&self, cmd: &str) -> Result<Child, Error> {
        Command::new(DOCKER_BINARY_NAME)
            .args(["exec", "-i", &self.id[..12], "/bin/bash", "-c", cmd])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
    }
}

/// checks if the `dockerd` daemon runs
pub fn daemon_is_running() -> Result<(), Error> {
    let mut process = Command::new(DOCKER_BINARY_NAME)
        .args(["ps"])
        .stdout(Stdio::null())
        .spawn()?;

    match process.wait() {
        Ok(exit_status) => {
            if exit_status.success() {
                Ok(())
            } else {
                Err(Error::new(
                    ErrorKind::Other,
                    format!(
                        "cannot connect to the Docker daemon: exit_status {}",
                        exit_status
                    ),
                ))
            }
        }
        Err(err) => Err(Error::new(
            ErrorKind::Other,
            format!("cannot connect to the Docker daemon: {}", err),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::{Container, ContainerOptions, Image};

    #[test]
    fn handle_containers() {
        let image = Image {
            name: "postgres".to_string(),
            tag: "13".to_string(),
        };

        let options = ContainerOptions {
            host_port: 5433,
            container_port: 5432,
        };

        let args = vec![
            "-e",
            "POSTGRES_PASSWORD=password",
            "-e",
            "POSTGRES_USER=root",
        ];

        let container = Container::new(&image, &options, args, None).unwrap();

        assert!(container.id != *"");
        assert!(container.stop().is_ok());
        assert!(container.rm().is_ok());
    }
}
