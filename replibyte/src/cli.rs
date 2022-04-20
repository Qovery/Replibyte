use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// RepliByte is a tool to synchronize cloud databases and fake sensitive data, just pass `-h`
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct CLI {
    /// replibyte configuration file
    #[clap(short, long, parse(from_os_str), value_name = "configuration file")]
    pub config: PathBuf,
    #[clap(subcommand)]
    pub sub_commands: SubCommand,
}

/// sub commands
#[derive(Subcommand, Debug)]
pub enum SubCommand {
    /// all backup commands
    #[clap(subcommand)]
    Backup(BackupCommand),
    /// all transformers command
    #[clap(subcommand)]
    Transformer(TransformerCommand),
    /// all restore commands
    #[clap(subcommand)]
    Restore(RestoreCommand),
}

/// all backup commands
#[derive(Subcommand, Debug)]
pub enum BackupCommand {
    /// list available backups
    List,
    /// launch backup -- use `-h` to show all the options
    Run(BackupRunArgs),
}

/// all transformer commands
#[derive(Subcommand, Debug)]
pub enum TransformerCommand {
    /// list available transformers
    List,
}

/// all restore commands
#[derive(Subcommand, Debug)]
pub enum RestoreCommand {
    /// Restore backup inside a local Docker container
    Local(RestoreLocalArgs),
    /// Restore backup inside the configured destination
    Remote(RestoreArgs),
}

/// all restore commands
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// restore backup -- set `latest` or `<backup name>` - use `backup list` command to list all backups available
    #[clap(short, long, value_name = "[latest | backup name]")]
    pub value: String,
    /// stream output on stdout
    #[clap(short, long)]
    pub output: bool,
}

/// restore backup in a local Docker container
#[derive(Args, Debug)]
pub struct RestoreLocalArgs {
    /// restore backup -- set `latest` or `<backup name>` - use `backup list` command to list all backups available
    #[clap(short, long, value_name = "[latest | backup name]")]
    pub value: String,
    /// stream output on stdout
    #[clap(short, long)]
    pub output: bool,
    /// Docker image tag for the container to spawn
    #[clap(short, long)]
    pub tag: Option<String>,
    /// Docker container port to map on the host
    #[clap(short, long)]
    pub port: Option<u16>,
    /// Remove the Docker container on Ctrl-c
    #[clap(short, long)]
    pub remove: bool,
    /// Docker image type
    #[clap(short, long, value_name = "[postgresql | mysql | mongodb]")]
    pub image: String,
}

/// all backup run commands
#[derive(Args, Debug)]
pub struct BackupRunArgs {
    #[clap(short, long, value_name = "[postgresql | mysql | mongodb]")]
    /// database source type to import
    pub source_type: Option<String>,
    /// import dump from stdin
    #[clap(short, long)]
    pub input: bool,
    #[clap(short, long, parse(from_os_str), value_name = "dump file")]
    /// dump file
    pub file: Option<PathBuf>,
}
