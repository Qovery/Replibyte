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
    /// disable telemetry
    #[clap(short, long)]
    pub no_telemetry: bool,
}

/// sub commands
#[derive(Subcommand, Debug)]
pub enum SubCommand {
    /// all backup commands
    #[clap(subcommand)]
    Dump(DumpCommand),
    /// all transformers command
    #[clap(subcommand)]
    Transformer(TransformerCommand),
}

/// all dump commands
#[derive(Subcommand, Debug)]
pub enum DumpCommand {
    /// list available backups
    List,
    /// launch backup -- use `-h` to show all the options
    Create(DumpCreateArgs),
    /// all restore commands
    #[clap(subcommand)]
    Restore(RestoreCommand),
    /// delete a backup from the defined datastore
    Delete(DumpDeleteArgs),
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
pub struct DumpCreateArgs {
    #[clap(short, long, value_name = "[postgresql | mysql | mongodb]")]
    /// database source type to import
    pub source_type: Option<String>,
    /// import dump from stdin
    #[clap(short, long)]
    pub input: bool,
    #[clap(short, long, parse(from_os_str), value_name = "dump file")]
    /// dump file
    pub file: Option<PathBuf>,
    /// backup name
    #[clap(short, long)]
    pub name: Option<String>,
}

#[derive(Args, Debug)]
#[clap(group = clap::ArgGroup::new("delete-mode").multiple(false))]
pub struct DumpDeleteArgs {
    /// Name of the backup to delete
    #[clap(group = "delete-mode")]
    pub dump: Option<String>,
    /// Remove all backups older than the specified number of days. Example: `14d` for deleting backups older than 14 days
    #[clap(long, group = "delete-mode")]
    pub older_than: Option<String>,
    /// Keep only the last N backups
    #[clap(long, group = "delete-mode")]
    pub keep_last: Option<usize>,
}
