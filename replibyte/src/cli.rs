use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

/// RepliByte is a tool to synchronize cloud databases and fake sensitive data, just pass `-h`
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct CLI {
    /// replibyte configuration file
    #[clap(
        short,
        long,
        default_value_t = String::from("replibyte.conf"),
        value_name = "configuration file"
    )]
    pub config: String,
    #[clap(subcommand)]
    pub sub_commands: SubCommand,
}

/// sub commands
#[derive(Subcommand, Debug)]
pub enum SubCommand {
    /// all backup commands
    #[clap(subcommand)]
    Backup(BackupCommand),
    /// all restore commands
    Restore(RestoreArgs),
}

/// all backup commands
#[derive(Subcommand, Debug)]
pub enum BackupCommand {
    /// list available backups
    List,
    /// launch backup -- use `-h` to show all the options
    Run(BackupRunArgs),
}

/// all restore commands
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// restore backup -- set `latest` or `<backup name>` - use `backup list` command to list all backups available
    #[clap(short, long, value_name = "[latest | backup name]")]
    pub value: String,
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
