use std::path::PathBuf;

use clap::{ArgEnum, Args, Parser, Subcommand};

/// RepliByte is a tool to synchronize cloud databases and fake sensitive data, just pass `-h`
#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
#[clap(propagate_version = true)]
pub struct CLI {
    #[clap(short, long, parse(from_os_str), value_name = "configuration file")]
    /// replibyte configuration file
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
    /// all restore commands
    Restore(RestoreArgs),
}

/// all backup commands
#[derive(Subcommand, Debug)]
pub enum BackupCommand {
    /// list available backups
    List,
    /// launch backup now
    Launch,
}

/// all restore commands
#[derive(Args, Debug)]
pub struct RestoreArgs {
    /// restore backup -- set `latest` or `<backup name>` - use `backup list` command to list all backups available
    #[clap(short, long, value_name = "latest | <backup name>")]
    pub value: String,
}
