use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};

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
    #[clap(subcommand)]
    Restore(RestoreCommand),
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
#[derive(Subcommand, Debug)]
pub enum RestoreCommand {
    /// restore latest backup -- use <list> command to list available restore
    Latest,
}
