#[macro_use]
extern crate prettytable;

use std::fs::File;
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::thread::sleep;
use std::time::Duration;
use std::{env, thread};

use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};

use crate::bridge::s3::S3;
use crate::bridge::Bridge;
use crate::cli::{BackupCommand, RestoreCommand, SubCommand, TransformerCommand, CLI};
use crate::config::{Config, DatabaseSubsetConfig};
use crate::source::{Source, SourceOptions};
use crate::tasks::{MaxBytes, TransferredBytes};
use crate::telemetry::{ClientOptions, TelemetryClient, TELEMETRY_TOKEN};
use crate::utils::epoch_millis;

mod bridge;
mod cli;
mod commands;
mod config;
mod connector;
mod destination;
mod runtime;
mod source;
mod tasks;
mod telemetry;
mod transformer;
mod types;
mod utils;

fn show_progress_bar(rx_pb: Receiver<(TransferredBytes, MaxBytes)>) {
    let pb = ProgressBar::new(0);
    pb.set_style(ProgressStyle::default_spinner());

    let mut style_is_progress_bar = false;
    let mut _max_bytes = 0usize;
    let mut last_transferred_bytes = 0usize;

    loop {
        let (transferred_bytes, max_bytes) = match rx_pb.try_recv() {
            Ok(msg) => msg,
            Err(_) => (last_transferred_bytes, _max_bytes),
        };

        if _max_bytes == 0 && style_is_progress_bar {
            // show spinner if there is no max_bytes indicated
            pb.set_style(ProgressStyle::default_spinner());
            style_is_progress_bar = false;
        } else if _max_bytes > 0 && !style_is_progress_bar {
            pb.set_style(ProgressStyle::default_bar()
                .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.green/blue}] {bytes}/{total_bytes} ({eta})")
                .progress_chars("#>-"));
            style_is_progress_bar = true;
        }

        if max_bytes != _max_bytes {
            pb.set_length(max_bytes as u64);
            _max_bytes = max_bytes;
        }

        last_transferred_bytes = transferred_bytes;
        pb.set_position(transferred_bytes as u64);

        sleep(Duration::from_micros(50));
    }
}

fn main() -> anyhow::Result<()> {
    let start_exec_time = utils::epoch_millis();
    let env_args = env::args().collect::<Vec<String>>();

    env_logger::init();
    let args = CLI::parse();

    let telemetry_client = match args.no_telemetry {
        true => None,
        false => Some(TelemetryClient::new(ClientOptions::from(TELEMETRY_TOKEN))),
    };

    let file = File::open(args.config)?;
    let config: Config = serde_yaml::from_reader(file)?;

    let mut bridge = S3::new(
        config.bridge.bucket()?,
        config.bridge.region()?,
        config.bridge.access_key_id()?,
        config.bridge.secret_access_key()?,
        config.bridge.endpoint()?,
    );

    match &config.source {
        Some(source) => {
            bridge.set_compression(source.compression.unwrap_or(true));
            bridge.set_encryption_key(source.encryption_key()?)
        }
        None => {}
    }

    match &config.destination {
        Some(dest) => {
            bridge.set_compression(dest.compression.unwrap_or(true));
            bridge.set_encryption_key(dest.encryption_key()?);
        }
        None => {}
    }

    let (tx_pb, rx_pb) = mpsc::sync_channel::<(TransferredBytes, MaxBytes)>(1000);
    let sub_commands: &SubCommand = &args.sub_commands;

    let telemetry_config = config.clone();

    if let Some(telemetry_client) = &telemetry_client {
        let _ = telemetry_client.capture_command(&telemetry_config, sub_commands, &env_args, None);
    }

    match sub_commands {
        // skip progress when output = true
        SubCommand::Restore(cmd) => match cmd {
            RestoreCommand::Local(args) => if args.output {},
            RestoreCommand::Remote(args) => if args.output {},
        },
        _ => {
            let _ = thread::spawn(move || show_progress_bar(rx_pb));
        }
    };

    let progress_callback = |bytes: TransferredBytes, max_bytes: MaxBytes| {
        let _ = tx_pb.send((bytes, max_bytes));
    };

    let _ = match sub_commands {
        SubCommand::Backup(cmd) => match cmd {
            BackupCommand::List => {
                let _ = commands::backup::list(&mut bridge)?;
                Ok(())
            }
            BackupCommand::Run(args) => {
                commands::backup::run(args, bridge, config, progress_callback)
            }
        },
        SubCommand::Transformer(cmd) => match cmd {
            TransformerCommand::List => {
                let _ = commands::transformer::list();
                Ok(())
            }
        },
        SubCommand::Restore(cmd) => match cmd {
            RestoreCommand::Local(args) => {
                commands::restore::local(args, bridge, progress_callback)
            }
            RestoreCommand::Remote(args) => {
                commands::restore::remote(args, bridge, config, progress_callback)
            }
        },
    };

    if let Some(telemetry_client) = &telemetry_client {
        let _ = telemetry_client.capture_command(
            &telemetry_config,
            sub_commands,
            &env_args,
            Some(epoch_millis() - start_exec_time),
        );
    }

    Ok(())
}
