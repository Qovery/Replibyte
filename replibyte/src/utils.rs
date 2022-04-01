use prettytable::{format, Table};
use std::io::{Error, ErrorKind};
use std::time::{SystemTime, UNIX_EPOCH};
use which::which;

pub fn epoch_millis() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
}

pub fn table() -> Table {
    // Create the table
    let mut table = Table::new();

    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);

    table
}

/// converts Bytes into Bytes, KB, MB, GB or TB
pub fn to_human_readable_unit(bytes: usize) -> String {
    match bytes {
        0..=1023 => format!("{} Bytes", bytes),
        1024..=1023_999 => format!("{:.2} kB", bytes / 1000),
        1024_000..=1023_999_999 => format!("{:.2} MB", bytes / 1_000_000),
        1024_000_000..=1023_999_999_999 => format!("{:.2} MB", bytes / 1_000_000_000),
        1024_000_000_000..=1023_999_999_999_999 => format!("{:.2} GB", bytes / 1_000_000_000_000),
        _ => format!("{:.2} TB", bytes / 1_000_000_000_000_000),
    }
}

/// check for binary presence in PATH
pub fn binary_exists(binary_name: &str) -> Result<(), Error> {
    let _ = which(binary_name).map_err(|_| {
        Error::new(
            ErrorKind::Other,
            format!("cannot find '{}' binary in path", binary_name),
        )
    })?;

    Ok(())
}
