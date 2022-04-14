use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Error, Write};
use std::path::Path;

pub type Line<'a> = &'a str;
pub type GroupHash<'a> = &'a str;

/// Deduplicate lines from a file.
///
/// ## How it works
/// This function basically take a file path as input and alternates the content of this file to
/// literally deduplicate every matching lines.
/// This function is optimized to not eat too much memory since it can be used on very large file.
/// (However, it has not been benched yet). Here is how it works and how we keep the memory usage as low as possible:
///
/// 1. Find the *portion* of the file where the *matched lines* are (start index, end index).
/// 2. Group lines by pattern on disk or memory
/// 3. Deduplicate groups
/// 4. Rewrite file portion with deduplicated data
pub fn dedup_lines<F: Fn(Line) -> bool, G: Fn(Line) -> GroupHash>(
    file_path: &Path,
    match_line: F,
    group: G,
) -> Result<(), Error> {
    let file = File::open(file_path)?;
    let reader = BufReader::new(file);
    let mut hashes = HashSet::new();
    let temp_directory = tempfile::tempdir()?;
    let temp_directory_path = temp_directory.as_ref();

    let mut first_line_portion_index = 0usize;
    let mut last_line_portion_index = 0usize;

    // dedup
    for (idx, line) in reader.lines().enumerate() {
        let line = line?;
        if match_line(line.as_str()) {
            last_line_portion_index = idx;
            if first_line_portion_index == 0 {
                first_line_portion_index = idx;
            }

            let hash = group(line.as_str());
            /// TODO we can use a bloom filter here to improve the `add_line` performance
            let _ = add_line(temp_directory_path, hash, line.as_str())?;
            let _ = hashes.insert(hash);
        }
    }

    // remove matched lines from the original file

    // aggregate
    for hash in hashes {
        // TODO
    }

    // put dedup matched lines to the original file
    // TODO

    Ok(())
}

/// Create or find the appropriate file based on the `group_hash` and append the line if it does not already exist.
fn add_line(temp_directory: &Path, group_hash: GroupHash, line: Line) -> Result<(), Error> {
    let file_path = temp_directory.join(group_hash);
    let file = match File::open(file_path.as_path()) {
        Ok(file) => file,
        Err(_) => File::create(file_path.as_path())?,
    };

    let reader = BufReader::new(file);
    for file_line in reader.lines() {
        let file_line = file_line?;
        if file_line.as_str() == line {
            // the line already exist in the file, we can stop here
            return Ok(());
        }
    }

    // append the line because it does not exist
    let mut file = OpenOptions::new()
        .write(true)
        .append(true)
        .open(file_path.as_path())?;

    let _ = write!(file, line)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn check_dedup_file() {
        // TODO
    }
}
