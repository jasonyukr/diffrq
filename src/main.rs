use std::{
    cmp::Ordering,
    collections::HashSet,
    ffi::OsString,
    fs::{self, File},
    io::{BufReader, Read},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};
use rayon::prelude::*;
use sha2::{Digest, Sha256};

/// A simple struct to hold necessary info from fs::DirEntry.
/// Caching this info avoids redundant metadata() syscalls.
#[derive(Debug)]
struct EntryInfo {
    path: PathBuf,
    file_name: OsString,
    is_dir: bool,
    len: u64,
}

impl EntryInfo {
    fn from_dir_entry(entry: fs::DirEntry) -> Result<Self> {
        let metadata = entry.metadata()?;
        Ok(EntryInfo {
            path: entry.path(),
            file_name: entry.file_name(),
            is_dir: entry.path().is_dir(),
            len: metadata.len(),
        })
    }
}

/// Formats a path or filename for output, adding quotes if it contains spaces.
fn format_path(path_like: &Path) -> String {
    let path_str = path_like.to_string_lossy();
    if path_str.contains(' ') {
        format!("\"{}\"", path_str)
    } else {
        path_str.into_owned()
    }
}

/// Compares the content of two files by size and then by SHA-256 hash.
fn are_files_same_with_reuse(
    path1: &Path,
    path2: &Path,
    buffer: &mut [u8],
    hasher1: &mut Sha256,
    hasher2: &mut Sha256,
) -> Result<bool> {
    let meta1 = fs::metadata(path1)?;
    let meta2 = fs::metadata(path2)?;

    if meta1.len() != meta2.len() {
        return Ok(false);
    }
    if meta1.len() == 0 {
        return Ok(true);
    }

    // Hash file 1
    hasher1.reset();
    let mut file1 = BufReader::new(File::open(path1)?);
    loop {
        let read = file1.read(buffer)?;
        if read == 0 { break; }
        hasher1.update(&buffer[..read]);
    }
    let hash1 = hasher1.finalize_reset();

    // Hash file 2
    hasher2.reset();
    let mut file2 = BufReader::new(File::open(path2)?);
    loop {
        let read = file2.read(buffer)?;
        if read == 0 { break; }
        hasher2.update(&buffer[..read]);
    }
    let hash2 = hasher2.finalize_reset();

    Ok(hash1 == hash2)
}

/// Recursively compares two directories in parallel and returns a list of differences.
fn compare_directories(
    dir1: &Path,
    dir2: &Path,
    excludes: &HashSet<OsString>,
) -> Result<Vec<String>> {
    let read_entries = |dir: &Path| -> Result<Vec<EntryInfo>> {
        fs::read_dir(dir)?
            .filter_map(|res| res.ok())
            // The core exclusion logic: filter out any entry whose name is in the excludes set.
            .filter(|entry| !excludes.contains(&entry.file_name()))
            .map(EntryInfo::from_dir_entry)
            .collect::<Result<Vec<_>>>()
    };

    let mut entries1 = read_entries(dir1)?;
    let mut entries2 = read_entries(dir2)?;

    entries1.sort_by(|a, b| a.file_name.cmp(&b.file_name));
    entries2.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    let mut differences = Vec::new();
    let mut files_to_compare = Vec::new();
    let mut dirs_to_compare = Vec::new();

    let mut iter1 = entries1.into_iter();
    let mut iter2 = entries2.into_iter();
    let mut entry1 = iter1.next();
    let mut entry2 = iter2.next();

    // Two-pointer comparison algorithm
    loop {
        match (entry1.as_ref(), entry2.as_ref()) {
            (Some(e1), Some(e2)) => match e1.file_name.cmp(&e2.file_name) {
                Ordering::Less => {
                    differences.push(format!("Only in {}: {}", format_path(dir1), format_path(Path::new(&e1.file_name))));
                    entry1 = iter1.next();
                }
                Ordering::Greater => {
                    differences.push(format!("Only in {}: {}", format_path(dir2), format_path(Path::new(&e2.file_name))));
                    entry2 = iter2.next();
                }
                Ordering::Equal => {
                    if e1.is_dir != e2.is_dir {
                        differences.push(format!(
                            "File {} is a {} while file {} is a {}",
                            format_path(&e1.path), if e1.is_dir { "directory" } else { "regular file" },
                            format_path(&e2.path), if e2.is_dir { "directory" } else { "regular file" }
                        ));
                    } else if e1.is_dir {
                        dirs_to_compare.push((e1.path.clone(), e2.path.clone()));
                    } else if e1.len != e2.len {
                        differences.push(format!("Files {} and {} differ", format_path(&e1.path), format_path(&e2.path)));
                    } else {
                        files_to_compare.push((e1.path.clone(), e2.path.clone()));
                    }
                    entry1 = iter1.next();
                    entry2 = iter2.next();
                }
            },
            (Some(e1), None) => {
                differences.push(format!("Only in {}: {}", format_path(dir1), format_path(Path::new(&e1.file_name))));
                entry1 = iter1.next();
            }
            (None, Some(e2)) => {
                differences.push(format!("Only in {}: {}", format_path(dir2), format_path(Path::new(&e2.file_name))));
                entry2 = iter2.next();
            }
            (None, None) => break,
        }
    }

    // === PARALLEL PROCESSING ===
    let file_diffs: Vec<String> = files_to_compare
        .into_par_iter()
        .filter_map(|(p1, p2)| {
            let mut buffer = vec![0u8; 8192]; // 8KB shared buffer
            let mut hasher1 = Sha256::new();
            let mut hasher2 = Sha256::new();
            match are_files_same_with_reuse(&p1, &p2, &mut buffer, &mut hasher1, &mut hasher2) {
                Ok(true) => None,
                Ok(false) => Some(format!("Files {} and {} differ", format_path(&p1), format_path(&p2))),
                Err(e) => Some(format!("Error comparing {} and {}: {}", format_path(&p1), format_path(&p2), e)),
            }
        })
        .collect();

    let dir_diffs: Vec<String> = dirs_to_compare
        .into_par_iter()
        .flat_map(|(d1, d2)| {
            // Pass the excludes set down in the recursive call.
            match compare_directories(&d1, &d2, excludes) {
                Ok(diffs) => diffs,
                Err(e) => vec![format!("Error comparing subdirectories {} and {}: {}", format_path(&d1), format_path(&d2), e)],
            }
        })
        .collect();

    differences.extend(file_diffs);
    differences.extend(dir_diffs);

    Ok(differences)
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let mut dir1 = None;
    let mut dir2 = None;
    let mut excludes = HashSet::new();

    // Manual argument parsing
    while let Some(arg) = args.next() {
        if arg == "--exclude" {
            if let Some(pattern) = args.next() {
                excludes.insert(OsString::from(pattern));
            } else {
                anyhow::bail!("--exclude flag requires a value.");
            }
        } else if !arg.starts_with('-') {
            if dir1.is_none() {
                dir1 = Some(PathBuf::from(arg));
            } else if dir2.is_none() {
                dir2 = Some(PathBuf::from(arg));
            } else {
                anyhow::bail!("Too many directory arguments specified.");
            }
        } else {
            anyhow::bail!("Unknown flag or option: {}", arg);
        }
    }

    let dir1 = dir1.context("Missing required argument: <directory1>")?;
    let dir2 = dir2.context("Missing required argument: <directory2>")?;

    for (name, path) in [("directory1", &dir1), ("directory2", &dir2)] {
        if !path.is_dir() {
            return Err(anyhow::anyhow!("Input path for {} is not a valid directory: {}", name, path.display()));
        }
    }

    let differences = compare_directories(&dir1, &dir2, &excludes)
        .with_context(|| format!("Failed to compare directories '{}' and '{}'", dir1.display(), dir2.display()))?;

    if differences.is_empty() {
        std::process::exit(0);
    } else {
        for diff in differences {
            println!("{}", diff);
        }
        std::process::exit(1);
    }
}
