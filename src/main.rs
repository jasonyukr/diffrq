use std::{
    cell::RefCell,
    cmp::Ordering,
    collections::HashSet,
    ffi::OsString,
    fs::{self, File},
    io::{self, BufReader, Read},
    path::{Path, PathBuf},
};

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
struct EntryInfo {
    path: PathBuf,
    file_name: OsString,
    is_dir: bool,
    len: u64,
}

impl EntryInfo {
    fn from_dir_entry(entry: fs::DirEntry) -> Result<Self> {
        let metadata = entry.metadata()?;
        Ok(Self {
            path: entry.path(),
            file_name: entry.file_name(),
            is_dir: entry.path().is_dir(),
            len: metadata.len(),
        })
    }
}

thread_local! {
    static THREAD_BUFFERS: RefCell<ThreadLocalBuffers> = RefCell::new(ThreadLocalBuffers::new());
}

struct ThreadLocalBuffers {
    buffer1: Vec<u8>,
    buffer2: Vec<u8>,
}

impl ThreadLocalBuffers {
    fn new() -> Self {
        Self {
            buffer1: vec![0; 131072], // 128 KB
            buffer2: vec![0; 131072],
        }
    }
}

fn files_are_identical(p1: &Path, p2: &Path) -> io::Result<bool> {
    let mut f1 = BufReader::new(File::open(p1)?);
    let mut f2 = BufReader::new(File::open(p2)?);

    THREAD_BUFFERS.with(|res| {
        let mut buffers = res.borrow_mut();
        let ThreadLocalBuffers { buffer1, buffer2 } = &mut *buffers;

        loop {
            let b1 = f1.read(buffer1)?;
            let b2 = f2.read(buffer2)?;
            if b1 != b2 || buffer1[..b1] != buffer2[..b2] {
                return Ok(false);
            }
            if b1 == 0 {
                return Ok(true);
            }
        }
    })
}

fn compare_directories<F>(dir1: &Path, dir2: &Path, excludes: &HashSet<OsString>, all_mode: bool, report: &F) -> Result<()>
where
    F: Fn(&str),
{
    let read_entries = |dir: &Path| -> Result<Vec<EntryInfo>> {
        fs::read_dir(dir)?
            .filter_map(Result::ok)
            .filter(|entry| !excludes.contains(&entry.file_name()))
            .map(EntryInfo::from_dir_entry)
            .collect()
    };

    let mut entries1 = read_entries(dir1)?;
    let mut entries2 = read_entries(dir2)?;

    entries1.sort_by(|a, b| a.file_name.cmp(&b.file_name));
    entries2.sort_by(|a, b| a.file_name.cmp(&b.file_name));

    let mut files_to_compare = vec![];
    let mut dirs_to_compare = vec![];

    let mut i1 = entries1.into_iter();
    let mut i2 = entries2.into_iter();
    let mut e1 = i1.next();
    let mut e2 = i2.next();

    while e1.is_some() || e2.is_some() {
        match (e1.as_ref(), e2.as_ref()) {
            (Some(a), Some(b)) => match a.file_name.cmp(&b.file_name) {
                Ordering::Less => {
                    report(&format!("D:{}", a.path.to_string_lossy()));
                    e1 = i1.next();
                }
                Ordering::Greater => {
                    report(&format!("A:{}", b.path.to_string_lossy()));
                    e2 = i2.next();
                }
                Ordering::Equal => {
                    if a.is_dir != b.is_dir {
                        report(&format!("D:{}", a.path.to_string_lossy()));
                        report(&format!("A:{}", b.path.to_string_lossy()));
                    } else if a.is_dir {
                        dirs_to_compare.push((a.path.clone(), b.path.clone()));
                    } else if a.len != b.len {
                        report(&format!("M:{}", b.path.to_string_lossy()));
                    } else if a.len > 0 {
                        if all_mode {
                            // report immediately in "--all-mode" to keep the order of files
                            let p1 = &a.path.clone();
                            let p2 = &b.path.clone();
                            match files_are_identical(&p1, &p2) {
                                Ok(false) => report(&format!("M:{}", p2.to_string_lossy())),
                                Ok(true) => if all_mode { report(&format!("-:{}", p2.to_string_lossy())) },
                                Err(e) => report(&format!(
                                        "E:Failed to compare '{}' and '{}': {}",
                                        p1.display(),
                                        p2.display(),
                                        e
                                )),
                            }
                        } else {
                            files_to_compare.push((a.path.clone(), b.path.clone()));
                        }
                    }
                    e1 = i1.next();
                    e2 = i2.next();
                }
            },
            (Some(a), None) => {
                report(&format!("D:{}", a.path.to_string_lossy()));
                e1 = i1.next();
            }
            (None, Some(b)) => {
                report(&format!("A:{}", b.path.to_string_lossy()));
                e2 = i2.next();
            }
            _ => break,
        }
    }

    for (p1, p2) in files_to_compare {
        match files_are_identical(&p1, &p2) {
            Ok(false) => report(&format!("M:{}", p2.to_string_lossy())),
            Ok(true) => if all_mode { report(&format!("-:{}", p2.to_string_lossy())) },
            Err(e) => report(&format!(
                "E:Failed to compare '{}' and '{}': {}",
                p1.display(),
                p2.display(),
                e
            )),
        }
    }

    for (d1, d2) in dirs_to_compare {
        if let Err(e) = compare_directories(&d1, &d2, excludes, all_mode, report) {
            report(&format!(
                "E:error comparing subdirectories {} and {}: {}",
                d1.display(),
                d2.display(),
                e
            ));
        }
    }

    Ok(())
}

fn main() -> Result<()> {
    let mut all_mode = false;
    let mut noformat_mode = false;
    let mut args = std::env::args().skip(1);
    let mut dir1 = None;
    let mut dir2 = None;
    let mut excludes = HashSet::new();

    while let Some(arg) = args.next() {
        if arg == "--all" {
            all_mode = true;
        } else if arg == "--noformat" {
            noformat_mode = true;
        } else if arg == "--exclude" {
            if let Some(value) = args.next() {
                excludes.insert(OsString::from(value));
            } else {
                anyhow::bail!("Missing value after --exclude");
            }
        } else if !arg.starts_with('-') {
            if dir1.is_none() {
                dir1 = Some(PathBuf::from(arg));
            } else if dir2.is_none() {
                dir2 = Some(PathBuf::from(arg));
            } else {
                anyhow::bail!("Too many positional arguments");
            }
        } else {
            anyhow::bail!("Unknown flag: {}", arg);
        }
    }

    let dir1 = dir1.context("Missing <directory1>")?;
    let dir2 = dir2.context("Missing <directory2>")?;

    for (label, path) in [("directory1", &dir1), ("directory2", &dir2)] {
        if !path.is_dir() {
            anyhow::bail!("{} is not a directory: {}", label, path.display());
        }
    }

    let dir1_ref = dir1.as_path();
    let dir2_ref = dir2.as_path();

    let report_fn = |line: &str| {
        if let Some((tag, raw_path)) = line.split_once(':') {
            let full_path = Path::new(raw_path);
            let reduced = match tag {
                "M" | "A" | "-" => full_path.strip_prefix(dir2_ref).unwrap_or(full_path),
                "D" => full_path.strip_prefix(dir1_ref).unwrap_or(full_path),
                _ => full_path,
            };

            let is_dir = full_path.is_dir();
            let path_str = reduced.to_string_lossy();
            let display_path = format!("{path_str}{}", if is_dir { "/" } else { "" });

            if noformat_mode {
                match tag {
                    "M" => println!("M: {display_path}"),
                    "A" => println!("A: {display_path}"),
                    "D" => println!("D: {display_path}"),
                    "-" => println!("-: {display_path}"),
                    "E" => eprintln!("Error: {display_path}"),
                    _ => {}
                }
            } else {
                match tag {
                    "M" => println!("M │\x1b[34m▮▮\x1b[0m│ \x1b[34m{display_path}\x1b[0m"),
                    "A" => println!("A │\x1b[32m ▮\x1b[0m│ \x1b[32m{display_path}\x1b[0m"),
                    "D" => println!("D │\x1b[31m▮ \x1b[0m│ \x1b[31m{display_path}\x1b[0m"),
                    "-" => println!("- │▮▮│ {display_path}"),
                    "E" => eprintln!("\x1b[91mError: {display_path}\x1b[0m"),
                    _ => {}
                }
            }
        }
    };

    compare_directories(dir1_ref, dir2_ref, &excludes, all_mode, &report_fn)?;

    Ok(())
}

