/// IndexFile
///
/// Contains a list of log-bin files:
///     /var/lib/mysql/binlog.000001
///     /var/lib/mysql/binlog.000002
///     /var/lib/mysql/binlog.000003
///
/// When binlog file is rotated, this file is updated first.
///
use std::fs;
use std::io::prelude::*;
use std::io::{self, Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

#[derive(Debug)]
pub struct IndexFile {
    path: PathBuf,
    modified: SystemTime,
}

impl IndexFile {
    pub fn new(base_dir: &PathBuf, index_file: String) -> Result<Self, Error> {
        let path = get_index_file_path(base_dir, index_file)?;
        let file_metadata = fs::metadata(&path)?;

        Ok(IndexFile {
            path,
            modified: file_metadata.modified()?,
        })
    }

    pub fn has_changed(&mut self) -> Result<bool, Error> {
        let file_metadata = fs::metadata(&self.path)?;
        let last_changed = file_metadata.modified()?;

        if last_changed > self.modified {
            self.modified = last_changed;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn get_bin_log_files(&self) -> Result<Vec<String>, Error> {
        let reader = io::BufReader::new(fs::File::open(&self.path)?);
        let mut result: Vec<String> = vec![];

        for line in reader.lines() {
            let line = line?;
            match Path::new(&line).file_name() {
                Some(name) => {
                    let file_name = name.to_str().unwrap_or("");
                    result.push(file_name.to_owned());
                }
                None => {
                    return Err(Error::new(
                        ErrorKind::InvalidInput,
                        format!("Invalid index file: {}", line),
                    ))
                }
            };
        }

        if result.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidInput,
                "no bin-log files found",
            ));
        }

        Ok(result)
    }
}

fn get_index_file_path(base_dir: &PathBuf, file: String) -> Result<PathBuf, Error> {
    let path = base_dir.join(file);

    match path.exists() {
        true => Ok(path),
        false => Err(Error::new(
            ErrorKind::InvalidInput,
            format!("cannot find {:?} file", path),
        )),
    }
}

#[cfg(test)]
mod test {
    use super::IndexFile;

    const INDEX_FILE: &str = "binlog.index";
    const TEST_PATH: &str = "test_files";
    const BL_FILE1: &str = "binlog.000001";
    const BL_FILE2: &str = "binlog.000002";
    const BL_FILE3: &str = "binlog.000003";

    #[test]
    fn test_index_file() {
        let program_dir = std::env::current_dir().unwrap();
        let base_dir = program_dir.join(TEST_PATH);
        let index_file_path = base_dir.join(INDEX_FILE);

        let index_file = IndexFile::new(&base_dir, INDEX_FILE.to_owned());

        assert!(index_file.is_ok());
        assert_eq!(index_file.as_ref().unwrap().path, index_file_path);

        let index_file = index_file.unwrap();
        let bn_files = index_file.get_bin_log_files();
        assert!(bn_files.is_ok());

        let exp_bn_files = [BL_FILE1, BL_FILE2, BL_FILE3];
        assert_eq!(bn_files.unwrap(), exp_bn_files);
    }
}
