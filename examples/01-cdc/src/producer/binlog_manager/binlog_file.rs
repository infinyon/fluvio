/// BinLogFile
///
/// Binary log file consists of all changes to the database.
///
use std::fmt;
use std::fs;
use std::io::Error;
use std::path::PathBuf;
use std::time::SystemTime;

use std::ffi::OsStr;

#[derive(Debug, PartialEq)]
pub struct BinLogFile {
    path: PathBuf,
    file: String,
    file_id: i32,
    offset: Option<u64>,
    modified: SystemTime,
}

impl fmt::Display for BinLogFile {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let offset = if let Some(offset) = self.offset {
            offset
        } else {
            0
        };
        write!(f, "[{:06} - {:20}] {:?}", self.file_id, offset, self.path)?;
        Ok(())
    }
}

impl BinLogFile {
    pub fn new(base_dir: &PathBuf, file_ref: &str, offset: Option<u64>) -> Result<Self, Error> {
        let path = base_dir.join(file_ref);
        let file_metadata = fs::metadata(&path)?;
        let file_id = get_file_id(&path);
        let file = file_ref.to_string();

        Ok(BinLogFile {
            path,
            file,
            file_id,
            offset,
            modified: file_metadata.modified()?,
        })
    }

    pub fn file_id(&self) -> i32 {
        self.file_id
    }

    pub fn file_name(&self) -> &str {
        &self.file
    }

    pub fn offset(&self) -> Option<u64> {
        self.offset
    }

    pub fn set_offset(&mut self, offset: Option<u64>) {
        if self.offset != offset {
            self.offset = offset;
        }
    }

    pub fn path_to_string(&self) -> String {
        self.path.clone().into_os_string().into_string().unwrap()
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
}

pub fn get_file_id(path: &PathBuf) -> i32 {
    let extension = path.extension().unwrap_or_else(|| OsStr::new("0"));
    let file_id = extension.to_str().unwrap_or("0").parse::<i32>().unwrap();

    file_id
}

#[cfg(test)]
mod test {
    use super::BinLogFile;

    const TEST_PATH: &str = "test_files";
    const BL_FILE1: &str = "binlog.000001";

    #[test]
    fn test_bin_file() {
        let program_dir = std::env::current_dir().unwrap();
        let bl_file_path = program_dir.join(TEST_PATH).join(BL_FILE1);
        let bl_path = program_dir.join(TEST_PATH);
        let file = BL_FILE1.to_owned();
        let offset = None;

        let bin_file_res = BinLogFile::new(&bl_path, &file, offset);
        assert!(bin_file_res.is_ok());

        // test - display
        let bin_file = bin_file_res.unwrap();
        assert_eq!(
            format!("{}", bin_file),
            format!("[000001 -                    0] {:?}", bl_file_path)
        );

        // test - file
        assert_eq!(bin_file.file, BL_FILE1);

        // test - id
        assert_eq!(bin_file.file_id, 1);

        // test - path to string
        assert_eq!(
            bin_file.path_to_string(),
            bl_file_path.into_os_string().into_string().unwrap()
        );
    }
}
