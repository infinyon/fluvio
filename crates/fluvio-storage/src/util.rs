use std::path::Path;
use std::path::PathBuf;
use std::num::ParseIntError;

use fluvio_protocol::record::Offset;

/// given parent directory, base offset, extension, generate path
pub fn generate_file_name<P>(parent_dir: P, base_offset: Offset, extension: &str) -> PathBuf
where
    P: AsRef<Path>,
{
    let mut file = parent_dir.as_ref().join(format!("{base_offset:020}"));
    file.set_extension(extension);
    file
}

#[derive(Debug, thiserror::Error)]
pub enum OffsetError {
    #[error("Offset does not exist")]
    NotExistent,
    #[error("Invalid path")]
    InvalidPath,
    #[error("Invalid logfile name")]
    InvalidLogFileName,
    #[error("Failed to parse offset")]
    OffsetParse(#[from] ParseIntError),
}

pub fn log_path_get_offset<P>(path: P) -> Result<Offset, OffsetError>
where
    P: AsRef<Path>,
{
    let log_path = path.as_ref();

    match log_path.file_stem() {
        None => Err(OffsetError::InvalidPath),
        Some(file_name) => {
            if file_name.len() != 20 {
                Err(OffsetError::InvalidLogFileName)
            } else {
                file_name
                    .to_str()
                    .unwrap()
                    .parse()
                    .map_err(|err: ParseIntError| err.into())
            }
        }
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;
    use std::ffi::OsStr;

    use super::generate_file_name;
    use super::log_path_get_offset;

    #[test]
    fn test_file_generation() {
        let dir = temp_dir();
        let path = generate_file_name(dir, 5, "log");
        assert_eq!(
            path.file_name(),
            Some(OsStr::new("00000000000000000005.log"))
        );
    }

    #[test]
    fn test_log_path() {
        let test_val = log_path_get_offset(temp_dir().join("00000000000000000005.log"));
        assert!(test_val.is_ok());
        assert_eq!(test_val.unwrap(), 5);
        assert!(log_path_get_offset(temp_dir().join("jwowow.log")).is_err());
        assert!(log_path_get_offset(temp_dir().join("00000000000000000005.txt")).is_ok());
        assert!(log_path_get_offset(temp_dir().join("00000000000000005.log")).is_err());
    }
}
