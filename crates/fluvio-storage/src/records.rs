use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::path::Path;

use tracing::debug;

use fluvio_future::fs::File;
use fluvio_future::fs::util as file_util;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::AsyncFileExtension;
use dataplane::{Offset, Size};

use crate::util::generate_file_name;
use crate::validator::validate;
use crate::validator::LogValidationError;
use crate::config::ConfigOption;
use crate::StorageError;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

pub trait FileRecords {
    fn get_base_offset(&self) -> Offset;

    // fn get_file(&self) -> &File;

    fn get_path(&self) -> &Path;

    /// as file slice from position
    fn as_file_slice(&self, start: Size) -> Result<AsyncFileSlice, IoError>;

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError>;
}

pub struct FileRecordsSlice {
    base_offset: Offset,
    file: File,
    path: PathBuf,
    len: u64,
}

impl FileRecordsSlice {
    pub async fn open(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<FileRecordsSlice, StorageError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("opening commit log at: {}", log_path.display());

        let file = file_util::open(&log_path).await?;
        let metadata = file.metadata().await?;
        let len = metadata.len();

        Ok(FileRecordsSlice {
            base_offset,
            file,
            path: log_path,
            len,
        })
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub async fn validate(&mut self) -> Result<Offset, LogValidationError> {
        validate(&self.path).await
    }
}

impl FileRecords for FileRecordsSlice {
    fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    // fn get_file(&self) -> &File {
    //     &self.file
    // }

    fn get_path(&self) -> &Path {
        &self.path
    }

    fn as_file_slice(&self, start_pos: Size) -> Result<AsyncFileSlice, IoError> {
        Ok(self
            .file
            .raw_slice(start_pos as u64, self.len - start_pos as u64))
    }

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError> {
        if len as u64 > self.len {
            Err(IoError::new(
                ErrorKind::UnexpectedEof,
                "len is smaller than actual len",
            ))
        } else {
            Ok(self.file.raw_slice(start as u64, len as u64))
        }
    }
}

// message log doesn't have circular structure
impl Unpin for FileRecordsSlice {}
