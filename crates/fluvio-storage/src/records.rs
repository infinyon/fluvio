use std::io::Error as IoError;
use std::io::ErrorKind;
use std::os::unix::prelude::AsRawFd;
use std::os::unix::prelude::FromRawFd;
use std::path::PathBuf;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use std::time::SystemTime;
use std::time::SystemTimeError;

use tracing::debug;
use tracing::error;
use tracing::info;
use anyhow::Result;

use fluvio_future::fs::File;
use fluvio_future::fs::remove_file;
use fluvio_future::fs::util as file_util;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::AsyncFileExtension;
use fluvio_protocol::record::{Offset, Size, Size64};

use crate::LogIndex;
use crate::config::SharedReplicaConfig;
use crate::util::generate_file_name;
use crate::validator::LogValidator;
use crate::StorageError;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

#[allow(clippy::len_without_is_empty)]
pub trait FileRecords {
    /// get clone of the file
    fn file(&self) -> File;

    fn get_base_offset(&self) -> Offset;

    fn len(&self) -> Size64;

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
    last_modified_time: SystemTime,
}

impl FileRecordsSlice {
    pub async fn open(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<FileRecordsSlice, StorageError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);

        let file = file_util::open(&log_path).await?;
        let metadata = file.metadata().await?;
        let len = metadata.len();
        let last_modified_time = metadata.modified()?;

        debug!(
            path = %log_path.display(),
            len,
            seconds = last_modified_time.elapsed().map_err(|err| StorageError::Other(format!("Other: {err:#?}")))?. as_secs(),
            "opened read only records");
        Ok(FileRecordsSlice {
            base_offset,
            file,
            path: log_path,
            len,
            last_modified_time,
        })
    }

    pub fn get_len(&self) -> u64 {
        self.len
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub async fn validate(&self, index: &LogIndex) -> Result<LogValidator> {
        LogValidator::default_validate(&self.path, Some(index)).await
    }

    pub fn modified_time_elapsed(&self) -> Result<Duration, SystemTimeError> {
        self.last_modified_time.elapsed()
    }

    pub(crate) fn is_expired(&self, expired_duration: &Duration) -> bool {
        match self.last_modified_time.elapsed() {
            Ok(ref elapsed) => {
                debug!(elapsed = %elapsed.as_secs(), path = %self.path.display(), "segment");
                elapsed > expired_duration
            }
            Err(err) => {
                error!(path = %self.path.display(),"failed to get last modified time: {:?}", err);
                false
            }
        }
    }

    pub(crate) async fn remove(self) -> Result<(), IoError> {
        info!(log_path = %self.path.display(),"removing log file");
        remove_file(&self.path).await
    }
}

impl FileRecords for FileRecordsSlice {
    fn file(&self) -> File {
        unsafe { File::from_raw_fd(self.file.as_raw_fd()) }
    }

    fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    fn len(&self) -> Size64 {
        self.len
    }

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
