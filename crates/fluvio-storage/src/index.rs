use std::ffi::OsStr;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;
use std::path::PathBuf;
use std::slice;
use std::sync::Arc;

use libc::c_void;
use tracing::debug;
use tracing::trace;
use pin_utils::unsafe_unpinned;

use fluvio_future::fs::mmap::MemoryMappedFile;
use fluvio_protocol::record::Offset;
use fluvio_protocol::record::{Size, Size64};

use crate::config::SharedReplicaConfig;
use crate::util::generate_file_name;
use crate::util::log_path_get_offset;
use crate::validator::LogValidationError;
use crate::config::ReplicaConfig;
use crate::StorageError;

/// size of the memory mapped isze
pub const INDEX_ENTRY_SIZE: u64 = size_of::<Entry>() as u64;

pub const EXTENSION: &str = "index";

pub type Entry = (Size, Size);

pub trait Index {
    /// Find Offset position in the index
    /// This will find index entry with least and min offset.
    /// For example, if we have index entries:
    /// 0: [30,100]    // offset: 30, position: 100
    /// 1: [100,2000] // offset: 100, position: 2000
    /// 2: [300,4000] // offset: 300, position: 4000
    /// 3: [500,6000] // offset: 500, position: 6000
    ///
    /// find_offset(50) will return Some((4,100))
    /// find_offset(1000) will return Some((500,6000))
    /// find_offset(10) will return None
    fn find_offset(&self, relative_offset: Size) -> Option<Entry>;

    /// Index actual size in bytes
    fn len(&self) -> Size64;

    /// Index capacity in bytes
    fn capacity(&self) -> Size64;
}

pub trait OffsetPosition: Sized {
    /// convert to big endian format, this must be performed before send out to caller
    fn to_be(self) -> Self;

    /// offset
    fn offset(&self) -> Size;

    /// file position
    fn position(&self) -> Size;
}

impl OffsetPosition for Entry {
    fn to_be(self) -> Self {
        (self.0.to_be(), self.1.to_be())
    }

    #[inline(always)]
    fn offset(&self) -> Size {
        self.0
    }

    #[inline(always)]
    fn position(&self) -> Size {
        self.1
    }
}

/// Segment index
///
/// Maps offset into file position (Seek)
///
/// It is backed by memory mapped file
///
/// For active segment, index can grow
/// For non active, it is fixed
pub struct LogIndex {
    #[allow(dead_code)]
    mmap: MemoryMappedFile,
    path: PathBuf,
    ptr: *mut c_void,
    len: Size,
}

unsafe impl Send for LogIndex {}

unsafe impl Sync for LogIndex {}

impl LogIndex {
    unsafe_unpinned!(mmap: MemoryMappedFile);

    pub async fn open_from_offset(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<Self, IoError> {
        let index_file_path = generate_file_name(&option.base_dir, base_offset, EXTENSION);

        debug!(?index_file_path, "opening index");

        // make sure it is log file
        let (m_file, file) = MemoryMappedFile::open(&index_file_path, INDEX_ENTRY_SIZE).await?;

        let len = (file.metadata().await?).len();

        debug!(len, "memory mapped len");

        if len > u32::MAX as u64 {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "index file should not exceed u32",
            ));
        }
        let ptr = {
            let b_slices: &[u8] = &m_file.inner();
            b_slices.as_ptr() as *mut libc::c_void
        };

        Ok(LogIndex {
            mmap: m_file,
            path: index_file_path,
            ptr,
            len: len as Size,
        })
    }

    pub async fn open_from_path<P>(path: P) -> Result<Self, StorageError>
    where
        P: AsRef<Path>,
    {
        let path_ref = path.as_ref();
        let base_offset = log_path_get_offset(path_ref)?;
        if path_ref.extension() != Some(OsStr::new(EXTENSION)) {
            return Err(StorageError::LogValidation(
                LogValidationError::InvalidExtension,
            ));
        }

        let option = ReplicaConfig {
            base_dir: path_ref.parent().unwrap().to_path_buf(),
            ..Default::default()
        };

        LogIndex::open_from_offset(base_offset, Arc::new(option.into()))
            .await
            .map_err(|err| err.into())
    }

    #[inline]
    pub fn ptr(&self) -> *const Entry {
        self.ptr as *const Entry
    }

    /// return file path to be removed
    pub fn clean(self) -> PathBuf {
        self.path
    }
}

impl Index for LogIndex {
    fn find_offset(&self, offset: Size) -> Option<Entry> {
        lookup_entry(self, offset).map(|idx| self[idx].to_be())
    }

    fn len(&self) -> Size64 {
        self.len as u64
    }

    /// Index capacity in bytes. Since [LogIndex] is a read-only index capacity always equals length.
    fn capacity(&self) -> Size64 {
        self.len()
    }
}

impl Deref for LogIndex {
    type Target = [Entry];

    #[inline]
    fn deref(&self) -> &[Entry] {
        unsafe { slice::from_raw_parts(self.ptr(), (self.capacity() / INDEX_ENTRY_SIZE) as usize) }
    }
}

/// perform binary search given
pub(crate) fn lookup_entry(offsets: &[Entry], offset: Size) -> Option<usize> {
    let first_entry = offsets[0].to_be();
    if offset < first_entry.offset() {
        trace!(
            offset,
            first = first_entry.offset(),
            "offset is less than: first",
        );
        return None;
    }

    match offsets.binary_search_by(|entry| entry.offset().to_be().cmp(&offset)) {
        Ok(idx) => Some(idx),
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
mod tests {

    use super::lookup_entry;
    use super::OffsetPosition;

    #[test]
    fn test_index_search() {
        fluvio_future::subscriber::init_logger();

        // offset increase by 4000
        let indexes = [
            (3, 10).to_be(),
            (7, 350).to_be(),
            (9, 400).to_be(),
            (13, 600).to_be(),
            (15, 8000).to_be(),
            (21, 12000).to_be(),
        ];

        assert!(lookup_entry(&indexes, 1).is_none());

        assert_eq!(lookup_entry(&indexes, 3), Some(0));
        assert_eq!(lookup_entry(&indexes, 10), Some(2)); // (9,400)
        assert_eq!(lookup_entry(&indexes, 14), Some(3)); // (13,600)
        assert_eq!(lookup_entry(&indexes, 50), Some(5)); // (21,12000) max
    }
}
