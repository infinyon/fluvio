use std::ffi::OsStr;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::mem::size_of;
use std::ops::Deref;
use std::path::Path;
use std::slice;
use std::sync::Arc;

use libc::c_void;
use tracing::debug;
use tracing::trace;
use pin_utils::unsafe_unpinned;

use fluvio_future::fs::mmap::MemoryMappedFile;
use dataplane::{Offset, Size};

use crate::config::SharedReplicaConfig;
use crate::util::generate_file_name;
use crate::util::log_path_get_offset;
use crate::validator::LogValidationError;
use crate::config::ReplicaConfig;
use crate::StorageError;

/// size of the memory mapped isze
const INDEX_ENTRY_SIZE: Size = (size_of::<Size>() * 2) as Size;

pub const EXTENSION: &str = "index";

pub trait Index {
    fn find_offset(&self, relative_offset: Size) -> Option<(Size, Size)>;

    fn len(&self) -> Size;
    fn entries(&self) -> Size {
        self.len() / INDEX_ENTRY_SIZE
    }
}

pub trait OffsetPosition: Sized {
    /// convert to be endian
    #[allow(clippy::wrong_self_convention)]
    fn to_be(self) -> Self;

    fn offset(&self) -> Size;

    fn position(&self) -> Size;
}

impl OffsetPosition for (Size, Size) {
    fn to_be(self) -> Self {
        let (offset, pos) = self;
        (offset.to_be(), pos.to_be())
    }

    #[inline(always)]
    fn offset(&self) -> Size {
        self.0.to_be()
    }

    #[inline(always)]
    fn position(&self) -> Size {
        self.1.to_be()
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

// implement index file
pub struct LogIndex {
    #[allow(dead_code)]
    mmap: MemoryMappedFile,
    ptr: *mut c_void,
    len: Size,
}

// const MEM_SIZE: u64 = 1024 * 1024 * 10; //10 MBs

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
        let (m_file, file) =
            MemoryMappedFile::open(index_file_path, INDEX_ENTRY_SIZE as u64).await?;

        let len = (file.metadata().await?).len();

        debug!(len, "memory mapped len");

        if len > std::u32::MAX as u64 {
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
    pub fn ptr(&self) -> *const (Size, Size) {
        self.ptr as *const (Size, Size)
    }
}

impl Index for LogIndex {
    fn find_offset(&self, offset: Size) -> Option<(Size, Size)> {
        lookup_entry(self, offset).map(|idx| self[idx])
    }

    fn len(&self) -> Size {
        self.len
    }
}

impl Deref for LogIndex {
    type Target = [(Size, Size)];

    #[inline]
    fn deref(&self) -> &[(Size, Size)] {
        unsafe { slice::from_raw_parts(self.ptr(), (self.len() / INDEX_ENTRY_SIZE) as usize) }
    }
}

/// find the index of the offset that matches
pub(crate) fn lookup_entry(offsets: &[(Size, Size)], offset: Size) -> Option<usize> {
    let first_entry = offsets[0];
    if offset < first_entry.offset() {
        trace!(
            "offset: {} is less than: first: {}",
            offset,
            first_entry.offset()
        );
        return None;
    }

    match offsets.binary_search_by(|entry| entry.offset().cmp(&offset)) {
        Ok(idx) => Some(idx),
        Err(idx) => Some(idx - 1),
    }
}

#[cfg(test)]
mod tests {

    use std::env::temp_dir;

    use flv_util::fixture::ensure_clean_file;

    use super::lookup_entry;
    use super::LogIndex;
    use crate::mut_index::MutLogIndex;
    use crate::config::ReplicaConfig;
    use super::OffsetPosition;

    #[allow(unused)]
    const TEST_FILE: &str = "00000000000000000921.index";

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

    #[allow(unused)]
    fn default_option() -> ReplicaConfig {
        ReplicaConfig {
            segment_max_bytes: 1000,
            base_dir: temp_dir(),
            index_max_bytes: 1000,
            index_max_interval_bytes: 0,
            ..Default::default()
        }
    }

    #[allow(unused)]
    //#[fluvio_future::test]
    async fn test_index_read_offset() {
        let option = default_option().shared();
        let test_file = option.base_dir.join(TEST_FILE);
        ensure_clean_file(&test_file);

        let mut mut_index = MutLogIndex::create(921, option.clone())
            .await
            .expect("create");

        mut_index.write_index((5, 16, 70)).await.expect("send");
        mut_index.write_index((10, 100, 70)).await.expect("send");

        mut_index.shrink().await.expect("shrink");

        let log_index = LogIndex::open_from_offset(921, option).await.expect("open");
        let offset1 = log_index[0];
        assert_eq!(offset1.offset(), 5);
        assert_eq!(offset1.position(), 16);

        let offset2 = log_index[1];
        assert_eq!(offset2.offset(), 10);
        assert_eq!(offset2.position(), 100);
    }

    /*  this is compound test which is not needed.
    const TEST_FILE3: &str = "00000000000000000922.index";

    #[fluvio_future::test]
    async fn test_index_read_findoffset()  {
        let option = default_option();
        let test_file = option.base_dir.join(TEST_FILE3);
        ensure_clean_file(&test_file);

        let mut mut_index = MutLogIndex::create(922, &option).await?;

        mut_index.send((100, 16, 70)).await?;
        mut_index.send((500, 200, 70)).await?;
        mut_index.send((800, 100, 70)).await?;
        mut_index.send((1000, 200, 70)).await?;

        mut_index.shrink().await?;

        let log_index = LogIndex::open_from_offset(922, &option).await?;
        assert_eq!(log_index.find_offset(600), Ok(1));
        assert_eq!(log_index.find_offset(2000), Ok(3));
    }
    */
}
