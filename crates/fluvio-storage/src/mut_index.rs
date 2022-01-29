use std::io::Error as IoError;
use std::io::ErrorKind;
use std::mem::size_of;
use std::ops::Deref;
use std::ops::DerefMut;
use std::slice;
use std::sync::Arc;

use libc::c_void;
use tracing::debug;
use tracing::instrument;
use tracing::trace;
use tracing::error;

use fluvio_future::fs::File;
use fluvio_future::fs::mmap::MemoryMappedMutFile;
use dataplane::{Offset, Size};

use crate::config::SharedReplicaConfig;
use crate::util::generate_file_name;
use crate::index::lookup_entry;
use crate::index::Index;
use crate::index::OffsetPosition;

/// size of each memory mapped entry
const INDEX_ENTRY_SIZE: Size = (size_of::<Size>() * 2) as Size;

pub const EXTENSION: &str = "index";

/// Index file for offset
/// Each entry in index consist of pair of (relative_offset, file_position)

// implement index file
pub struct MutLogIndex {
    mmap: MemoryMappedMutFile,
    file: File,
    base_offset: Offset,         // base offset of segment
    accumulated_batch_len: Size, // accumulated batches len
    last_offset_delta: Size,
    slot_index: Size, // track of the index slot
    option: Arc<SharedReplicaConfig>,
    ptr: *mut c_void,
}

// const MEM_SIZE: u64 = 1024 * 1024 * 10; //10 MBs

unsafe impl Sync for MutLogIndex {}
unsafe impl Send for MutLogIndex {}

impl MutLogIndex {
    pub async fn create(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<Self, IoError> {
        let index_file_path = generate_file_name(&option.base_dir, base_offset, EXTENSION);

        if option.index_max_bytes.get() == 0 {
            return Err(IoError::new(
                ErrorKind::InvalidInput,
                "index max bytes must be greater than 0",
            ));
        }

        debug!(
            ?index_file_path,
            index_max_bytes = option.index_max_bytes.get(),
            index_max_interval_bytes = option.index_max_interval_bytes.get(),
            "creating index file"
        );

        let (m_file, file) =
            MemoryMappedMutFile::create(&index_file_path, option.index_max_bytes.get() as u64)
                .await?;

        let ptr = {
            let b_slices: &[u8] = &m_file.mut_inner();
            b_slices.as_ptr() as *mut libc::c_void
        };

        Ok(MutLogIndex {
            mmap: m_file,
            file,
            slot_index: 0,
            accumulated_batch_len: 0,
            last_offset_delta: 0,
            option,
            ptr,
            base_offset,
        })
    }

    pub async fn open(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<Self, IoError> {
        let index_file_path = generate_file_name(&option.base_dir, base_offset, EXTENSION);

        debug!(path = ?index_file_path, "opening mut index");
        // create new memory file and
        if option.index_max_bytes.get() == 0 {
            return Err(IoError::new(ErrorKind::InvalidInput, "invalid API"));
        }

        // make sure it is log file
        let (m_file, file) =
            MemoryMappedMutFile::create(&index_file_path, option.index_max_bytes.get() as u64)
                .await?;

        let ptr = {
            let b_slices: &[u8] = &m_file.mut_inner();
            b_slices.as_ptr() as *mut libc::c_void
        };

        let mut index = MutLogIndex {
            mmap: m_file,
            file,
            slot_index: 0,
            accumulated_batch_len: 0,
            last_offset_delta: 0,
            option,
            ptr,
            base_offset,
        };

        index.slot_index = index.find_next_index()?;
        debug!(index.slot_index, "next index slot");

        Ok(index)
    }

    // shrink index file to last know position

    pub async fn shrink(&mut self) -> Result<(), IoError> {
        let target_len = (self.slot_index * INDEX_ENTRY_SIZE) as u64;
        debug!(
            target_len,
            base_offset = self.base_offset,
            "shrinking index"
        );
        self.file.set_len(target_len).await
    }

    #[inline]
    pub fn ptr(&self) -> *const (Size, Size) {
        self.ptr as *const (Size, Size)
    }

    pub fn mut_ptr(&mut self) -> *mut (Size, Size) {
        self.ptr as *mut (Size, Size)
    }

    #[allow(dead_code)]
    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    /// find next index to write by finding empty
    fn find_next_index(&self) -> Result<u32, IoError> {
        let entries = self.entries();
        trace!("updating position with: {}", entries);

        for i in 0..entries {
            if self[i as usize].position() == 0 {
                debug!(i, "found empty slot");
                return Ok(i as u32);
            }
        }

        Err(IoError::new(
            ErrorKind::InvalidData,
            "empty slot was not found",
        ))
    }

    /// write index entry
    /// offset_delta is relative offset in the segment, this should be always increase
    #[instrument(skip(self))]
    pub async fn write_index(
        &mut self,
        offset_delta: Size,
        file_position: Size,
        batch_size: Size,
    ) -> Result<(), IoError> {
        let accumulated_len = self.accumulated_batch_len;
        let max_interval = self.option.index_max_interval_bytes.get();

        // check that offset delta should be incremental
        if offset_delta > 0 {
            assert!(offset_delta > self.last_offset_delta);
        } else {
            assert_eq!(self.last_offset_delta, 0);
        }

        self.last_offset_delta = offset_delta;

        // only write to index if accmulated batch size is greater than max interval
        if accumulated_len < max_interval {
            self.accumulated_batch_len = accumulated_len + batch_size;
            trace!(
                bytes_delta = self.accumulated_batch_len,
                max_interval = max_interval,
                "no write due to less than max interval"
            );
            return Ok(());
        }

        // write new index entry

        let max_entries = self.entries();

        if self.slot_index < max_entries {
            let slot_index = self.slot_index as usize;
            debug!(slot_index, offset_delta, file_position, "add new entry at");
            self[slot_index] = (offset_delta.to_be(), file_position.to_be());
            self.mmap.flush_ft().await?;
            self.accumulated_batch_len = 0;
            self.slot_index += 1;
        } else {
            error!(
                "index position: {} is greater than max entries: {}, ignoring",
                self.slot_index, max_entries
            );
        }

        Ok(())
    }
}

impl Index for MutLogIndex {
    /// find offset indexes using relative offset
    /// returns (relative_offset, file_position)
    #[instrument(level = "trace",skip(self),fields(slot=self.slot_index))]
    fn find_offset(&self, relative_offset: Size) -> Option<(Size, Size)> {
        if self.slot_index == 0 {
            trace!("no entries, returning none");
            return None;
        }
        let (lower, _) = self.split_at(self.slot_index as usize);
        if let Some(index) = lookup_entry(lower, relative_offset) {
            trace!(index, "found index slot");
            Some(self[index].to_be())
        } else {
            trace!("no index slot found");
            None
        }
    }

    fn len(&self) -> Size {
        self.option.index_max_bytes.get()
    }
}

impl Deref for MutLogIndex {
    type Target = [(Size, Size)];

    #[inline]
    fn deref(&self) -> &[(Size, Size)] {
        unsafe { slice::from_raw_parts(self.ptr(), (self.len() / INDEX_ENTRY_SIZE) as usize) }
    }
}

impl DerefMut for MutLogIndex {
    #[inline]
    fn deref_mut(&mut self) -> &mut [(Size, Size)] {
        unsafe {
            slice::from_raw_parts_mut(self.mut_ptr(), (self.len() / INDEX_ENTRY_SIZE) as usize)
        }
    }
}

#[cfg(test)]
mod tests {

    use std::fs::File;
    use std::io::Read;

    use dataplane::Offset;
    use flv_util::fixture::ensure_clean_file;

    use super::MutLogIndex;
    use crate::LogIndex;
    use crate::index::Index;
    use crate::fixture::default_option;

    const TEST_FILE: &str = "00000000000000000000.index";

    #[fluvio_future::test]
    async fn test_index_simple_write() {
        const BASE_OFFSET: Offset = 0;

        let option = default_option(200).shared();
        assert_eq!(option.index_max_interval_bytes.get(), 200);
        let test_file = option.base_dir.join(TEST_FILE);
        ensure_clean_file(&test_file);

        let mut index = MutLogIndex::create(BASE_OFFSET, option.clone())
            .await
            .expect("crate");

        assert_eq!(index.slot_index, 0);
        assert_eq!(index.base_offset, BASE_OFFSET);

        index.write_index(0, 0, 50).await.expect("send"); // this will be ignored, since pending size is less than max interval

        assert_eq!(index.slot_index, 0);
        assert_eq!(index.accumulated_batch_len, 50);
        assert_eq!(index.last_offset_delta, 0);

        index.write_index(10, 50, 100).await.expect("send"); // this should be also ignored.

        assert_eq!(index.slot_index, 0);
        assert_eq!(index.accumulated_batch_len, 150);
        assert_eq!(index.last_offset_delta, 10);

        index.write_index(15, 150, 100).await.expect("send"); // still ignored but next one will write.

        assert_eq!(index.slot_index, 0);
        assert_eq!(index.accumulated_batch_len, 250);
        assert_eq!(index.last_offset_delta, 15);

        index.write_index(20, 250, 100).await.expect("send"); // trigger write

        assert_eq!(index.slot_index, 1);
        assert_eq!(index.accumulated_batch_len, 0);
        assert_eq!(index.last_offset_delta, 20);

        index.write_index(30, 300, 100).await.expect("send"); // no trigger

        assert_eq!(index.slot_index, 1);
        assert_eq!(index.accumulated_batch_len, 100);
        assert_eq!(index.last_offset_delta, 30);

        let mut f = File::open(&test_file).expect("open");
        let mut buffer = vec![0; 32];
        f.read_exact(&mut buffer).expect("read");

        // ensure offset,position are stored in the big endian format
        assert_eq!(buffer[0], 0);
        assert_eq!(buffer[1], 0);
        assert_eq!(buffer[2], 0);
        assert_eq!(buffer[3], 20); // offset_delta,
        assert_eq!(buffer[4], 0);
        assert_eq!(buffer[5], 0);
        assert_eq!(buffer[6], 0);
        assert_eq!(buffer[7], 250); // file position

        assert_eq!(index.find_offset(16), None);

        assert_eq!(index.find_offset(20), Some((20, 250)));
        assert_eq!(index.find_offset(40), Some((20, 250)));

        // write more
        index.write_index(40, 400, 100).await.expect("send"); // no trigger
        assert_eq!(index.slot_index, 1);
        assert_eq!(index.accumulated_batch_len, 200);
        assert_eq!(index.last_offset_delta, 40);

        index.write_index(60, 500, 300).await.expect("send"); // trigger write
        assert_eq!(index.slot_index, 2);
        assert_eq!(index.accumulated_batch_len, 0);
        assert_eq!(index.last_offset_delta, 60);

        assert_eq!(index.find_offset(50), Some((20, 250)));
        assert_eq!(index.find_offset(70), Some((60, 500)));

        drop(index);

        // open same file and check index

        let index_sink = MutLogIndex::open(121, option).await.expect("open");
        assert_eq!(index_sink.slot_index, 1);
    }

    const TEST_FILE2: &str = "00000000000000000122.index";

    #[fluvio_future::test]
    async fn test_index_shrink() {
        let option = default_option(0).shared();
        let test_file = option.base_dir.join(TEST_FILE2);
        ensure_clean_file(&test_file);

        let mut index_sink = MutLogIndex::create(122, option).await.expect("create");

        index_sink.write_index(5, 16, 70).await.expect("send");

        index_sink.shrink().await.expect("shrink");

        let f = File::open(&test_file).expect("open");
        let m = f.metadata().expect("meta");
        assert_eq!(m.len(), 8);
    }

    const TEST_FILE3: &str = "00000000000000000123.index";

    #[fluvio_future::test]
    async fn test_mut_index_findoffset() {
        let option = default_option(0).shared();
        let test_file = option.base_dir.join(TEST_FILE3);
        ensure_clean_file(&test_file);

        let mut mut_index = MutLogIndex::create(123, option.clone())
            .await
            .expect("create");

        mut_index.write_index(100, 16, 70).await.expect("send");
        mut_index.write_index(500, 200, 70).await.expect("send");
        mut_index.write_index(800, 100, 70).await.expect("send");
        mut_index.write_index(1000, 200, 70).await.expect("send");

        mut_index.shrink().await.expect("shrink");
        drop(mut_index);

        let index = LogIndex::open_from_offset(123, option).await.expect("open");

        assert_eq!(index.find_offset(600), Some((500, 200)));
        assert_eq!(index.find_offset(2000), Some((1000, 200)));
    }
}
