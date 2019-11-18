use std::io::Error as IoError;
use std::io::ErrorKind;
use std::mem::size_of;
use std::mem::transmute;
use std::ops::Deref;
use std::ops::DerefMut;
use std::slice;
use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::task::Waker;
use std::sync::Arc;
use std::sync::RwLock;

use futures::sink::Sink;
use libc::c_void;
use log::debug;
use log::trace;
use log::error;
use pin_utils::pin_mut;
use pin_utils::unsafe_unpinned;


use future_aio::fs::File;
use future_helper::spawn_blocking;
use future_aio::fs::MemoryMappedMutFile;
use kf_protocol::api::Offset;
use kf_protocol::api::Size;

use crate::util::generate_file_name;
use crate::ConfigOption;
use crate::index::lookup_entry;
use crate::index::Index;
use crate::index::OffsetPosition;


/// size of the memory mapped isze
const INDEX_ENTRY_SIZE: Size = (size_of::<Size>() * 2) as Size;

pub const EXTENSION: &str = "index";


enum LogIndexState {
    Ready,
    Dirty,
    Flushing(Waker)
}


/// Segment index
///
/// Maps offset into file position (Seek)
///
/// It is backed by memory mapped file
///
/// For active segment, index can grow
/// For non active, it is fixed
/// we maintain state in the lock to handle waker

// implement index file
pub struct MutLogIndex {
    mmap: MemoryMappedMutFile,
    state: Arc<RwLock<LogIndexState>>,
    file: File,
    base_offset: Offset,
    bytes_delta: Size,
    pos: Size,
    option: ConfigOption,
    ptr: *mut c_void,
}

// const MEM_SIZE: u64 = 1024 * 1024 * 10; //10 MBs

unsafe impl Sync for MutLogIndex {}
unsafe impl Send for MutLogIndex {}

impl MutLogIndex {
    unsafe_unpinned!(mmap: MemoryMappedMutFile);
    unsafe_unpinned!(pos: Size);
    unsafe_unpinned!(bytes_delta: Size);
    unsafe_unpinned!(state: Arc<RwLock<LogIndexState>>);

    pub async fn create(base_offset: Offset, option: &ConfigOption) -> Result<Self, IoError> {
        let index_file_path = generate_file_name(&option.base_dir, base_offset, EXTENSION);

        if option.index_max_bytes == 0 {
            return Err(IoError::new(ErrorKind::InvalidInput, "index max bytes must be greater than 0"));
        }

        debug!("creating index mm at: {:#?}", index_file_path);
        let ( m_file, file) = MemoryMappedMutFile::create(
            &index_file_path,
            option.index_max_bytes as u64
        ).await?;

        let ptr = {
            let b_slices: &[u8] = &m_file.mut_inner();
            unsafe { transmute::<*const u8, *mut c_void>(b_slices.as_ptr()) }
        };

        Ok(MutLogIndex {
            mmap: m_file,
            state: Arc::new(RwLock::new(LogIndexState::Ready)),
            file,
            pos: 0,
            bytes_delta: 0,
            option: option.to_owned(),
            ptr,
            base_offset
        })
    }

    pub async fn open(base_offset: Offset, option: &ConfigOption) -> Result<Self, IoError> {
        let index_file_path = generate_file_name(&option.base_dir, base_offset, EXTENSION);

        // create new memory file and
        if option.index_max_bytes == 0 {
            return Err(IoError::new(ErrorKind::InvalidInput, "invalid API"));
        }

       
        // make sure it is log file
        let (m_file, file) = MemoryMappedMutFile::create(
            &index_file_path,
            option.index_max_bytes as u64
        ).await?;

        
        let ptr = {
            let b_slices: &[u8] = &m_file.mut_inner();
            unsafe { transmute::<*const u8, *mut c_void>(b_slices.as_ptr()) }
        };

        trace!("opening mut index at: {:#?}, pos: {}", index_file_path,0);
      
        let mut index = MutLogIndex {
            mmap: m_file,
            state: Arc::new(RwLock::new(LogIndexState::Ready)),
            file,
            pos: 0,
            bytes_delta: 0,
            option: option.to_owned(),
            ptr,
            base_offset
        };

        index.update_pos()?;

        Ok(index)
    }

    // shrink index file to last know position

    pub async fn shrink(&mut self) -> Result<(), IoError> {
        let len = (self.pos * INDEX_ENTRY_SIZE) as u64;
        debug!("shrinking index: {:#?} to {} bytes", self.file, len);
        let file = &mut self.file;
        pin_mut!(file);
        file.set_len(len).await
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

    /// recalculate the 
    fn update_pos(&mut self) -> Result<(),IoError> {

        let entries = self.entries();
        trace!("updating position with: {}",entries);

        for i in 0..entries {
            if self[i as usize].position() == 0 {
                trace!("set positioning: {}",i);
                self.pos = i;
                return Ok(())
            }
        }

        Err(IoError::new(ErrorKind::InvalidData, "empty slot was not found"))
    }
}

impl Index for MutLogIndex {
    
     /// find offset indexes using relative offset
    fn find_offset(&self, relative_offset: Size) -> Option<(Size,Size)> {
        trace!("try to find relative offset: {}  index: {}", relative_offset,self.pos);

        if self.pos == 0 {
            trace!("no entries, returning none");
            return None
        }
        let (lower, _) = self.split_at(self.pos as usize);
        lookup_entry(lower, relative_offset).map( |idx| self[idx])
    }

    fn len(&self) -> Size {
        self.option.index_max_bytes
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

/// Sink with item (offset, position, batch size)
impl Sink<(Size,Size,Size)> for MutLogIndex {
   
    type Error = IoError;

    fn poll_ready(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        match *self.state.read().unwrap() {
            LogIndexState::Ready => {
                trace!("poll ready: write state is ready => ready");
                Poll::Ready(Ok(()))
            },
            _ => {
                trace!("poll ready: write state is flushing => pending");
                Poll::Pending
            }
        }
    }

    fn start_send(mut self: Pin<&mut Self>, item: (Size,Size,Size)) -> Result<(), Self::Error> {
      
        
        trace!("index start: {:#?}",item);

        let mut write_state = self.as_mut().state().write().unwrap();
        *write_state = LogIndexState::Dirty;
        drop(write_state);
        trace!("set mode to dirty");  

        let batch_size = item.2;

        let bytes_delta = self.bytes_delta;
        if bytes_delta < self.option.index_max_interval_bytes {
            trace!("index writing skipped accumulated bytes {} less than max less interval: {}",bytes_delta,self.option.index_max_interval_bytes);
            *self.as_mut().bytes_delta() = bytes_delta + batch_size;
            trace!("index updated accumulated bytes: {}",self.bytes_delta);
            return Ok(());
        }

        let pos = self.pos as usize;
        let max_entries = self.entries();
        *self.as_mut().pos() = (pos + 1) as Size;
        *self.as_mut().bytes_delta() = 0;
        let this = unsafe { Pin::get_unchecked_mut(self) };

        if pos < max_entries as usize {
            this[pos] = (item.0,item.1).to_be();
            trace!("index successfully written: {:#?} at: {}", item,pos);
        } else {
            error!("index position: {} is greater than max entries: {}, ignoring",pos,max_entries);
        }

              
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, ctx: &mut Context) -> Poll<Result<(), Self::Error>> {
        
        trace!("poll flush");

        // return early if state is ready or pending if still flushing
        match *self.state.read().unwrap() {
            LogIndexState::Ready => {
                trace!("flushing finished. returning ok");
                return Poll::Ready(Ok(()));
            }
            LogIndexState::Flushing(_) => {
                trace!("flushing is still pending, returning pending");
                return Poll::Pending;
            },
            _ => {}
        }

        // state is dirty. need to schedule flush and store state with waker
        let mut write_state = self.as_mut().state().write().unwrap();
        *write_state = LogIndexState::Flushing(ctx.waker().clone());
        drop(write_state);
        trace!("state is dirty, scheduling mmap flush");

        // clone state an mmap so it can be passed to spawn blocking
        let state_clone = self.as_ref().state.clone();
        let mmap = self.as_mut().mmap().inner_map();
        
        spawn_blocking(move || {
            let lock = mmap.write().unwrap();
            let _ = lock.flush();
            drop(lock);
            trace!("done flushing");
            let mut state_lock = state_clone.write().unwrap();
            let prev_state = std::mem::replace(&mut *state_lock, LogIndexState::Ready);
            drop(state_lock);
            match prev_state {
                LogIndexState::Flushing(waker) => {
                    trace!("waking up");
                    waker.wake();
                },
                _ => {
                    trace!("invalid state waking up");
                }
            }
            trace!("update state back to ready");
        });
        
        Poll::Pending
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}


#[cfg(test)]
mod tests {

    use futures::sink::SinkExt;
    use std::fs::File;
    use std::io::Error as IoError;
    use std::io::Read;
   
    use future_helper::test_async;

    use super::MutLogIndex;
    use crate::index::Index;
    use crate::fixture::default_option;
    use crate::fixture::ensure_clean_file;
    use crate::index::OffsetPosition;

    const TEST_FILE: &str = "00000000000000000121.index";

   

    #[test_async]
    async fn test_index_write() -> Result<(), IoError> {
        let option = default_option(50);
        let test_file = option.base_dir.join(TEST_FILE);
        ensure_clean_file(&test_file);

        let mut index_sink = MutLogIndex::create(121, &option).await?;


        index_sink.send((5, 200,70)).await?;      // this will be ignored
        index_sink.send((10, 100,70)).await?;     // this will be written since batch size 70 is greater than 50

        assert_eq!(index_sink.pos,1);

        let mut f = File::open(&test_file)?;
        let mut buffer = vec![0; 32];
        f.read(&mut buffer)?;

        // ensure offset,position are stored in the big endian format
        assert_eq!(buffer[0], 0);
        assert_eq!(buffer[1], 0);
        assert_eq!(buffer[2], 0);
        assert_eq!(buffer[3], 10);
        assert_eq!(buffer[4], 0);
        assert_eq!(buffer[5], 0);
        assert_eq!(buffer[6], 0);
        assert_eq!(buffer[7], 100);

        drop(index_sink);

        // open same file

        let index_sink = MutLogIndex::open(121, &option).await?;
        assert_eq!(index_sink.pos,1);

        Ok(())
    }

    const TEST_FILE2: &str = "00000000000000000122.index";

    #[test_async]
    async fn test_index_shrink() -> Result<(), IoError> {
        let option = default_option(0);
        let test_file = option.base_dir.join(TEST_FILE2);
        ensure_clean_file(&test_file);

        let mut index_sink = MutLogIndex::create(122, &option).await?;

        index_sink.send((5, 16,70)).await?;

        index_sink.shrink().await?;

        let f = File::open(&test_file)?;
        let m = f.metadata()?;
        assert_eq!(m.len(), 8);

        Ok(())
    }


    const TEST_FILE3: &str = "00000000000000000123.index";

    #[test_async]
    async fn test_mut_index_findoffset() -> Result<(), IoError> {
        let option = default_option(0);
        let test_file = option.base_dir.join(TEST_FILE3);
        ensure_clean_file(&test_file);

        let mut index_sink = MutLogIndex::create(123, &option).await?;

        index_sink.send((100, 16,70)).await?;
        index_sink.send((500, 200,70)).await?;
        index_sink.send((800, 100,70)).await?;
        index_sink.send((1000, 200,70)).await?;

        assert_eq!(index_sink.find_offset(600).map(|p| p.to_be()), Some((500,200)));
        assert_eq!(index_sink.find_offset(2000).map(|p| p.to_be()), Some((1000,200)));
        Ok(())
    }

}
