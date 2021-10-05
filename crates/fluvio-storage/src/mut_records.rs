use std::io::Error as IoError;
use std::path::PathBuf;
use std::path::Path;
use std::fmt;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use async_lock::Mutex;

use tracing::{debug, warn, trace};

use futures_lite::io::AsyncWriteExt;
use async_channel::Sender;

use fluvio_future::timer;
// use fluvio_future::fs::File;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::BoundedFileSink;
use fluvio_future::fs::BoundedFileOption;
use fluvio_future::fs::BoundedFileSinkError;
use dataplane::batch::Batch;
use dataplane::{Offset, Size};
use dataplane::core::Encoder;

use crate::util::generate_file_name;
use crate::validator::validate;
use crate::validator::LogValidationError;
use crate::config::ConfigOption;
use crate::StorageError;
use crate::records::FileRecords;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

// Delay flush scheduling interrival time constant
//   not something that a user should change
// this is a delay put in because the scheduler switch
// to the flush task is fast enough that it might beat
// out waiting writes which should be preferred
const DELAY_FLUSH_SIA_MSEC: u64 = 3;

/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    item_last_offset_delta: Size,
    f_sink: Arc<Mutex<BoundedFileSink>>,
    f_slice_root: AsyncFileSlice,
    cached_len: u64,
    flush_policy: FlushPolicy,
    write_count: u64,
    flush_count: Arc<AtomicU32>,
    path: PathBuf,
    flush_time_tx: Option<Sender<Instant>>,
}

impl fmt::Debug for MutFileRecords {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Log({})", self.base_offset)
    }
}

impl Unpin for MutFileRecords {}

fn get_flush_policy_from_config(option: &ConfigOption) -> FlushPolicy {
    if option.flush_idle_msec > 0 {
        FlushPolicy::IdleFlush {
            delay_millis: option.flush_idle_msec,
        }
    } else if option.flush_write_count == 0 {
        FlushPolicy::NoFlush
    } else if option.flush_write_count == 1 {
        FlushPolicy::EveryWrite
    } else {
        FlushPolicy::CountWrites {
            n_writes: option.flush_write_count,
            write_tracking: 0,
        }
    }
}

impl MutFileRecords {
    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, BoundedFileSinkError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        let sink_option = BoundedFileOption {
            max_len: Some(option.segment_max_bytes as u64),
        };
        debug!(log_path = ?log_path, max_len = ?sink_option.max_len,"creating log at");

        let f_sink = BoundedFileSink::open_append(&log_path, sink_option).await?;
        debug!("file created");
        let f_slice_root = f_sink.slice_from(0, 0)?;
        Ok(MutFileRecords {
            base_offset,
            f_sink: Arc::new(Mutex::new(f_sink)),
            f_slice_root,
            cached_len: 0,
            flush_policy: get_flush_policy_from_config(option),
            write_count: 0,
            flush_count: Arc::new(AtomicU32::new(0)),
            item_last_offset_delta: 0,
            path: log_path.to_owned(),
            flush_time_tx: None,
        })
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    pub async fn validate(&mut self) -> Result<Offset, LogValidationError> {
        let f_sink = self.f_sink.lock().await;
        validate(f_sink.get_path()).await
    }

    pub fn get_pos(&self) -> Size {
        if self.cached_len > u32::MAX.into() {
            warn!(
                "mutRecord position truncation {} -> {}",
                self.cached_len, self.cached_len as Size
            );
        }
        self.cached_len as Size
    }

    pub fn get_item_last_offset_delta(&self) -> Size {
        self.item_last_offset_delta
    }

    /// try to write batch
    /// if there is enough room, return true, false otherwise
    pub async fn write_batch(&mut self, item: &Batch) -> Result<bool, StorageError> {
        trace!("start sending using batch {:#?}", item.get_header());
        if item.base_offset < self.base_offset {
            return Err(StorageError::LogValidation(LogValidationError::BaseOff));
        }

        self.item_last_offset_delta = item.get_last_offset_delta();
        let mut buffer: Vec<u8> = vec![];
        item.encode(&mut buffer, 0)?;
        let mf_sink = self.f_sink.clone();
        let mut f_sink = mf_sink.lock().await;

        if f_sink.can_be_appended(buffer.len() as u64) {
            debug!(buffer_len = buffer.len(), "writing bytes");
            f_sink.write_all(&buffer).await?;
            self.cached_len = f_sink.get_current_len();
            drop(f_sink); // unlock because flush may reaqire the lock
            self.write_count = self.write_count.saturating_add(1);
            match self.flush_policy.should_flush() {
                FlushAction::NoFlush => {}

                FlushAction::Now => {
                    self.flush().await?;
                    debug!(
                        flush_count = self.flush_count(),
                        write_count = self.write_count,
                        "Flushing Now"
                    );
                }

                FlushAction::Delay(delay_millis) => {
                    debug!("  delay flush start {:?}", self.flush_policy);
                    self.delay_flush(delay_millis).await?;
                }
            }

            Ok(true)
        } else {
            debug!(
                len = f_sink.get_current_len(),
                buffer_len = buffer.len(),
                "no more room to add"
            );
            Ok(false)
        }
    }

    #[allow(unused)]
    pub async fn flush(&mut self) -> Result<(), IoError> {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        let mut f_sink = self.f_sink.lock().await;
        debug!("flush: count {:?}", self.flush_count);
        f_sink.flush().await
    }

    pub fn flush_count(&self) -> u32 {
        self.flush_count.load(Ordering::Relaxed)
    }

    async fn delay_flush(&mut self, delay_millis: u32) -> Result<(), IoError> {
        let delay_tgt = delay_millis as u64;
        let mf_sink = self.f_sink.clone();

        if self.flush_time_tx.is_none() {
            // no task running so start one
            let (tx, rx) = async_channel::bounded(100);
            self.flush_time_tx = Some(tx);
            let delay_tgt = Duration::from_millis(delay_tgt);
            let flush_count = self.flush_count.clone();

            fluvio_future::task::spawn(async move {
                let mut delay_dur = delay_tgt;
                let mut write_time = Instant::now();

                // when the mut_record struct is dropped self.flush_time_tx
                // will also be closed and dropped, causing this while loop
                // to end then exit the task
                while !rx.is_closed() {
                    if let Ok(wt) = rx.recv().await {
                        // always grab inital write time, but also
                        // guarantee a wait on a write time for flush
                        // for wait times after the first delay runs
                        write_time = wt;
                    }
                    //  A short fixed wait was added here to give a little
                    //  breathing space to accumulate clusters of writes which
                    //  arrive closely but not fast enough for this task loop
                    //  between the first and following check for write times
                    //  It prevents extra flushes and the time is still accounted for
                    //  in the delay_dur calculation.
                    timer::after(Duration::from_millis(DELAY_FLUSH_SIA_MSEC)).await;

                    // Clear out any accumulated write times but don't wait
                    // Update to latest accumulated write time

                    // could use a mutex for last write time instead?
                    // could bundle write time with f_sink inside a stuct
                    // held by the existing mutex, but that's more invasive
                    while let Ok(wt) = rx.try_recv() {
                        write_time = wt;
                        debug!("update write time for delayed flush {:?}", write_time);
                    }
                    if write_time.elapsed() < delay_tgt {
                        // calculate remaining delay time
                        delay_dur = delay_tgt - write_time.elapsed();
                    }
                    debug!(
                        "flush delay wait start delay:tgt {:?}:{:?}",
                        delay_dur, delay_tgt
                    );
                    timer::after(delay_dur).await;

                    debug!("delay flush: get lock");
                    let mut f_sink = mf_sink.lock().await;
                    if let Err(e) = f_sink.flush().await {
                        warn!("flush error {}", e);
                    } else {
                        let fc = flush_count.fetch_add(1, Ordering::Relaxed);
                        debug!(" - flushed: delay task flush cnt: {}", fc);
                    }
                }
                debug!("delay_flush task exited");
            });
        }

        // update running flush
        if let Some(flush_tx) = &self.flush_time_tx {
            if let Err(serr) = flush_tx.send(Instant::now()).await {
                use std::io::{Error, ErrorKind};
                warn!("Flush send error {}", serr);
                return Err(Error::new(ErrorKind::Other, "flush send error"));
            }
        }
        Ok(())
    }
}

impl FileRecords for MutFileRecords {
    fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    // the get_file method was never called, leaving it in makes
    // the ownership management of the f_sink impossible to place in a mutex
    // to manage access from send() writers and a async timed delay flush
    // not sure how to resolve this

    // fn get_file(&self) -> &File {
    //
    //     // &self.f_sink.inner()
    //
    //     block_on(async {
    //         let am = self.f_sink.clone();
    //         let f_sink = am.lock().await;
    //         &f_sink.inner().clone()
    //     })
    // }

    fn get_path(&self) -> &Path {
        &self.path
    }

    fn as_file_slice(&self, start: Size) -> Result<AsyncFileSlice, IoError> {
        let reslice = AsyncFileSlice::new(
            self.f_slice_root.fd(),
            start as u64,
            self.cached_len - start as u64,
        );
        Ok(reslice)
    }

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError> {
        let reslice = AsyncFileSlice::new(self.f_slice_root.fd(), start as u64, len as u64);
        Ok(reslice)
    }
}

/// MutFileRecordsFlushPolicy describes and implements a flush policy
#[derive(Debug)]
pub enum FlushPolicy {
    NoFlush,
    EveryWrite,
    CountWrites { n_writes: u32, write_tracking: u32 },
    IdleFlush { delay_millis: u32 },
}

enum FlushAction {
    NoFlush,
    Now,
    Delay(u32),
}

impl FlushPolicy {
    /// Evaluates the flush policy and returns a flush action
    // to take
    fn should_flush(&mut self) -> FlushAction {
        use FlushPolicy::{NoFlush, EveryWrite, CountWrites, IdleFlush};
        match self {
            NoFlush => FlushAction::NoFlush,

            EveryWrite => FlushAction::Now,

            CountWrites {
                n_writes: n_max,
                write_tracking: wcount,
            } => {
                *wcount += 1;
                if *wcount >= *n_max {
                    *wcount = 0;
                    return FlushAction::Now;
                }
                FlushAction::NoFlush
            }

            IdleFlush { delay_millis } => FlushAction::Delay(*delay_millis),
        }
    }
}

#[cfg(test)]
mod tests {

    // use std::time::{Duration, Instant};
    use std::env::temp_dir;
    use std::io::Cursor;
    use tracing::debug;

    use flv_util::fixture::ensure_clean_file;
    use dataplane::batch::{Batch, MemoryRecords};
    use dataplane::core::{Decoder, Encoder};
    use dataplane::fixture::create_batch;
    use dataplane::fixture::read_bytes_from_file;

    use crate::config::ConfigOption;
    use super::MutFileRecords;

    const TEST_FILE_NAME: &str = "00000000000000000100.log"; // for offset 100
    const TEST_FILE_NAMEC: &str = "00000000000000000200.log"; // for offset 200

    #[allow(clippy::unnecessary_mut_passed)]
    #[fluvio_future::test]
    async fn test_write_records_every() {
        debug!("test_write_records_every");

        let test_file = temp_dir().join(TEST_FILE_NAME);
        ensure_clean_file(&test_file);

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(100, &options).await.expect("create");

        debug!("{:?}", msg_sink.flush_policy);

        let batch = create_batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("create");

        debug!("read start");
        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");
        debug!("read ok");
        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 2);
        let mut records = batch.own_records();
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.as_ref(), vec![10, 20]);
        let record2 = records.remove(0);
        assert_eq!(record2.value.as_ref(), vec![10, 20]);

        debug!("write 2");
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("write");

        let bytes = read_bytes_from_file(&test_file).expect("read");
        assert_eq!(bytes.len(), write_size * 2, "should be 158 bytes");

        let old_msg_sink = MutFileRecords::create(100, &options).await.expect("open");
        assert_eq!(old_msg_sink.get_base_offset(), 100);
    }

    // This Test configures policy to flush after every NUM_WRITES
    // and checks to see when the flush occurs relative to the write count
    #[allow(clippy::unnecessary_mut_passed)]
    #[fluvio_future::test]
    async fn test_write_records_count() {
        let test_file = temp_dir().join(TEST_FILE_NAMEC);
        ensure_clean_file(&test_file);

        const NUM_WRITES: u32 = 4;
        const OFFSET: i64 = 200;

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            flush_write_count: NUM_WRITES,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(OFFSET, &options)
            .await
            .expect("create");

        let batch = create_batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("create");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created

        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");

        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 2);
        let mut records = batch.own_records();
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.as_ref(), vec![10, 20]);
        let record2 = records.remove(0);
        assert_eq!(record2.value.as_ref(), vec![10, 20]);

        // check flush counts don't increment yet
        let flush_count = msg_sink.flush_count();
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        for _ in 1..(NUM_WRITES - 2) {
            msg_sink
                .write_batch(&mut create_batch())
                .await
                .expect("send");
            assert_eq!(flush_count, msg_sink.flush_count());
        }

        // flush count should increment after final write
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * NUM_WRITES as usize;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        let old_msg_sink = MutFileRecords::create(OFFSET, &options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);
    }

    // This test configures policy to flush after some write idle time
    // and checks to see when the flush occurs relative to the write count

    // Until the flush counts in the delay flush task are sent back to the
    // main mut_records context, the test is verifid by inspection of the trace
    // RUST_LOG=debug cargo test test_write_records_idle_delay

    // The test still verifies that flushes on writes have occured within the
    // expected timeframe
    #[cfg(not(target_os = "macos"))]
    #[allow(clippy::unnecessary_mut_passed)]
    #[fluvio_future::test]
    async fn test_write_records_idle_delay() {
        use std::time::Duration;
        use fluvio_future::timer;
        const TEST_FILE_NAMEI: &str = "00000000000000000300.log"; // for offset 300

        let test_file = temp_dir().join(TEST_FILE_NAMEI);
        ensure_clean_file(&test_file);

        const IDLE_FLUSH: u32 = 500;
        const OFFSET: i64 = 300;

        let options = ConfigOption {
            base_dir: temp_dir(),
            segment_max_bytes: 1000,
            flush_write_count: 0,
            flush_idle_msec: IDLE_FLUSH,
            ..Default::default()
        };
        let mut msg_sink = MutFileRecords::create(OFFSET, &options)
            .await
            .expect("create");

        let batch = create_batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("create");

        debug!("direct flush");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created
        debug!("direct done");

        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");

        let batch =
            Batch::<MemoryRecords>::decode_from(&mut Cursor::new(bytes), 0).expect("decode");
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records().len(), 2);
        let mut records = batch.own_records();
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.as_ref(), vec![10, 20]);
        let record2 = records.remove(0);
        assert_eq!(record2.value.as_ref(), vec![10, 20]);

        // check flush counts don't increment immediately
        let flush_count = msg_sink.flush_count();
        // debug!("flush_count: {} {:?}", flush_count, Instant::now());
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        // debug!("flush_count: {}", flush_count);
        assert_eq!(flush_count, msg_sink.flush_count());

        debug!("check single write delayed flush: wait for flush");
        let dur = Duration::from_millis((IDLE_FLUSH + 100).into());
        timer::after(dur).await;
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        // flush count needs some ownership rework to increment correctly
        // assert_eq!(flush_count + 1, msg_sink.flush_count());
        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * 2;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        debug!("check multi write delayed flush: wait for flush");
        let flush_count = msg_sink.flush_count();

        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        assert_eq!(flush_count, msg_sink.flush_count());
        msg_sink
            .write_batch(&mut create_batch())
            .await
            .expect("send");
        assert_eq!(flush_count, msg_sink.flush_count());

        let dur = Duration::from_millis((IDLE_FLUSH + 100).into());
        timer::after(dur).await;
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * 4;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        // this drop and await is useful to verify the flush task exits the
        // drop drops the mutrecords struct, and the await allows a scheduling
        // switchover to the delay flush task to end itself
        // TODO: find a way to use tracing lib to automatically verify this?
        drop(msg_sink);
        timer::after(dur).await;

        let old_msg_sink = MutFileRecords::create(OFFSET, &options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);
    }
}
