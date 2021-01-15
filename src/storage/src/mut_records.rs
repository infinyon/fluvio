use std::io::Error as IoError;
use std::path::PathBuf;
use std::path::Path;
use std::time::{Duration, Instant};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use async_mutex::Mutex;

use tracing::debug;
use tracing::warn;
use tracing::trace;
use futures_lite::future::block_on;
use futures_lite::io::AsyncWriteExt;
use async_channel::Sender;

use fluvio_future::timer;
// use fluvio_future::fs::File;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::BoundedFileSink;
use fluvio_future::fs::BoundedFileOption;
use fluvio_future::fs::BoundedFileSinkError;
use dataplane::batch::DefaultBatch;
use dataplane::{Offset, Size};
use dataplane::core::Encoder;

use crate::util::generate_file_name;
use crate::validator::validate;
use crate::validator::LogValidationError;
use crate::ConfigOption;
use crate::StorageError;
use crate::records::FileRecords;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    item_last_offset_delta: Size,
    f_sink: SinkMutex,
    flush_policy: MutFileRecordsFlushPolicy,
    write_count: u64,
    flush_count: AtomicU32,
    path: PathBuf,
    flush_time_tx: Option<Sender<Instant>>,
}
type SinkMutex = Arc<Mutex<BoundedFileSink>>;

impl Unpin for MutFileRecords {}

fn get_flush_policy_from_config(option: &ConfigOption) -> MutFileRecordsFlushPolicy {
    if option.flush_idle_msec > 0 {
        MutFileRecordsFlushPolicy::IdleFlush {
            delay_millis: option.flush_idle_msec,
        }
    } else {
        if option.flush_write_count == 0 {
            MutFileRecordsFlushPolicy::NoFlush
        } else if option.flush_write_count == 1 {
            MutFileRecordsFlushPolicy::EveryWrite
        } else {
            MutFileRecordsFlushPolicy::CountWrites {
                n_writes: option.flush_write_count,
                write_tracking: 0,
            }
        }
    }
}

impl MutFileRecords {
    pub async fn create(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, BoundedFileSinkError> {
        let sink_option = BoundedFileOption {
            max_len: Some(option.segment_max_bytes as u64),
        };
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("creating log at: {}", log_path.display());
        let f_sink = BoundedFileSink::open_append(&log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink: Arc::new(Mutex::new(f_sink)),
            flush_policy: get_flush_policy_from_config(option),
            write_count: 0,
            flush_count: AtomicU32::new(0),
            item_last_offset_delta: 0,
            path: log_path.to_owned(),
            flush_time_tx: None,
        })
    }

    pub async fn open(
        base_offset: Offset,
        option: &ConfigOption,
    ) -> Result<MutFileRecords, StorageError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        debug!("opening commit log at: {}", log_path.display());

        let sink_option = BoundedFileOption {
            max_len: Some(option.segment_max_bytes as u64),
        };
        let f_sink = BoundedFileSink::open_append(&log_path, sink_option).await?;
        Ok(MutFileRecords {
            base_offset,
            f_sink: Arc::new(Mutex::new(f_sink)),
            flush_policy: get_flush_policy_from_config(option),
            write_count: 0,
            flush_count: AtomicU32::new(0),
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
        block_on(async {
            let mf_sink = self.f_sink.clone();
            let f_sink = mf_sink.lock().await;
            f_sink.get_current_len() as Size
        })
    }

    pub fn get_item_last_offset_delta(&self) -> Size {
        self.item_last_offset_delta
    }

    pub async fn send(&mut self, item: DefaultBatch) -> Result<(), StorageError> {
        trace!("start sending using batch {:#?}", item.get_header());
        self.item_last_offset_delta = item.get_last_offset_delta();
        let mut buffer: Vec<u8> = vec![];
        item.encode(&mut buffer, 0)?;
        let mf_sink = self.f_sink.clone();
        let mut f_sink = mf_sink.lock().await;

        if f_sink.can_be_appended(buffer.len() as u64) {
            debug!("writing {} bytes at: {}", buffer.len(), self.path.display());

            f_sink.write_all(&buffer).await?;
            drop(f_sink); // unlock because flush may reaqire the lock
            self.write_count = self.write_count.saturating_add(1);

            use MutFileRecordsFlushAction::*;
            match self.flush_policy.should_flush() {
                FlushNone => {}

                FlushNow => {
                    debug!("  flushing {:?}", self.flush_policy);
                    self.flush().await?;
                }

                FlushDelay(delay_millis) => {
                    debug!("  delay flush start {:?}", self.flush_policy);
                    self.delay_flush(delay_millis)?;
                }
            }

            Ok(())
        } else {
            Err(StorageError::NoRoom(item))
        }
    }

    #[allow(unused)]
    pub async fn flush(&mut self) -> Result<(), IoError> {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        let mut f_sink = self.f_sink.lock().await;
        debug!("flush: count {:?}", self.flush_count);
        f_sink.flush().await
    }

    #[allow(unused)]
    pub fn flush_count(&self) -> u32 {
        self.flush_count.load(Ordering::Relaxed)
    }

    fn delay_flush(&mut self, delay_millis: u32) -> Result<(), IoError> {
        let delay_tgt = delay_millis as u64;
        let mf_sink = self.f_sink.clone();

        if self.flush_time_tx.is_none() {
            // no task running so start one
            let (tx, rx) = async_channel::bounded(100);
            self.flush_time_tx = Some(tx);
            let delay_tgt = Duration::from_millis(delay_tgt);
            // let flush_count_ref = self.flush_count.get_mut();

            // TBD need the task or exit the loop when the mut_records
            // struct is dropped
            fluvio_future::task::spawn(async move {
                let mut delay_dur = delay_tgt.clone();
                let mut task_flush_count = 0;
                let mut write_time = Instant::now();
                while !rx.is_closed() {
                    if let Ok(wt) = rx.recv().await {
                        // always grab inital write time, but also
                        // guarantee a wait on a write time for flush
                        // for wait times after the first delay runs
                        write_time = wt;
                    }
                    //  a short fixed wait was added here to give a little
                    //  breathing space to accumulate clusters of writes which
                    //  arrive closely but not fast enough for this task loop
                    //  between the first and following check for write times

                    //  It prevents extra flushes and doesn't affet the delay
                    //  from the last write in the cluster but
                    timer::after(Duration::from_millis(3)).await;

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
                        // *flush_count_ref += 1;
                        task_flush_count += 1;
                        debug!(" - flushed: delay task flush cnt: {}", task_flush_count);
                    }
                }
                debug!("delay_flush task exited");
            });
        }

        // update running flush
        if let Some(flush_tx) = &self.flush_time_tx {
            let tx = flush_tx.clone();
            fluvio_future::task::run(async move {
                if let Err(e) = tx.send(Instant::now()).await {
                    warn!("flush error {}", e);
                }
            });
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
        block_on(async {
            let am = self.f_sink.clone();
            let f_sink = am.lock().await;
            let slice = f_sink.slice_from(start as u64, f_sink.get_current_len() - start as u64);
            slice
        })
    }

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError> {
        block_on(async {
            let am = self.f_sink.clone();
            let f_sink = am.lock().await;
            let slice = f_sink.slice_from(start as u64, len as u64);
            slice
        })
    }
}

/// MutFileRecordsFlushPolicy describes and implements a flush policy
#[derive(Debug)]
pub enum MutFileRecordsFlushPolicy {
    NoFlush,
    EveryWrite,
    CountWrites { n_writes: u32, write_tracking: u32 },
    IdleFlush { delay_millis: u32 },
}

enum MutFileRecordsFlushAction {
    FlushNone,
    FlushNow,
    FlushDelay(u32),
}

impl MutFileRecordsFlushPolicy {
    /// Evaluates the flush policy and returns a flush action
    // to take
    fn should_flush(&mut self) -> MutFileRecordsFlushAction {
        use MutFileRecordsFlushPolicy::*;
        use MutFileRecordsFlushAction::*;

        match self {
            NoFlush => FlushNone,

            EveryWrite => FlushNow,

            CountWrites {
                n_writes: n_max,
                write_tracking: wcount,
            } => {
                *wcount += 1;
                if *wcount >= *n_max {
                    *wcount = 0;
                    return FlushNow;
                }
                FlushNone
            }

            IdleFlush { delay_millis } => FlushDelay(*delay_millis),
        }
    }
}

#[cfg(test)]
mod tests {

    // use std::time::{Duration, Instant};
    use std::time::Duration;
    use std::env::temp_dir;
    use std::io::Cursor;

    use tracing::debug;

    use fluvio_future::test_async;
    use fluvio_future::timer;
    use dataplane::batch::DefaultBatch;
    use dataplane::core::Decoder;
    use dataplane::core::Encoder;
    use flv_util::fixture::ensure_clean_file;

    use super::MutFileRecords;
    use super::StorageError;
    use crate::fixture::create_batch;
    use crate::fixture::read_bytes_from_file;
    use crate::ConfigOption;

    const TEST_FILE_NAME: &str = "00000000000000000100.log"; // for offset 100
    const TEST_FILE_NAMEC: &str = "00000000000000000200.log"; // for offset 200
    const TEST_FILE_NAMEI: &str = "00000000000000000300.log"; // for offset 300

    #[test_async]
    async fn test_write_records_every() -> Result<(), StorageError> {
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
        msg_sink.send(create_batch()).await.expect("create");

        debug!("read start");
        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");
        debug!("read ok");
        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 2);
        let mut records = batch.records;
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.inner_value(), Some(vec![10, 20]));
        let record2 = records.remove(0);
        assert_eq!(record2.value.inner_value(), Some(vec![10, 20]));

        debug!("write 2");
        msg_sink.send(create_batch()).await?;

        let bytes = read_bytes_from_file(&test_file)?;
        assert_eq!(bytes.len(), write_size * 2, "should be 158 bytes");

        let old_msg_sink = MutFileRecords::open(100, &options).await?;
        assert_eq!(old_msg_sink.get_base_offset(), 100);

        Ok(())
    }

    // This Test configures policy to flush after every NUM_WRITES
    // and checks to see when the flush occurs relative to the write count
    #[test_async]
    async fn test_write_records_count() -> Result<(), StorageError> {
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
        msg_sink.send(create_batch()).await.expect("create");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created

        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 2);
        let mut records = batch.records;
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.inner_value(), Some(vec![10, 20]));
        let record2 = records.remove(0);
        assert_eq!(record2.value.inner_value(), Some(vec![10, 20]));

        // check flush counts don't increment yet
        let flush_count = msg_sink.flush_count();
        msg_sink.send(create_batch()).await.expect("send");
        for _ in 1..(NUM_WRITES - 2) {
            msg_sink.send(create_batch()).await.expect("send");
            assert_eq!(flush_count, msg_sink.flush_count());
        }

        // flush count should increment after final write
        msg_sink.send(create_batch()).await.expect("send");
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * NUM_WRITES as usize;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        let old_msg_sink = MutFileRecords::open(OFFSET, &options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);

        Ok(())
    }

    // This test configures policy to flush after some write idle time
    // and checks to see when the flush occurs relative to the write count

    // Until the flush counts in the delay flush task are sent back to the
    // main mut_records context, the test is verific by inspection of the trace
    // RUST_LOG=debug cargo test test_write_records_idle_delay
    #[test_async]
    async fn test_write_records_idle_delay() -> Result<(), StorageError> {
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
        msg_sink.send(create_batch()).await.expect("create");

        debug!("direct flush");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created
        debug!("direct done");

        let bytes = read_bytes_from_file(&test_file).expect("read bytes");
        assert_eq!(bytes.len(), write_size, "incorrect size for write");

        let batch = DefaultBatch::decode_from(&mut Cursor::new(bytes), 0)?;
        assert_eq!(batch.get_header().magic, 2, "check magic");
        assert_eq!(batch.records.len(), 2);
        let mut records = batch.records;
        assert_eq!(records.len(), 2);
        let record1 = records.remove(0);
        assert_eq!(record1.value.inner_value(), Some(vec![10, 20]));
        let record2 = records.remove(0);
        assert_eq!(record2.value.inner_value(), Some(vec![10, 20]));

        // check flush counts don't increment immediately
        let flush_count = msg_sink.flush_count();
        // debug!("flush_count: {} {:?}", flush_count, Instant::now());
        msg_sink.send(create_batch()).await.expect("send");
        // debug!("flush_count: {}", flush_count);
        assert_eq!(flush_count, msg_sink.flush_count());

        debug!("check single write delayed flush: wait for flush");
        let dur = Duration::from_millis((IDLE_FLUSH + 100).into());
        timer::after(dur).await;
        debug!("flush should have occured");
        // debug!("check flush_count: {} {:?}", flush_count, Instant::now());

        // flush count needs some ownership rework to increment correctly
        // assert_eq!(flush_count + 1, msg_sink.flush_count());
        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * 2 as usize;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        debug!("check multi write delayed flush: wait for flush");
        let flush_count = msg_sink.flush_count();
        // debug!("flush_count: {} {:?}", flush_count, Instant::now());
        msg_sink.send(create_batch()).await.expect("send");
        msg_sink.send(create_batch()).await.expect("send");

        // debug!("flush_count: {}", flush_count);
        assert_eq!(flush_count, msg_sink.flush_count());
        let dur = Duration::from_millis((IDLE_FLUSH + 100).into());
        timer::after(dur).await;
        debug!("flush should have occured");
        // debug!("check flush_count: {} {:?}", flush_count, Instant::now());
        // flush count needs some ownership rework to increment correctly
        // assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * 4 as usize;
        assert_eq!(bytes.len(), nbytes, "should be {} bytes", nbytes);

        let old_msg_sink = MutFileRecords::open(OFFSET, &options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);

        Ok(())
    }
}
