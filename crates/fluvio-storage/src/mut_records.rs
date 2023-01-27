use std::io::Error as IoError;
use std::io::Write;
use std::os::unix::prelude::AsRawFd;
use std::os::unix::prelude::FromRawFd;
use std::path::PathBuf;
use std::path::Path;
use std::fmt;
use std::time::{Instant};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use tracing::instrument;
use tracing::{debug, trace};
use futures_lite::io::AsyncWriteExt;
use async_channel::Sender;
use anyhow::Result;

use fluvio_protocol::record::BatchRecords;
use fluvio_future::fs::File;
use fluvio_future::file_slice::AsyncFileSlice;
use fluvio_future::fs::BoundedFileSinkError;
use fluvio_protocol::record::Batch;
use fluvio_protocol::record::{Offset, Size, Size64};
use fluvio_protocol::Encoder;

use crate::config::SharedReplicaConfig;
use crate::mut_index::MutLogIndex;
use crate::util::generate_file_name;
use crate::validator::LogValidationError;
use crate::records::FileRecords;
use crate::validator::LogValidator;

pub const MESSAGE_LOG_EXTENSION: &str = "log";

// Delay flush scheduling interrival time constant
//   not something that a user should change
// this is a delay put in because the scheduler switch
// to the flush task is fast enough that it might beat
// out waiting writes which should be preferred
//const DELAY_FLUSH_SIA_MSEC: u64 = 3;

/// Can append new batch to file
pub struct MutFileRecords {
    base_offset: Offset,
    file: File,
    len: u32,
    max_len: u32,
    _flush_policy: FlushPolicy,
    write_count: u64,
    flush_count: Arc<AtomicU32>,
    path: PathBuf,
    _flush_time_tx: Option<Sender<Instant>>,
}

impl fmt::Debug for MutFileRecords {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Log({})", self.base_offset)
    }
}

impl Unpin for MutFileRecords {}

fn get_flush_policy_from_config(option: &SharedReplicaConfig) -> FlushPolicy {
    if option.flush_idle_msec.get() > 0 {
        FlushPolicy::IdleFlush {
            delay_millis: option.flush_idle_msec.get(),
        }
    } else if option.flush_write_count.get() == 0 {
        FlushPolicy::NoFlush
    } else if option.flush_write_count.get() == 1 {
        FlushPolicy::EveryWrite
    } else {
        FlushPolicy::CountWrites {
            n_writes: option.flush_write_count.get(),
            write_tracking: 0,
        }
    }
}

impl MutFileRecords {
    pub async fn create(
        base_offset: Offset,
        option: Arc<SharedReplicaConfig>,
    ) -> Result<MutFileRecords, BoundedFileSinkError> {
        let log_path = generate_file_name(&option.base_dir, base_offset, MESSAGE_LOG_EXTENSION);
        let max_len = option.segment_max_bytes.get();
        debug!(log_path = ?log_path, max_len = "creating log at");
        let file = fluvio_future::fs::util::open_read_append(log_path.clone()).await?;
        let metadata = file.metadata().await?;
        let len = metadata.len() as u32;
        debug!(len, "log created");
        Ok(MutFileRecords {
            base_offset,
            file,
            len,
            max_len,
            _flush_policy: get_flush_policy_from_config(&option),
            write_count: 0,
            flush_count: Arc::new(AtomicU32::new(0)),
            path: log_path.to_owned(),
            _flush_time_tx: None,
        })
    }

    pub fn get_base_offset(&self) -> Offset {
        self.base_offset
    }

    /// readjust to new length
    pub(crate) async fn set_len(&mut self, len: u32) -> Result<()> {
        self.file.set_len(len as u64).await?;
        self.len = len;
        Ok(())
    }

    pub(crate) async fn validate(&mut self, index: &MutLogIndex) -> Result<LogValidator> {
        LogValidator::default_validate(&self.path, Some(index)).await
    }

    /// get current file position
    #[inline(always)]
    pub fn get_pos(&self) -> Size {
        self.len
    }

    /// try to write batch
    /// if there is enough room, return true, false otherwise
    #[instrument(skip(self,batch),fields(pos=self.get_pos()))]
    pub async fn write_batch<R: BatchRecords>(
        &mut self,
        batch: &Batch<R>,
    ) -> Result<(bool, usize, u32)> {
        trace!("start sending using batch {:#?}", batch.get_header());
        if batch.base_offset < self.base_offset {
            return Err(LogValidationError::InvalidBaseOffsetMinimum {
                invalid_batch_offset: batch.base_offset,
            }
            .into());
        }

        let batch_len = batch.write_size(0);
        debug!(batch_len, "writing batch of size",);

        if (batch_len as u32 + self.len) <= self.max_len {
            let mut buffer: Vec<u8> = Vec::with_capacity(batch_len);
            batch.encode(&mut buffer, 0)?;
            assert_eq!(buffer.len(), batch_len);

            let raw_fd = self.file.as_raw_fd();
            let mut std_file = unsafe { std::fs::File::from_raw_fd(raw_fd) };
            std_file.write_all(&buffer)?;
            std::mem::forget(std_file);

            self.len += batch_len as u32;
            debug!(pos = self.get_pos(), "update pos",);
            self.write_count = self.write_count.saturating_add(1);

            self.flush().await?;
            debug!(
                flush_count = self.flush_count(),
                write_count = self.write_count,
                "Flushing Now"
            );

            /*
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
            */

            Ok((true, batch_len, self.len))
        } else {
            debug!(self.len, batch_len, "no more room to add");
            Ok((false, batch_len, self.len))
        }
    }

    pub async fn flush(&mut self) -> Result<(), IoError> {
        self.flush_count.fetch_add(1, Ordering::Relaxed);
        self.file.flush().await?;
        Ok(())
    }

    pub fn flush_count(&self) -> u32 {
        self.flush_count.load(Ordering::Relaxed)
    }

    /*
    async fn delay_flush(&mut self, _delay_millis: u32) -> Result<(), IoError> {
        //let delay_tgt = delay_millis as u64;

        if self.flush_time_tx.is_none() {
            // no task running so start one
            let (tx, rx) = async_channel::bounded(100);
            self.flush_time_tx = Some(tx);
           // let delay_tgt = Duration::from_millis(delay_tgt);
            let flush_count = self.flush_count.clone();

            if let Err(e) = self.file.flush().await {
                warn!("flush error {}", e);
            } else {
                let fc = flush_count.fetch_add(1, Ordering::Relaxed);
                debug!(fc,"flush");
            }


            fluvio_future::task::spawn(async move {
                let mut delay_dur = delay_tgt;
                let mut write_time = Instant::now();

                // when the mut_record struct is dropped self.flush_time_tx
                // will also be closed and dropped, causing this while loop
                // to end then exit the task
                while !rx.is_closed() {
                    if let Ok(wt) = rx.recv().await {
                        // always grab initial write time, but also
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
                    // could bundle write time with f_sink inside a struct
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
                    if let Err(e) = self.file.flush().await {
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
    */
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

    fn len(&self) -> Size64 {
        self.len as u64
    }

    fn get_path(&self) -> &Path {
        &self.path
    }

    fn as_file_slice(&self, start: Size) -> Result<AsyncFileSlice, IoError> {
        let reslice = AsyncFileSlice::new(
            self.file.as_raw_fd(),
            start as u64,
            (self.len - start) as u64,
        );
        Ok(reslice)
    }

    fn as_file_slice_from_to(&self, start: Size, len: Size) -> Result<AsyncFileSlice, IoError> {
        let reslice = AsyncFileSlice::new(self.file.as_raw_fd(), start as u64, len as u64);
        Ok(reslice)
    }

    fn file(&self) -> File {
        unsafe { File::from_raw_fd(self.file.as_raw_fd()) }
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

#[allow(unused)]
enum FlushAction {
    NoFlush,
    Now,
    Delay(u32),
}

impl FlushPolicy {
    /// Evaluates the flush policy and returns a flush action
    // to take
    #[allow(unused)]
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

    use fluvio_protocol::record::Offset;
    use flv_util::fixture::{ensure_new_dir};
    use fluvio_protocol::record::{Batch, MemoryRecords};
    use fluvio_protocol::{Decoder, Encoder};
    use fluvio_protocol::fixture::read_bytes_from_file;

    use crate::config::ReplicaConfig;
    use crate::records::FileRecords;
    use crate::fixture::BatchProducer;
    use super::MutFileRecords;

    #[fluvio_future::test]
    async fn test_records_with_invalid_base() {
        const BASE_OFFSET: Offset = 401;

        let test_dir = temp_dir().join("records_with_invalid_base");
        ensure_new_dir(&test_dir).expect("new");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
        .shared();

        let mut builder = BatchProducer::builder()
            .base_offset(BASE_OFFSET)
            .build()
            .expect("build");

        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, options)
            .await
            .expect("create");

        msg_sink
            .write_batch(&builder.batch())
            .await
            .expect("create");

        // use wrong base offset
        let mut wrong_builder = BatchProducer::builder()
            .base_offset(200)
            .build()
            .expect("build");

        assert!(msg_sink.write_batch(&wrong_builder.batch()).await.is_err());
    }

    #[fluvio_future::test]
    async fn test_write_records_every() {
        const BASE_OFFSET: Offset = 100;

        let test_dir = temp_dir().join("write_records_every");
        ensure_new_dir(&test_dir).expect("new");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            ..Default::default()
        }
        .shared();
        let mut msg_sink = MutFileRecords::create(BASE_OFFSET, options.clone())
            .await
            .expect("create");

        //debug!("{:?}", msg_sink.flush_policy);
        assert_eq!(msg_sink.get_pos(), 0);

        let mut builder = BatchProducer::builder()
            .base_offset(BASE_OFFSET)
            .build()
            .expect("build");

        let batch = builder.batch();
        let write_size = batch.write_size(0);
        debug!(write_size, "write size"); // for now, this is 79 bytes
        msg_sink.write_batch(&batch).await.expect("create");
        assert_eq!(msg_sink.get_pos() as usize, write_size);

        let log_path = msg_sink.get_path().to_owned();

        debug!("read start");
        let bytes = read_bytes_from_file(&log_path).expect("read bytes");
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

        debug!("write 2");
        msg_sink.write_batch(&builder.batch()).await.expect("write");
        assert_eq!(msg_sink.get_pos() as usize, write_size * 2);

        let bytes = read_bytes_from_file(&log_path).expect("read");
        assert_eq!(bytes.len(), write_size * 2, "should be 158 bytes");

        // check if we can read the records and get base offset
        let old_msg_sink = MutFileRecords::create(BASE_OFFSET, options)
            .await
            .expect("open");
        assert_eq!(old_msg_sink.get_base_offset(), BASE_OFFSET);
        assert_eq!(old_msg_sink.get_pos() as usize, write_size * 2);
    }

    // This Test configures policy to flush after every NUM_WRITES
    // and checks to see when the flush occurs relative to the write count

    //#[fluvio_future::test]
    #[allow(unused)]
    async fn test_write_records_count() {
        let test_dir = temp_dir().join("mut_records_word_count");
        ensure_new_dir(&test_dir).expect("new");

        const NUM_WRITES: u32 = 4;
        const OFFSET: i64 = 200;

        let mut builder = BatchProducer::builder()
            .base_offset(OFFSET)
            .build()
            .expect("build");

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            flush_write_count: NUM_WRITES,
            ..Default::default()
        }
        .shared();
        let mut msg_sink = MutFileRecords::create(OFFSET, options.clone())
            .await
            .expect("create");
        let test_file = msg_sink.get_path().to_owned();

        let batch = builder.batch();
        let write_size = batch.write_size(0);
        debug!(write_size, "write size"); // for now, this is 79 bytes
        msg_sink.write_batch(&batch).await.expect("create");
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
        msg_sink.write_batch(&builder.batch()).await.expect("send");
        for _ in 1..(NUM_WRITES - 2) {
            msg_sink.write_batch(&builder.batch()).await.expect("send");
            assert_eq!(flush_count, msg_sink.flush_count());
        }

        // flush count should increment after final write
        msg_sink.write_batch(&builder.batch()).await.expect("send");
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * NUM_WRITES as usize;
        assert_eq!(bytes.len(), nbytes, "should be {nbytes} bytes");

        let old_msg_sink = MutFileRecords::create(OFFSET, options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);
    }

    // This test configures policy to flush after some write idle time
    // and checks to see when the flush occurs relative to the write count

    // Until the flush counts in the delay flush task are sent back to the
    // main mut_records context, the test is verified by inspection of the trace
    // RUST_LOG=debug cargo test test_write_records_idle_delay

    // The test still verifies that flushes on writes have occurred within the
    // expected timeframe
    #[cfg(not(target_os = "macos"))]
    #[allow(unused)]
    //#[fluvio_future::test]
    async fn test_write_records_idle_delay() {
        use std::time::Duration;
        use fluvio_future::timer;

        let test_dir = temp_dir().join("test_write_records_idle_delay");
        ensure_new_dir(&test_dir).expect("new");

        const IDLE_FLUSH: u32 = 500;
        const OFFSET: i64 = 300;

        let options = ReplicaConfig {
            base_dir: test_dir,
            segment_max_bytes: 1000,
            flush_write_count: 0,
            flush_idle_msec: IDLE_FLUSH,
            ..Default::default()
        }
        .shared();

        let mut msg_sink = MutFileRecords::create(OFFSET, options.clone())
            .await
            .expect("create");

        let mut builder = BatchProducer::builder()
            .base_offset(OFFSET)
            .build()
            .expect("build");

        let batch = builder.batch();
        let write_size = batch.write_size(0);
        debug!("write size: {}", write_size); // for now, this is 79 bytes
        msg_sink.write_batch(&batch).await.expect("create");

        debug!("direct flush");
        msg_sink.flush().await.expect("create flush"); // ensure the file is created
        debug!("direct done");

        let test_file = msg_sink.get_path().to_owned();

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
        msg_sink.write_batch(&builder.batch()).await.expect("send");
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
        assert_eq!(bytes.len(), nbytes, "should be {nbytes} bytes");

        debug!("check multi write delayed flush: wait for flush");
        let flush_count = msg_sink.flush_count();

        msg_sink.write_batch(&builder.batch()).await.expect("send");
        assert_eq!(flush_count, msg_sink.flush_count());
        msg_sink.write_batch(&builder.batch()).await.expect("send");
        assert_eq!(flush_count, msg_sink.flush_count());

        let dur = Duration::from_millis((IDLE_FLUSH + 100).into());
        timer::after(dur).await;
        assert_eq!(flush_count + 1, msg_sink.flush_count());

        let bytes = read_bytes_from_file(&test_file).expect("read bytes final");
        let nbytes = write_size * 4;
        assert_eq!(bytes.len(), nbytes, "should be {nbytes} bytes");

        // this drop and await is useful to verify the flush task exits the
        // drop drops the mutrecords struct, and the await allows a scheduling
        // switchover to the delay flush task to end itself
        // TODO: find a way to use tracing lib to automatically verify this?
        drop(msg_sink);
        timer::after(dur).await;

        let old_msg_sink = MutFileRecords::create(OFFSET, options)
            .await
            .expect("check old sink");
        assert_eq!(old_msg_sink.get_base_offset(), OFFSET);
    }
}
