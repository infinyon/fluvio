use std::{io::Error as IoError, sync::Arc, time::Instant};
use std::io::{ErrorKind, Cursor};
use std::sync::Mutex;
use std::os::unix::io::RawFd;

use anyhow::{Result, Error, anyhow};

use tracing::{debug, warn};
use nix::sys::uio::pread;
use wasmtime::{Caller, Extern, Func, Instance, Trap, TypedFunc, Memory};

use fluvio_future::file_slice::AsyncFileSlice;
use dataplane::core::{Decoder, Encoder};
use dataplane::Offset;
use dataplane::batch::{BATCH_FILE_HEADER_SIZE, BATCH_HEADER_SIZE, Batch};
use dataplane::batch::MemoryRecords;
use crate::smart_stream::SmartStreamModuleInner;

const FILTER_FN_NAME: &str = "filter";

impl SmartStreamModuleInner {
    pub fn create_filter(&self) -> Result<SmartFilter> {
        let callback = Arc::new(RecordsCallBack::new());
        let callback2 = callback.clone();
        let copy_records = Func::wrap(
            &self.store,
            move |caller: Caller<'_>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm filter");
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };

                let records = RecordsMemory { ptr, len, memory };

                callback.set(records);

                Ok(())
            },
        );

        let instance = Instance::new(&self.store, &self.module, &[copy_records.into()])?;

        let filter_fn = instance.get_typed_func::<(i32, i32), i32>(FILTER_FN_NAME)?;

        Ok(SmartFilter::new(
            filter_fn,
            SmartStreamInstance::new(instance),
            callback2,
        ))
    }
}

pub struct SmartStreamInstance(Instance);

impl SmartStreamInstance {
    fn new(instance: Instance) -> Self {
        Self(instance)
    }

    pub fn copy_memory_to(&self, bytes: &[u8]) -> Result<isize, Error> {
        use super::memory::copy_memory_to_instance;
        copy_memory_to_instance(bytes, &self.0)
    }
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self) -> Vec<u8> {
        unsafe {
            if let Some(data) = self
                .memory
                .data_unchecked()
                .get(self.ptr as u32 as usize..)
                .and_then(|arr| arr.get(..self.len as u32 as usize))
            {
                debug!(wasm_mem_len = self.len, "copying from wasm");
                let mut bytes = vec![0u8; self.len as u32 as usize];
                bytes.copy_from_slice(data);
                bytes
            } else {
                vec![]
            }
        }
    }
}

pub struct RecordsCallBack(Mutex<Option<RecordsMemory>>);

impl RecordsCallBack {
    fn new() -> Self {
        Self(Mutex::new(None))
    }

    fn set(&self, records: RecordsMemory) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.replace(records);
    }

    fn clear(&self) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.take();
    }

    fn get(&self) -> Option<RecordsMemory> {
        let reader = self.0.lock().unwrap();
        reader.clone()
    }
}

/// Instance are not thread safe, we need to take care to ensure access to instance are thread safe

/// Instance must be hold in thread safe lock to ensure only one thread can access at time
pub struct SmartFilter {
    filter_fn: TypedFunc<(i32, i32), i32>,
    instance: SmartStreamInstance,
    records_cb: Arc<RecordsCallBack>,
}

impl SmartFilter {
    pub fn new(
        filter_fn: TypedFunc<(i32, i32), i32>,
        instance: SmartStreamInstance,
        records_cb: Arc<RecordsCallBack>,
    ) -> Self {
        Self {
            filter_fn,
            instance,
            records_cb,
        }
    }

    /// filter batches with maximum bytes to be send back consumer
    pub fn filter(&self, slice: AsyncFileSlice, max_bytes: usize) -> Result<Batch, Error> {
        use std::os::unix::io::AsRawFd;

        let fd = slice.as_raw_fd();

        let mut batch_iterator =
            FileBatchIterator::new(fd, slice.position() as i64, slice.len() as i64);

        let mut filter_batch = Batch::<MemoryRecords>::default();
        filter_batch.base_offset = -1; // indicate this is unitialized
        filter_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            if let Some(batch_result) = batch_iterator.next() {
                // we filter-map entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is dicarded
                let file_batch = batch_result?;

                debug!(
                    current_batch_offset = file_batch.batch.base_offset,
                    current_batch_offset_delta = file_batch.offset_delta(),
                    filter_offset_delta = filter_batch.get_header().last_offset_delta,
                    filter_base_offset = filter_batch.base_offset,
                    filter_records = filter_batch.records().len(),
                    "starting filter processing"
                );

                let now = Instant::now();

                self.records_cb.clear();

                let array_ptr = self.instance.copy_memory_to(&file_batch.records)?;

                let filter_record_count = self
                    .filter_fn
                    .call((array_ptr as i32, file_batch.records.len() as i32))?;

                debug!(filter_record_count,filter_execution_time = %now.elapsed().as_millis());

                if filter_record_count == -1 {
                    return Err(anyhow!("filter failed"));
                }

                let bytes = self
                    .records_cb
                    .get()
                    .map(|m| m.copy_memory_from())
                    .unwrap_or_default();
                debug!(out_filter_bytes = bytes.len());
                // this is inefficient for now
                let mut records: MemoryRecords = vec![];
                records.decode(&mut Cursor::new(bytes), 0)?;

                // there are filtered records!!
                if records.is_empty() {
                    debug!("filters records empty");
                } else {
                    // set base offset if this is first time
                    if filter_batch.base_offset == -1 {
                        filter_batch.base_offset = file_batch.base_offset();
                    }

                    // difference between filter batch and and current batch
                    // since base are different we need update delta offset for each records
                    let relative_base_offset = filter_batch.base_offset - file_batch.base_offset();

                    for record in &mut records {
                        record.add_base_offset(relative_base_offset);
                    }

                    let record_bytes = records.write_size(0);

                    // if filter bytes exceed max bytes then we skip this batch
                    if total_bytes + record_bytes > max_bytes {
                        debug!(
                            total_bytes = total_bytes + record_bytes,
                            max_bytes, "total filter bytes reached"
                        );
                        return Ok(filter_batch);
                    }

                    total_bytes += record_bytes;

                    debug!(
                        filter_records = records.len(),
                        total_bytes, "finished filtering"
                    );
                    filter_batch.mut_records().append(&mut records);
                }

                // only increment filter offset delta if filter_batch has been initialized
                if filter_batch.base_offset != -1 {
                    debug!(
                        offset_delta = file_batch.offset_delta(),
                        "adding to offset delta"
                    );
                    filter_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
                }
            } else {
                debug!(
                    total_records = filter_batch.records().len(),
                    "no more batches filter end"
                );
                return Ok(filter_batch);
            }
        }
    }
}

/*
async fn read<'a>(fd: RawFd, bytes: &'a mut [u8], position: i64) -> Result<usize, IoError> {



    spawn_blocking(  || {
        pread(fd, &mut bytes,position)
    }).await.map_err(|err| IoError::new(
        ErrorKind::Other,
        format!("pread error {}",err)
    ))

}
*/

// only encode information necessary to decode batches efficiently
struct FileBatch {
    batch: Batch,
    records: Vec<u8>,
}

impl FileBatch {
    fn base_offset(&self) -> Offset {
        self.batch.base_offset
    }

    fn offset_delta(&self) -> i32 {
        self.batch.header.last_offset_delta
    }
}

/// Iterator that returns batch from file
struct FileBatchIterator {
    fd: RawFd,
    offset: i64,
    end: i64,
}

impl FileBatchIterator {
    fn new(fd: RawFd, offset: i64, len: i64) -> Self {
        Self {
            fd,
            offset,
            end: offset + len,
        }
    }
}

impl Iterator for FileBatchIterator {
    type Item = Result<FileBatch, IoError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.offset >= self.end {
            return None;
        }

        let mut header = vec![0u8; BATCH_FILE_HEADER_SIZE];
        let bytes_read = match pread(self.fd, &mut header, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {}", err)))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < header.len() {
            warn!(bytes_read, header_len = header.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not eough for batch header {} out of {}",
                    bytes_read,
                    header.len()
                ),
            )));
        }

        let mut batch = Batch::default();
        if let Err(err) = batch.decode_from_file_buf(&mut Cursor::new(header), 0) {
            return Some(Err(IoError::new(
                ErrorKind::Other,
                format!("decodinge batch header error {}", err),
            )));
        }

        let remainder = batch.batch_len as usize - BATCH_HEADER_SIZE as usize;

        debug!(
            file_offset = self.offset,
            base_offset = batch.base_offset,
            "fbatch header"
        );

        let mut records = vec![0u8; remainder];

        self.offset += BATCH_FILE_HEADER_SIZE as i64;

        let bytes_read = match pread(self.fd, &mut records, self.offset)
            .map_err(|err| IoError::new(ErrorKind::Other, format!("pread error {}", err)))
        {
            Ok(bytes) => bytes,
            Err(err) => return Some(Err(err)),
        };

        if bytes_read < records.len() {
            warn!(bytes_read, record_len = records.len());
            return Some(Err(IoError::new(
                ErrorKind::UnexpectedEof,
                format!(
                    "not enough for batch records {} out of {}",
                    bytes_read,
                    records.len()
                ),
            )));
        }

        self.offset += bytes_read as i64;

        debug!(file_offset = self.offset, "fbatch end");

        Some(Ok(FileBatch { batch, records }))
    }
}

#[cfg(test)]
mod test {
    use std::{fs::File, io::Write};
    use std::env::temp_dir;
    use std::os::unix::io::AsRawFd;

    use fluvio_storage::config::DEFAULT_MAX_BATCH_SIZE;

    use super::*;

    #[test]
    fn test_file() {
        let path = temp_dir().join("pread.txt");
        let mut file = File::create(&path).expect("create");
        file.write_all(b"Hello, world!").expect("write");
        file.sync_all().expect("flush");
        drop(file);

        let read_only = File::open(path).expect("open");
        let fd = read_only.as_raw_fd();
        let mut buf = vec![0; DEFAULT_MAX_BATCH_SIZE as usize];
        // let mut buf = BytesMut::with_capacity(64);
        let bytes_read = pread(fd, &mut buf, 1).expect("");
        println!("bytes read: {}", bytes_read);
        assert!(bytes_read > 2);
    }
}
