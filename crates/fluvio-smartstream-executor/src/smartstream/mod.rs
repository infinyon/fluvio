use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::fmt::{self, Debug};

use tracing::{debug, trace};
use anyhow::{Result, Error};
use wasmtime::{Memory, Store, Engine, Module, Func, Caller, Extern, Trap, Instance};

use crate::smartstream::filter::SmartStreamFilter;
use crate::smartstream::map::SmartStreamMap;
use crate::smartstream::flatmap::SmartStreamFlatmap;
use crate::smartstream::aggregate::SmartStreamAggregate;
use dataplane::core::{Encoder, Decoder};
use dataplane::smartstream::{SmartStreamInput, SmartStreamOutput, SmartStreamRuntimeError};
use crate::smartstream::file_batch::FileBatchIterator;
use dataplane::batch::{Batch, MemoryRecords};
use std::path::Path;

mod memory;
pub mod filter;
pub mod map;
pub mod flatmap;
pub mod aggregate;
pub mod file_batch;

pub type WasmSlice = (i32, i32);

#[derive(Default, Clone)]
pub struct SmartStreamEngine(pub(crate) Engine);

impl SmartStreamEngine {
    pub fn create_module_from_binary(&self, bytes: &[u8]) -> Result<SmartStreamModule> {
        let module = Module::from_binary(&self.0, bytes)?;
        Ok(SmartStreamModule(module))
    }
    pub fn create_module_from_path<P: AsRef<Path>>(&self, path: P) -> Result<SmartStreamModule> {
        let binary = std::fs::read(path)?;
        let smart_stream_module = self
            .create_module_from_binary(&binary)
            .expect("Failed to create wasm module from binary");
        Ok(smart_stream_module)
    }
}

impl Debug for SmartStreamEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartStreamEngine")
    }
}

pub struct SmartStreamModule(pub(crate) Module);

impl SmartStreamModule {
    pub fn create_filter(&self, engine: &SmartStreamEngine) -> Result<SmartStreamFilter> {
        let filter = SmartStreamFilter::new(engine, self)?;
        Ok(filter)
    }

    pub fn create_map(&self, engine: &SmartStreamEngine) -> Result<SmartStreamMap> {
        let map = SmartStreamMap::new(engine, self)?;
        Ok(map)
    }

    pub fn create_flatmap(&self, engine: &SmartStreamEngine) -> Result<SmartStreamFlatmap> {
        let map = SmartStreamFlatmap::new(engine, self)?;
        Ok(map)
    }

    pub fn create_aggregate(
        &self,
        engine: &SmartStreamEngine,
        accumulator: Vec<u8>,
    ) -> Result<SmartStreamAggregate> {
        let aggregate = SmartStreamAggregate::new(engine, self, accumulator)?;
        Ok(aggregate)
    }
}

pub struct SmartStreamContext {
    store: Store<()>,
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
}

impl SmartStreamContext {
    pub fn new(engine: &SmartStreamEngine, module: &SmartStreamModule) -> Result<Self> {
        let mut store = Store::new(&engine.0, ());
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records = Func::wrap(
            &mut store,
            move |mut caller: Caller<'_, ()>, ptr: i32, len: i32| {
                debug!(len, "callback from wasm filter");
                let memory = match caller.get_export("memory") {
                    Some(Extern::Memory(mem)) => mem,
                    _ => return Err(Trap::new("failed to find host memory")),
                };

                let records = RecordsMemory { ptr, len, memory };
                cb.set(records);
                Ok(())
            },
        );

        let instance = Instance::new(&mut store, &module.0, &[copy_records.into()])?;
        Ok(Self {
            store,
            instance,
            records_cb,
        })
    }

    pub fn write_input<E: Encoder>(&mut self, input: &E) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, 0)?;
        let array_ptr =
            self::memory::copy_memory_to_instance(&mut self.store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32))
    }

    pub fn read_output<D: Decoder + Default>(&mut self) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(&mut self.store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), 0)?;
        Ok(output)
    }
}

pub trait SmartStream: Send {
    fn process(&mut self, input: SmartStreamInput) -> Result<SmartStreamOutput>;
}

impl dyn SmartStream + '_ {
    pub fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
    ) -> Result<(Batch, Option<SmartStreamRuntimeError>), Error> {
        let mut smartstream_batch = Batch::<MemoryRecords>::default();
        smartstream_batch.base_offset = -1; // indicate this is unitialized
        smartstream_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            let file_batch = match iter.next() {
                // we process entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is discarded
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = smartstream_batch.records().len(),
                        "No more batches, SmartStream end"
                    );
                    return Ok((smartstream_batch, None));
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                smartstream_offset_delta = smartstream_batch.get_header().last_offset_delta,
                smartstream_base_offset = smartstream_batch.base_offset,
                smartstream_records = smartstream_batch.records().len(),
                "Starting SmartStream processing"
            );

            let now = Instant::now();
            let input = SmartStreamInput {
                base_offset: file_batch.batch.base_offset,
                record_data: file_batch.records.clone(),
            };
            let output = self.process(input)?;
            debug!(smartstream_execution_time = %now.elapsed().as_millis());

            let maybe_error = output.error;
            let mut records = output.successes;

            trace!("smartstream processed records: {:#?}", records);

            // there are smartstreamed records!!
            if records.is_empty() {
                debug!("smartstreams records empty");
            } else {
                // set base offset if this is first time
                if smartstream_batch.base_offset == -1 {
                    smartstream_batch.base_offset = file_batch.base_offset();
                }

                // difference between smartstream batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset = smartstream_batch.base_offset - file_batch.base_offset();

                for record in &mut records {
                    record.add_base_offset(relative_base_offset);
                }

                let record_bytes = records.write_size(0);

                // if smartstream bytes exceed max bytes then we skip this batch
                if total_bytes + record_bytes > max_bytes {
                    debug!(
                        total_bytes = total_bytes + record_bytes,
                        max_bytes, "Total SmartStream bytes reached"
                    );
                    return Ok((smartstream_batch, maybe_error));
                }

                total_bytes += record_bytes;

                debug!(
                    smartstream_records = records.len(),
                    total_bytes, "finished smartstreaming"
                );
                smartstream_batch.mut_records().append(&mut records);
            }

            // only increment smartstream offset delta if smartstream_batch has been initialized
            if smartstream_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                smartstream_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }

            // If we had a processing error, return current batch and error
            if maybe_error.is_some() {
                return Ok((smartstream_batch, maybe_error));
            }
        }
    }
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self, store: &mut Store<()>) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; self.len as u32 as usize];
        self.memory.read(store, self.ptr as usize, &mut bytes)?;
        Ok(bytes)
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
