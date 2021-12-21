use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::fmt::{self, Debug};

use dataplane::record::Record;
use futures_util::Future;
use dataplane::smartmodule::SmartModuleExtraParams;
use tracing::{debug, instrument, trace};
use anyhow::{Error, Result};
use wasmtime::{Memory, Store, Engine, Module, Func, Caller, Extern, Trap, Instance};

use crate::smartmodule::filter::SmartModuleFilter;
use crate::smartmodule::map::SmartModuleMap;
use crate::filter_map::SmartModuleFilterMap;
use crate::smartmodule::array_map::SmartModuleArrayMap;
use crate::smartmodule::aggregate::SmartModuleAggregate;
use crate::smartmodule::join::SmartModuleJoin;

use dataplane::core::{Encoder, Decoder};
use dataplane::smartmodule::{SmartModuleInput, SmartModuleOutput, SmartModuleRuntimeError};
use crate::smartmodule::file_batch::FileBatchIterator;
use dataplane::batch::{Batch, MemoryRecords};
use fluvio_spu_schema::server::stream_fetch::{SmartModuleKind, LegacySmartModulePayload};

mod memory;
pub mod filter;
pub mod map;
pub mod array_map;
pub mod filter_map;
pub mod aggregate;
pub mod join;
pub mod file_batch;
pub mod join_stream;

pub type WasmSlice = (i32, i32, u32);
#[cfg(feature = "smartmodule")]
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec};

use self::join_stream::SmartModuleJoinStream;

const DEFAULT_SMARTENGINE_VERSION: i16 = 16;

#[derive(Default, Clone)]
pub struct SmartEngine(pub(crate) Engine);

impl SmartEngine {
    #[cfg(feature = "smartmodule")]
    pub fn create_module_from_smartmodule_spec(
        self,
        spec: &SmartModuleSpec,
    ) -> Result<SmartModuleWithEngine> {
        use fluvio_controlplane_metadata::smartmodule::{SmartModuleWasmFormat};
        use flate2::bufread::GzDecoder;
        use std::io::Read;

        let wasm_module = &spec.wasm;
        let mut decoder = GzDecoder::new(&*wasm_module.payload);
        let mut buffer = Vec::with_capacity(wasm_module.payload.len());
        decoder.read_to_end(&mut buffer)?;

        let module = match wasm_module.format {
            SmartModuleWasmFormat::Binary => Module::from_binary(&self.0, &buffer)?,
            SmartModuleWasmFormat::Text => return Err(Error::msg("Format not supported")),
        };
        Ok(SmartModuleWithEngine {
            module,
            engine: self,
        })
    }

    pub fn create_module_from_binary(self, bytes: &[u8]) -> Result<SmartModuleWithEngine> {
        let module = Module::from_binary(&self.0, bytes)?;
        Ok(SmartModuleWithEngine {
            module,
            engine: self,
        })
    }
    pub fn create_module_from_payload(
        self,
        smart_payload: LegacySmartModulePayload,
        maybe_version: Option<i16>,
    ) -> Result<Box<dyn SmartModuleInstance>> {
        let version = maybe_version.unwrap_or(DEFAULT_SMARTENGINE_VERSION);
        let smartmodule = self.create_module_from_binary(&smart_payload.wasm.get_raw()?)?;
        let smartmodule_instance: Box<dyn SmartModuleInstance> = match &smart_payload.kind {
            SmartModuleKind::Filter => {
                Box::new(smartmodule.create_filter(smart_payload.params, version)?)
            }
            SmartModuleKind::FilterMap => {
                Box::new(smartmodule.create_filter_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Map => {
                Box::new(smartmodule.create_map(smart_payload.params, version)?)
            }
            SmartModuleKind::ArrayMap => {
                Box::new(smartmodule.create_array_map(smart_payload.params, version)?)
            }
            SmartModuleKind::Join(_) => {
                Box::new(smartmodule.create_join(smart_payload.params, version)?)
            }
            SmartModuleKind::JoinStream {
                topic: _,
                derivedstream: _,
            } => Box::new(smartmodule.create_join_stream(smart_payload.params, version)?),
            SmartModuleKind::Aggregate { accumulator } => Box::new(smartmodule.create_aggregate(
                smart_payload.params,
                accumulator.clone(),
                version,
            )?),
        };
        Ok(smartmodule_instance)
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

pub struct SmartModuleWithEngine {
    pub(crate) module: Module,
    pub(crate) engine: SmartEngine,
}

impl SmartModuleWithEngine {
    fn create_filter(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleFilter> {
        let filter = SmartModuleFilter::new(&self.engine, self, params, version)?;
        Ok(filter)
    }

    fn create_map(&self, params: SmartModuleExtraParams, version: i16) -> Result<SmartModuleMap> {
        let map = SmartModuleMap::new(&self.engine, self, params, version)?;
        Ok(map)
    }

    fn create_filter_map(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleFilterMap> {
        let filter_map = SmartModuleFilterMap::new(&self.engine, self, params, version)?;
        Ok(filter_map)
    }

    fn create_array_map(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleArrayMap> {
        let map = SmartModuleArrayMap::new(&self.engine, self, params, version)?;
        Ok(map)
    }

    fn create_join(&self, params: SmartModuleExtraParams, version: i16) -> Result<SmartModuleJoin> {
        let join = SmartModuleJoin::new(&self.engine, self, params, version)?;
        Ok(join)
    }

    fn create_join_stream(
        &self,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<SmartModuleJoinStream> {
        let join = SmartModuleJoinStream::new(&self.engine, self, params, version)?;
        Ok(join)
    }

    fn create_aggregate(
        &self,
        params: SmartModuleExtraParams,
        accumulator: Vec<u8>,
        version: i16,
    ) -> Result<SmartModuleAggregate> {
        let aggregate =
            SmartModuleAggregate::new(&self.engine, self, params, accumulator, version)?;
        Ok(aggregate)
    }
}

pub struct SmartModuleContext {
    store: Store<()>,
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}

impl SmartModuleContext {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleWithEngine,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self> {
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

        let instance = Instance::new(&mut store, &module.module, &[copy_records.into()])?;
        Ok(Self {
            store,
            instance,
            records_cb,
            params,
            version,
        })
    }

    pub fn write_input<E: Encoder>(&mut self, input: &E) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(len = input_data.len(), "input data");
        let array_ptr =
            self::memory::copy_memory_to_instance(&mut self.store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as u32))
    }

    pub fn read_output<D: Decoder + Default>(&mut self) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(&mut self.store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }
}

pub trait SmartModuleInstance: Send + Sync {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput>;
    fn params(&self) -> SmartModuleExtraParams;
}

impl dyn SmartModuleInstance + '_ {
    #[instrument(skip(self, iter, max_bytes, join_last_record))]
    pub fn process_batch<Fut, OnBatchFn>(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        join_last_record: Option<&Record>,
    ) -> Result<(i64, bool), Error>
    where
        Fut: Future<Output = Result<(i64, bool), Error>>,
        OnBatchFn: Fn(Batch, Option<SmartStreamRuntimeError>) -> Fut,
    {
        let mut smartmodule_batch = Batch::<MemoryRecords>::default();
        smartmodule_batch.base_offset = -1; // indicate this is unitialized
        smartmodule_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut batch_bytes = 0;
        let mut total_records = 0;

        loop {
            let file_batch = match iter.next() {
                // we process entire batches.  entire batches are process as group
                Some(Ok(batch_result)) => batch_result,
                Some(Err(err)) => return Err(Error::new(err)),
                None => {
                    debug!(total_records, smartmodule_batch.batch_len, "No more batches, SmartModule end");
                    return on_batch(smartmodule_batch, None)
                        .await
                        .map(|(records, wait)| (total_records + records, wait));
                }
            };

            debug!(
                current_batch_offset = file_batch.batch.base_offset,
                current_batch_offset_delta = file_batch.offset_delta(),
                smartmodule_offset_delta = smartmodule_batch.get_header().last_offset_delta,
                smartmodule_base_offset = smartmodule_batch.base_offset,
                smartmodule_records = smartmodule_batch.records().len(),
                "Starting SmartModuleInstance processing"
            );

            let now = Instant::now();

            let mut join_record = vec![];
            join_last_record.encode(&mut join_record, 0)?;

            let input = SmartModuleInput {
                base_offset: file_batch.batch.base_offset,
                record_data: file_batch.records.clone(),
                join_record,
                params: self.params().clone(),
            };
            let output = self.process(input)?;
            debug!(smartmodule_execution_time = %now.elapsed().as_millis());

            let mut maybe_error = output.error;
            let mut records = output.successes;

            trace!("smartmodule processed records: {:#?}", records);

            // there are smartmoduleed records!!
            if records.is_empty() {
                debug!("smartmodules records empty");
            } else {
                // set base offset if this is first time
                if smartmodule_batch.base_offset == -1 {
                    smartmodule_batch.base_offset = file_batch.base_offset();
                }

                // difference between smartmodule batch and and current batch
                // since base are different we need update delta offset for each records
                let relative_base_offset = smartmodule_batch.base_offset - file_batch.base_offset();

                for record in &mut records {
                    record.add_base_offset(relative_base_offset);
                }

                let record_bytes = records.write_size(0);

                // if smartstream bytes exceed max bytes then we send this batch
                if batch_bytes + record_bytes > max_bytes {
                    debug!(
                        batch_bytes,
                        record_bytes,
                        max_bytes, "Max SmartModuleInstance bytes reached, sending records"
                    );

                    let last_offset= smartmodule_batch.get_last_offset();
                    let last_offset_delta= smartmodule_batch.get_last_offset_delta();
                    debug!(
                        ?smartmodule_batch,
                        "old batch"
                    );

                    let (records, _wait) = on_batch(smartstream_batch, maybe_error.take()).await?;
                    total_records += records;

                    smartstream_batch = Batch::<MemoryRecords>::default();
                    smartstream_batch.base_offset = last_offset + 1;
                    smartstream_batch.set_offset_delta(last_offset_delta as i32 + 1);

                    debug!(
                        ?smartstream_batch,
                        "new batch"
                    );
                    batch_bytes = 0;
                }
                
                batch_bytes += record_bytes;

                debug!(
                    smartmodule_records = records.len(),
                    total_bytes, "finished smartmoduleing"
                );
                smartmodule_batch.mut_records().append(&mut records);
            }

            // only increment smartmodule offset delta if smartmodule_batch has been initialized
            if smartmodule_batch.base_offset != -1 {
                debug!(
                    offset_delta = file_batch.offset_delta(),
                    "adding to offset delta"
                );
                smartmodule_batch.add_to_offset_delta(file_batch.offset_delta() + 1);
            }

            // If we had a processing error, return current batch and error
            if maybe_error.is_some() {
                return on_batch(smartmodule_batch, maybe_error).await;
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
