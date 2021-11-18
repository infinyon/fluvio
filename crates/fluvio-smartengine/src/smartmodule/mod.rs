use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::fmt::{self, Debug};

use dataplane::record::Record;
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
use fluvio_spu_schema::server::stream_fetch::{SmartModuleKind, SmartModulePayload};

mod memory;
pub mod filter;
pub mod map;
pub mod array_map;
pub mod filter_map;
pub mod aggregate;
pub mod join;
pub mod file_batch;
pub mod join_stream;

pub type WasmSlice = (i32, i32);
#[cfg(feature = "smartmodule")]
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec};

use self::join_stream::SmartModuleJoinStream;

#[derive(Default, Clone)]
pub struct SmartEngine(pub(crate) Engine);

impl SmartEngine {
    #[cfg(feature = "smartmodule")]
    pub fn create_module_from_smartmodule_spec(
        self,
        spec: &SmartModuleSpec,
    ) -> Result<SmartModuleModule> {
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
        Ok(SmartModuleModule {
            module,
            engine: self,
        })
    }

    pub fn create_module_from_binary(self, bytes: &[u8]) -> Result<SmartModuleModule> {
        let module = Module::from_binary(&self.0, bytes)?;
        Ok(SmartModuleModule {
            module,
            engine: self,
        })
    }
    pub fn create_module_from_payload(
        self,
        smart_payload: SmartModulePayload,
    ) -> Result<Box<dyn SmartModuleInstance>> {
        let smartmodule = self.create_module_from_binary(&smart_payload.wasm.get_raw()?)?;
        let smart_stream: Box<dyn SmartModuleInstance> = match &smart_payload.kind {
            SmartModuleKind::Filter => Box::new(smartmodule.create_filter(smart_payload.params)?),
            SmartModuleKind::FilterMap => {
                Box::new(smartmodule.create_filter_map(smart_payload.params)?)
            }
            SmartModuleKind::Map => Box::new(smartmodule.create_map(smart_payload.params)?),
            SmartModuleKind::ArrayMap => {
                Box::new(smartmodule.create_array_map(smart_payload.params)?)
            }
            SmartModuleKind::Join(_) => Box::new(smartmodule.create_join(smart_payload.params)?),
            SmartModuleKind::JoinStream {
                topic: _,
                smartstream: _,
            } => Box::new(smartmodule.create_join_stream(smart_payload.params)?),
            SmartModuleKind::Aggregate { accumulator } => {
                Box::new(smartmodule.create_aggregate(smart_payload.params, accumulator.clone())?)
            }
        };
        Ok(smart_stream)
    }
}

impl Debug for SmartEngine {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleEngine")
    }
}

pub struct SmartModuleModule {
    pub(crate) module: Module,
    pub(crate) engine: SmartEngine,
}

impl SmartModuleModule {
    fn create_filter(&self, params: SmartModuleExtraParams) -> Result<SmartModuleFilter> {
        let filter = SmartModuleFilter::new(&self.engine, self, params)?;
        Ok(filter)
    }

    fn create_map(&self, params: SmartModuleExtraParams) -> Result<SmartModuleMap> {
        let map = SmartModuleMap::new(&self.engine, self, params)?;
        Ok(map)
    }

    fn create_filter_map(&self, params: SmartModuleExtraParams) -> Result<SmartModuleFilterMap> {
        let filter_map = SmartModuleFilterMap::new(&self.engine, self, params)?;
        Ok(filter_map)
    }

    fn create_array_map(&self, params: SmartModuleExtraParams) -> Result<SmartModuleArrayMap> {
        let map = SmartModuleArrayMap::new(&self.engine, self, params)?;
        Ok(map)
    }

    fn create_join(&self, params: SmartModuleExtraParams) -> Result<SmartModuleJoin> {
        let join = SmartModuleJoin::new(&self.engine, self, params)?;
        Ok(join)
    }

    fn create_join_stream(&self, params: SmartModuleExtraParams) -> Result<SmartModuleJoinStream> {
        let join = SmartModuleJoinStream::new(&self.engine, self, params)?;
        Ok(join)
    }

    fn create_aggregate(
        &self,
        params: SmartModuleExtraParams,
        accumulator: Vec<u8>,
    ) -> Result<SmartModuleAggregate> {
        let aggregate = SmartModuleAggregate::new(&self.engine, self, params, accumulator)?;
        Ok(aggregate)
    }
}

pub struct SmartModuleContext {
    store: Store<()>,
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
}

impl SmartModuleContext {
    pub fn new(
        engine: &SmartEngine,
        module: &SmartModuleModule,
        params: SmartModuleExtraParams,
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
        })
    }

    pub fn write_input<E: Encoder>(&mut self, input: &E, version: i16) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, version)?;
        debug!(len = input_data.len(), "input data");
        let array_ptr =
            self::memory::copy_memory_to_instance(&mut self.store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32))
    }

    pub fn read_output<D: Decoder + Default>(&mut self, version: i16) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(&mut self.store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), version)?;
        Ok(output)
    }
}

pub trait SmartModuleInstance: Send + Sync {
    fn process(&mut self, input: SmartModuleInput) -> Result<SmartModuleOutput>;
    fn params(&self) -> SmartModuleExtraParams;
}

impl dyn SmartModuleInstance + '_ {
    #[instrument(skip(self, iter, max_bytes, join_last_record))]
    pub fn process_batch(
        &mut self,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        join_last_record: Option<&Record>,
    ) -> Result<(Batch, Option<SmartModuleRuntimeError>), Error> {
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
                        "No more batches, SmartModuleInstance end"
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
                        max_bytes, "Total SmartModuleInstance bytes reached"
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
