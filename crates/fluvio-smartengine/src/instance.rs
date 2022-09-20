use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::fmt::{self, Debug};

use tracing::{debug, instrument, trace};
use anyhow::{Error, Result};
use wasmtime::{Memory, Module, Caller, Extern, Trap, Instance, Func, AsContextMut};

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::record::Record;
use fluvio_protocol::record::{Batch, MemoryRecords};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput, SmartModuleInitInput,
    SmartModuleInitOutput,
};
use fluvio_protocol::link::smartmodule::SmartModuleRuntimeError;

use crate::error::EngineError;
use crate::init::SmartModuleInit;
use crate::{WasmSlice, memory, SmartModuleChainBuilder, State, WasmState};
use crate::file_batch::FileBatchIterator;

pub(crate) struct SmartModuleInstance {
    ctx: SmartModuleInstanceContext,
    init: Option<SmartModuleInit>,
    transform: Box<dyn SmartModuleTransform>,
}

impl SmartModuleInstance {
    #[cfg(test)]
    pub(crate) fn transform(&self) -> &Box<dyn SmartModuleTransform> {
        &self.transform
    }

    pub(crate) fn new(
        ctx: SmartModuleInstanceContext,
        init: Option<SmartModuleInit>,
        transform: Box<dyn SmartModuleTransform>,
    ) -> Self {
        Self {
            ctx,
            init,
            transform,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput> {
        self.transform.process(input, &mut self.ctx, store)
    }

    // TODO: Move this to SPU
    #[instrument(skip(self, store, iter, max_bytes, join_last_record))]
    pub(crate) fn process_batch(
        &mut self,
        store: &mut WasmState,
        iter: &mut FileBatchIterator,
        max_bytes: usize,
        join_last_record: Option<&Record>,
    ) -> Result<(Batch, Option<SmartModuleRuntimeError>), Error> {
        let mut smartmodule_batch = Batch::<MemoryRecords>::default();
        smartmodule_batch.base_offset = -1; // indicate this is uninitialized
        smartmodule_batch.set_offset_delta(-1); // make add_to_offset_delta correctly

        let mut total_bytes = 0;

        loop {
            let file_batch = match iter.next() {
                // we process entire batches.  entire batches are process as group
                // if we can't fit current batch into max bytes then it is discarded
                Some(batch_result) => batch_result?,
                None => {
                    debug!(
                        total_records = smartmodule_batch.records().len(),
                        "No more batches, SmartModuleInstance end"
                    );
                    return Ok((smartmodule_batch, None));
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
                params: self.ctx.params.clone(),
            };
            let output = self.transform.process(input, &mut self.ctx, store)?;
            debug!(smartmodule_execution_time = %now.elapsed().as_millis());

            let maybe_error = output.error;
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

                // if smartmodule bytes exceed max bytes then we skip this batch
                if total_bytes + record_bytes > max_bytes {
                    debug!(
                        total_bytes = total_bytes + record_bytes,
                        max_bytes, "Total SmartModuleInstance bytes reached"
                    );
                    return Ok((smartmodule_batch, maybe_error));
                }

                total_bytes += record_bytes;

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
                return Ok((smartmodule_batch, maybe_error));
            }
        }
    }

    pub fn init(&mut self, store: &mut WasmState) -> Result<SmartModuleInitOutput, Error> {
        if let Some(init) = &mut self.init {
            let input = SmartModuleInitInput {
                params: self.ctx.params.clone(),
            };
            init.initialize(input, &mut self.ctx, store)
        } else {
            Ok(SmartModuleInitOutput::default())
        }
    }
}

pub(crate) struct SmartModuleInstanceContext {
    instance: Instance,
    records_cb: Arc<RecordsCallBack>,
    params: SmartModuleExtraParams,
    version: i16,
}

impl Debug for SmartModuleInstanceContext {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "SmartModuleInstanceBase")
    }
}

impl SmartModuleInstanceContext {
    /// instantiate new module instance that contain context
    #[tracing::instrument(skip(module, params, chain))]
    pub(crate) fn instantiate(
        module: Module,
        chain: &mut SmartModuleChainBuilder,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, EngineError> {
        debug!("creating WasmModuleInstance");
        let cb = Arc::new(RecordsCallBack::new());
        let records_cb = cb.clone();
        let copy_records_fn = move |mut caller: Caller<'_, State>, ptr: i32, len: i32| {
            debug!(len, "callback from wasm filter");
            let memory = match caller.get_export("memory") {
                Some(Extern::Memory(mem)) => mem,
                _ => return Err(Trap::new("failed to find host memory")),
            };

            let records = RecordsMemory { ptr, len, memory };
            cb.set(records);
            Ok(())
        };

        debug!("instantiating WASMtime");
        let instance = chain
            .instantiate(&module, copy_records_fn)
            .map_err(EngineError::Instantiate)?;
        Ok(Self {
            instance,
            records_cb,
            params,
            version,
        })
    }

    /// get wasm function from instance
    pub(crate) fn get_wasm_func(&self, store: &mut impl AsContextMut, name: &str) -> Option<Func> {
        self.instance.get_func(store, name)
    }

    pub(crate) fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        store: &mut impl AsContextMut,
    ) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(len = input_data.len(), "input data");
        let array_ptr = memory::copy_memory_to_instance(store, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as u32))
    }

    pub(crate) fn read_output<D: Decoder + Default>(
        &mut self,
        store: impl AsContextMut,
    ) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(store).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }
}

pub(crate) trait SmartModuleTransform: Send + Sync {
    /// transform records
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        store: &mut WasmState,
    ) -> Result<SmartModuleOutput>;

    /// return name of transform, this is used for identifying transform and debugging
    fn name(&self) -> &str;
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self, store: impl AsContextMut) -> Result<Vec<u8>> {
        let mut bytes = vec![0u8; self.len as u32 as usize];
        self.memory.read(store, self.ptr as usize, &mut bytes)?;
        Ok(bytes)
    }
}

pub struct RecordsCallBack(Mutex<Option<RecordsMemory>>);

impl RecordsCallBack {
    pub(crate) fn new() -> Self {
        Self(Mutex::new(None))
    }

    pub(crate) fn set(&self, records: RecordsMemory) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.replace(records);
    }

    pub(crate) fn clear(&self) {
        let mut write_inner = self.0.lock().unwrap();
        write_inner.take();
    }

    pub(crate) fn get(&self) -> Option<RecordsMemory> {
        let reader = self.0.lock().unwrap();
        reader.clone()
    }
}
