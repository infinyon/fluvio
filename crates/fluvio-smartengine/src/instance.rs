use std::sync::{Arc, Mutex};
use std::time::Instant;
use std::fmt::{self, Debug};

use tracing::{debug, instrument, trace};
use anyhow::{Error, Result};
use wasmtime::{
    Memory, Module, Caller, Extern, Trap, Instance, Store, Func, TypedFunc, WasmParams,
    WasmResults, AsContextMut, AsContext,
};

use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::record::Record;
use fluvio_protocol::record::{Batch, MemoryRecords};
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleExtraParams, SmartModuleInput, SmartModuleOutput,
};
use fluvio_protocol::link::smartmodule::SmartModuleRuntimeError;

use crate::{WasmSlice, memory, SmartModuleChain, State};
use crate::file_batch::FileBatchIterator;

use super::error;

pub(crate) struct SmartModuleInstance<T> {
    ctx: SmartModuleInstanceContext,
    transform: T,
}

impl<T> SmartModuleInstance<T>
where
    T: SmartModuleTransform,
{
    #[instrument(skip(self, iter, max_bytes, join_last_record,chain))]
    pub fn process_batch(
        &mut self,
        ctx: &mut SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
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
            let output = self.transform.process(input,ctx,chain)?;
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
        chain: &mut SmartModuleChain,
        params: SmartModuleExtraParams,
        version: i16,
    ) -> Result<Self, error::Error> {
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
            .map_err(error::Error::Instantiate)?;
        Ok(Self {
            instance,
            records_cb,
            params,
            version,
        })
    }

    /// get wasm function from instance
    pub(crate) fn get_wasm_func<'a,'b>(
        &'a self,
        chain: &'b mut SmartModuleChain,
        name: &str,
    ) -> Option<WasmFunction<'b>> {
        self.instance
            .get_func(chain.as_context_mut(), name)
            .map(|func| WasmFunction { func, chain })
    }

    pub fn write_input<E: Encoder>(
        &mut self,
        input: &E,
        chain: &mut SmartModuleChain,
    ) -> Result<WasmSlice> {
        self.records_cb.clear();
        let mut input_data = Vec::new();
        input.encode(&mut input_data, self.version)?;
        debug!(len = input_data.len(), "input data");
        let array_ptr = memory::copy_memory_to_instance(chain, &self.instance, &input_data)?;
        let length = input_data.len();
        Ok((array_ptr as i32, length as i32, self.version as u32))
    }

    pub fn read_output<D: Decoder + Default>(&mut self, chain: &mut SmartModuleChain) -> Result<D> {
        let bytes = self
            .records_cb
            .get()
            .and_then(|m| m.copy_memory_from(chain).ok())
            .unwrap_or_default();
        let mut output = D::default();
        output.decode(&mut std::io::Cursor::new(bytes), self.version)?;
        Ok(output)
    }

    /// initialize smartmodule instance using parameters
    /// it must be have fn called init with parameters
    /// this is experimental feature and may be removed in future
    #[instrument(skip(chain))]
    pub fn invoke_constructor(&mut self,chain: &mut SmartModuleChain) -> Result<(), Error> {
        use wasmtime::TypedFunc;
        use fluvio_smartmodule::dataplane::smartmodule::SmartModuleInternalError;

        const INIT_FN_NAME: &str = "init";

        let params = self.params.clone();
        let slice = self.write_input(&params,chain)?;
        if let Some(func) = self.get_wasm_func(chain, INIT_FN_NAME) {
            let init_fn: TypedFunc<(i32, i32, u32), i32> = func.typed()?;
            let filter_output = init_fn.call(chain.as_context_mut(), slice)?;
            if filter_output < 0 {
                let internal_error = SmartModuleInternalError::try_from(filter_output)
                    .unwrap_or(SmartModuleInternalError::UnknownError);
                return Err(internal_error.into());
            }

        } else {
            debug!("smartmodule does not have init function");
        }
        

        Ok(())
    }

}

pub(crate) struct WasmFunction<'a> {
    func: Func,
    chain: &'a SmartModuleChain,
}

impl<'a> WasmFunction<'a> {
    pub fn typed<Params, Results>(&self) -> Result<TypedFunc<Params, Results>>
    where
        Params: WasmParams,
        Results: WasmResults,
    {
        self.func.typed(self.chain.as_context())
    }
}

pub(crate) trait SmartModuleTransform {
    fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut SmartModuleInstanceContext,
        chain: &mut SmartModuleChain,
    ) -> Result<SmartModuleOutput>;
}

#[derive(Clone)]
pub struct RecordsMemory {
    ptr: i32,
    len: i32,
    memory: Memory,
}

impl RecordsMemory {
    fn copy_memory_from(&self, store: &mut Store<State>) -> Result<Vec<u8>> {
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
