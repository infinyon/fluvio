use fluvio_protocol::{Decoder, Encoder};
use tracing::debug;
use wasmedge_sdk::error::HostFuncError;
use wasmedge_sdk::types::Val;
use wasmedge_sdk::{
    host_function, Caller, CallingFrame, Engine, Executor, Func, ImportObjectBuilder, Instance,
    Memory, Module, Store, WasmValue,
};

use super::{WasmedgeInstance, WasmedgeContext};
use crate::engine::common::SmartModuleInit;
use crate::engine::common::DowncastableTransform;
use crate::engine::error::EngineError;
use crate::metrics::SmartModuleChainMetrics;
use crate::engine::wasmedge::memory;
use crate::engine::{config::*, WasmSlice};
use anyhow::Result;
use fluvio_smartmodule::dataplane::smartmodule::{
    SmartModuleInput, SmartModuleOutput, SmartModuleInitInput, SmartModuleExtraParams,
};
use std::any::Any;
use std::fmt::{self, Debug};
use std::sync::{Arc, Mutex};
use super::WasmedgeFn;

use super::Init;

pub(crate) struct SmartModuleInstance {
    pub instance: WasmedgeInstance,
    pub transform: Box<dyn DowncastableTransform<WasmedgeInstance>>,
    pub init: Option<Init>,
}

impl SmartModuleInstance {
    #[cfg(test)]
    #[allow(clippy::borrowed_box)]
    pub(crate) fn transform(&self) -> &Box<dyn DowncastableTransform<WasmedgeInstance>> {
        &self.transform
    }

    #[cfg(test)]
    pub(crate) fn get_init(&self) -> &Option<Init> {
        &self.init
    }

    pub(crate) fn new(
        instance: WasmedgeInstance,
        init: Option<Init>,
        transform: Box<dyn DowncastableTransform<WasmedgeInstance>>,
    ) -> Self {
        Self {
            instance,
            init,
            transform,
        }
    }

    pub(crate) fn process(
        &mut self,
        input: SmartModuleInput,
        ctx: &mut WasmedgeContext,
    ) -> Result<SmartModuleOutput> {
        self.transform.process(input, &mut self.instance, ctx)
    }

    pub fn init(&mut self, ctx: &mut WasmedgeContext) -> Result<()> {
        if let Some(init) = &mut self.init {
            let input = SmartModuleInitInput {
                params: self.instance.params.clone(),
            };
            init.initialize(input, &mut self.instance, ctx)
        } else {
            Ok(())
        }
    }
}

// TODO: revise later to see whether Clone is necessary
#[derive(Clone)]
pub(crate) struct RecordsMemory {
    pub ptr: u32,
    pub len: u32,
    pub memory: Memory,
}

impl RecordsMemory {
    pub(crate) fn copy_memory_from(&self) -> Result<Vec<u8>> {
        let bytes = self.memory.read(self.ptr, self.len)?;
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
