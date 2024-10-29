use wasmtime::ResourceLimiter;

use crate::engine::error::EngineError;

#[derive(Debug, Default)]
pub(crate) struct StoreResourceLimiter {
    pub memory_size: Option<usize>,
}

impl StoreResourceLimiter {
    pub(crate) fn set_memory_size(&mut self, memory_size: usize) -> &mut Self {
        self.memory_size = Some(memory_size);
        self
    }
}

impl ResourceLimiter for StoreResourceLimiter {
    fn memory_growing(
        &mut self,
        current: usize,
        desired: usize,
        maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        let allow = match self.memory_size {
            Some(limit) if desired > limit => false,
            _ => !matches!(maximum, Some(max) if desired > max),
        };
        if !allow {
            Err(EngineError::StoreMemoryExceeded {
                current,
                requested: desired,
                max: self.memory_size.or(maximum).unwrap_or_default(),
            }
            .into())
        } else {
            Ok(allow)
        }
    }

    fn table_growing(
        &mut self,
        _current: usize,
        _desired: usize,
        _maximum: Option<usize>,
    ) -> anyhow::Result<bool> {
        Ok(true)
    }
}
