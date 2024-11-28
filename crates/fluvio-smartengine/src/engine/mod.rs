//! SmartModule configuration

mod config;
mod error;
mod wasmtime;

#[cfg(test)]
mod fixture;

pub mod metrics;

pub use error::EngineError;
pub use config::{
    SmartModuleConfig, SmartModuleConfigBuilder, SmartModuleConfigBuilderError,
    SmartModuleInitialData, Lookback, DEFAULT_SMARTENGINE_VERSION,
};

pub type WasmSlice = (i32, i32, u32);

/// SmartEngine Version
pub type Version = i16;

pub use self::wasmtime::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};
