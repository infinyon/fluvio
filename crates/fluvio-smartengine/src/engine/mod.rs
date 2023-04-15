pub mod metrics;
/// SmartModule configuration
mod config;
pub use config::{
    SmartModuleConfig, SmartModuleConfigBuilder, SmartModuleConfigBuilderError,
    SmartModuleInitialData,
};
mod error;

#[cfg(test)]
mod fixture;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

mod wasmtime_engine;
pub use wasmtime_engine::{SmartEngine, SmartModuleChainBuilder, SmartModuleChainInstance};
