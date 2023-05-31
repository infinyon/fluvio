/// Implementations of the public API
mod engine;
pub use engine::{initialize_imp, SmartEngineImp, SmartModuleChainInstanceImp};
/// Implementations of the traits in `common` for the Wasmtime engine
mod instance;
#[cfg(test)]
pub use instance::WasmtimeFn as WasmFnImp;
mod memory;
mod state;
