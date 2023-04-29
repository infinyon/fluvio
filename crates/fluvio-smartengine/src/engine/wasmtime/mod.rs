/// Implementations of the public API
mod imp;
pub use imp::{SmartEngineImp, initialize_imp, SmartModuleChainInstanceImp};
/// Implementations of the traits in `common` for the Wasmtime engine
mod instance;
#[cfg(test)]
pub use instance::WasmtimeFn as WasmFnImp;
mod memory;
mod state;
