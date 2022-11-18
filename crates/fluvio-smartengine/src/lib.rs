cfg_if::cfg_if! {
    if #[cfg(feature = "engine")] {
        pub(crate) mod memory;
        pub(crate) mod transforms;
        pub(crate) mod init;
        pub(crate) mod error;
        pub mod metrics;
        mod engine;
        mod state;
        pub use engine::*;
        pub mod instance;
    }
}
#[cfg(feature = "transformation")]
pub mod transformation;

pub type WasmSlice = (i32, i32, u32);
pub type Version = i16;

#[cfg(test)]
mod fixture;
