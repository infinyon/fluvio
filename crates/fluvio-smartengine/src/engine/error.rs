#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("No valid smartmodule found")]
    UnknownSmartModule,
    #[error("Failed to instantiate: {0}")]
    Instantiate(anyhow::Error),
    #[error("Requested memory {requested}b exceeded max allowed {max}b")]
    StoreMemoryExceeded {
        current: usize,
        requested: usize,
        max: usize,
    },
}
