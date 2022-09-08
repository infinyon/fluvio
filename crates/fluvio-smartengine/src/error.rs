#[derive(thiserror::Error, Debug)]
pub enum EngineError {
    #[error("Function Ty failed conversion {0}: {1}")]
    TypeConversion(&'static str, anyhow::Error),
    #[error("No valid smartmodule found")]
    UnknownSmartModule,
    #[error("Failed to instantiate: {0}")]
    Instantiate(anyhow::Error),
}
