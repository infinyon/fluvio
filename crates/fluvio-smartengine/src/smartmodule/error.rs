#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("Failed to get valid exports for {0}: {1}")]
    NotNamedExport(&'static str, anyhow::Error),
    #[error("Failed to get valid exports for any kind of smartmodule.")]
    NotValidExports,
    #[error("Failed to instantiate: {0}")]
    Instantiate(anyhow::Error),
}
