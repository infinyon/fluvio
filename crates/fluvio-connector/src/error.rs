use thiserror::Error;
use fluvio_extension_common::output::OutputError;
use fluvio::FluvioError;
use std::io::Error as IoError;
use serde_yaml::Error as SerdeError;

#[derive(Debug, Error)]
pub enum ConnectorError {
    #[error("Fluvio Error `{0:?}`")]
    Fluvio(#[from] FluvioError),

    #[error("Output Error `{0:?}`")]
    Output(#[from] OutputError),

    #[error("Io Error `{0:?}`")]
    Io(#[from] IoError),

    #[error("Serde Error `{0:?}`")]
    Serde(#[from] SerdeError),

}
