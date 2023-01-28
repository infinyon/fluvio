mod spu;
mod sc;

pub use spu::*;
pub use sc::*;
pub use process::*;

pub use error::*;

mod error {

    use std::io::Error as IoError;

    use fluvio_command::{CommandError};

    #[derive(thiserror::Error, Debug)]
    pub enum LocalRuntimeError {
        #[error(transparent)]
        IoError(#[from] IoError),
        /// Failed to execute a command
        #[error(transparent)]
        CommandError(#[from] CommandError),
        /// Attempted to launch local cluster without fluvio-run
        #[error("Local cluster requires the fluvio-run plugin to be installed")]
        MissingFluvioRunner,
        /// A different kind of error occurred.
        #[error("An unknown error occurred: {0}")]
        Other(String),
    }
}

mod process {

    use std::{borrow::Cow, process::Command};

    use tracing::{info, instrument};

    use fluvio::config::{TlsConfig, TlsPaths};

    use crate::tls_config_to_cert_paths;

    use super::LocalRuntimeError;

    pub trait FluvioLocalProcess {
        #[instrument(skip(self, cmd, tls, port))]
        fn set_server_tls(
            &self,
            cmd: &mut Command,
            tls: &TlsConfig,
            port: u16,
        ) -> Result<(), LocalRuntimeError> {
            let paths: Cow<TlsPaths> = tls_config_to_cert_paths(tls)?;

            info!("starting SC with TLS options");
            let ca_cert = paths.ca_cert.to_str().ok_or_else(|| {
                LocalRuntimeError::Other("ca_cert must be a valid path".to_string())
            })?;
            let server_cert = paths.cert.to_str().ok_or_else(|| {
                LocalRuntimeError::Other("server_cert must be a valid path".to_string())
            })?;
            let server_key = paths.key.to_str().ok_or_else(|| {
                LocalRuntimeError::Other("server_key must be a valid path".to_string())
            })?;
            cmd.arg("--tls")
                .arg("--enable-client-cert")
                .arg("--server-cert")
                .arg(server_cert)
                .arg("--server-key")
                .arg(server_key)
                .arg("--ca-cert")
                .arg(ca_cert)
                .arg("--bind-non-tls-public")
                .arg(format!("0.0.0.0:{port}"));
            Ok(())
        }
    }
}
