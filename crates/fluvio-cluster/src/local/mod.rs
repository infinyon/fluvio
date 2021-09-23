mod spu;
mod sc;

pub use spu::*;
pub use sc::*;
pub use process::*;

mod process {

    use std::{borrow::Cow, process::Command};

    use tracing::{info, instrument};

    use fluvio::config::{TlsConfig, TlsPaths};

    use crate::{LocalInstallError};

    pub trait FluvioProcess {
        #[instrument(skip(self, cmd, tls, port))]
        fn set_server_tls(
            &self,
            cmd: &mut Command,
            tls: &TlsConfig,
            port: u16,
        ) -> Result<(), LocalInstallError> {
            let paths: Cow<TlsPaths> = match tls {
                TlsConfig::Files(paths) => Cow::Borrowed(paths),
                TlsConfig::Inline(certs) => Cow::Owned(certs.try_into_temp_files()?),
            };

            info!("starting SC with TLS options");
            let ca_cert = paths.ca_cert.to_str().ok_or_else(|| {
                LocalInstallError::Other("ca_cert must be a valid path".to_string())
            })?;
            let server_cert = paths.cert.to_str().ok_or_else(|| {
                LocalInstallError::Other("server_cert must be a valid path".to_string())
            })?;
            let server_key = paths.key.to_str().ok_or_else(|| {
                LocalInstallError::Other("server_key must be a valid path".to_string())
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
                .arg(format!("0.0.0.0:{}", port));
            Ok(())
        }
    }
}
