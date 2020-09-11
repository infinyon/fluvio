use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::convert::TryInto;

use tracing::debug;
use structopt::StructOpt;

use fluvio::config::{TlsPolicy, TlsConfig, TlsPaths};

/// Optional Tls Configuration to Client
#[derive(Debug, StructOpt, Default, Clone)]
pub struct TlsOpt {
    /// Enable TLS
    #[structopt(long)]
    pub tls: bool,

    /// Required if client cert is used
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// TLS: use client cert
    #[structopt(long)]
    pub enable_client_cert: bool,
    /// Path to TLS client certificate
    #[structopt(long, parse(from_os_str))]
    pub client_cert: Option<PathBuf>,
    /// Path to TLS client private key
    #[structopt(long, parse(from_os_str))]
    pub client_key: Option<PathBuf>,
    /// Path to TLS ca cert, required when client cert is enabled
    #[structopt(long, parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,
}

impl TryInto<TlsPolicy> for TlsOpt {
    type Error = IoError;

    fn try_into(self) -> Result<TlsPolicy, Self::Error> {
        match (self.client_cert, self.client_key, self.ca_cert, self.domain) {
            _ if !self.tls => {
                debug!("no optional tls");
                Ok(TlsPolicy::Disabled)
            }
            _ if !self.enable_client_cert => {
                debug!("using no cert verification");
                Ok(TlsPolicy::Anonymous)
            }
            (Some(client_cert), Some(client_key), Some(ca_cert), Some(domain)) => {
                debug!("using tls and client cert");
                Ok(TlsPolicy::Verified(TlsConfig::Files(TlsPaths {
                    cert: client_cert,
                    key: client_key,
                    ca_cert,
                    domain,
                })))
            }
            (None, _, _, _) => Err(IoError::new(
                ErrorKind::InvalidInput,
                "client cert is missing".to_owned(),
            )),
            (_, None, _, _) => Err(IoError::new(
                ErrorKind::InvalidInput,
                "client private key is missing".to_owned(),
            )),
            (_, _, None, _) => Err(IoError::new(
                ErrorKind::InvalidInput,
                "CA cert is missing".to_owned(),
            )),
            (_, _, _, None) => Err(IoError::new(
                ErrorKind::InvalidInput,
                "domain is missing".to_owned(),
            )),
        }
    }
}

// impl TlsOpt {
//     pub fn try_into_inline(self) -> Result<Option<TlsConfig>, IoError> {
//         let maybe_tls_paths: Option<TlsConfigPaths> = self.try_into()?;
//         let maybe_tls_config = match maybe_tls_paths {
//             None => None,
//             Some(tls_paths) => Some(tls_paths.try_into()?),
//         };
//         Ok(maybe_tls_config)
//     }
// }
