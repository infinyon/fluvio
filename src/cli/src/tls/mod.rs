use std::path::PathBuf;

use tracing::debug;
use structopt::StructOpt;

use fluvio::config::{TlsPolicy, TlsPaths};

/// Optional Tls Configuration to Client
#[derive(Debug, StructOpt, Default, Clone)]
pub struct TlsClientOpt {
    /// Enable TLS
    #[structopt(long)]
    pub tls: bool,

    /// TLS: use client cert
    #[structopt(long, required_if("tls", "true"))]
    pub enable_client_cert: bool,

    /// Required if client cert is used
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// Path to TLS ca cert, required when client cert is enabled
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,

    /// Path to TLS client certificate
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_cert: Option<PathBuf>,

    /// Path to TLS client private key
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_key: Option<PathBuf>,
}

impl From<TlsClientOpt> for TlsPolicy {
    fn from(opt: TlsClientOpt) -> Self {
        if !opt.tls {
            debug!("no optional tls");
            return TlsPolicy::Disabled;
        }

        if !opt.enable_client_cert {
            debug!("using no cert verification");
            return TlsPolicy::Anonymous;
        }

        // Since opt.tls is true, the following args are required, so we can unwrap safely.
        let client_cert = opt.client_cert.unwrap();
        let client_key = opt.client_key.unwrap();
        let ca_cert = opt.ca_cert.unwrap();
        let domain = opt.domain.unwrap();

        TlsPolicy::from(TlsPaths {
            domain,
            ca_cert,
            cert: client_cert,
            key: client_key,
        })
    }
}
