use std::path::PathBuf;
use tracing::debug;
use structopt::StructOpt;
use fluvio::config::{TlsPolicy, TlsPaths};

#[derive(Debug, StructOpt)]
pub struct TlsOpt {
    /// tls
    #[structopt(long)]
    pub tls: bool,

    /// TLS: domain
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// TLS: path to server certificate
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub server_cert: Option<PathBuf>,

    /// TLS: path to server private key
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub server_key: Option<PathBuf>,

    /// TLS: client cert
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_cert: Option<PathBuf>,

    /// TLS: client key
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub client_key: Option<PathBuf>,

    /// TLS: ca cert
    #[structopt(long, required_if("tls", "true"), parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,
}

impl From<TlsOpt> for (TlsPolicy, TlsPolicy) {
    /// Returns (Client TLS Policy, Server TLS Policy)
    fn from(opt: TlsOpt) -> Self {
        if !opt.tls {
            debug!("no optional tls");
            return (TlsPolicy::Disabled, TlsPolicy::Disabled);
        }

        // Since opt.tls is true, the following args are required, so we can unwrap safely.
        let server_cert = opt.server_cert.unwrap();
        let server_key = opt.server_key.unwrap();
        let client_cert = opt.client_cert.unwrap();
        let client_key = opt.client_key.unwrap();
        let ca_cert = opt.ca_cert.unwrap();
        let domain = opt.domain.unwrap();

        let server_policy = TlsPolicy::from(TlsPaths {
            domain: domain.clone(),
            ca_cert: ca_cert.clone(),
            cert: server_cert,
            key: server_key,
        });

        let client_policy = TlsPolicy::from(TlsPaths {
            domain,
            ca_cert,
            cert: client_cert,
            key: client_key,
        });

        (client_policy, server_policy)
    }
}
