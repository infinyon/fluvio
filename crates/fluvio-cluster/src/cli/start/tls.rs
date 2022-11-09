use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use clap::Parser;

use fluvio::config::{TlsPolicy, TlsPaths};

use crate::cli::ClusterCliError;

#[derive(Debug, Parser)]
pub struct TlsOpt {
    /// Whether to use TLS
    #[clap(long)]
    pub tls: bool,

    /// TLS: domain
    #[clap(long)]
    pub domain: Option<String>,

    /// TLS: ca cert
    #[clap(long, value_parser)]
    pub ca_cert: Option<PathBuf>,

    /// TLS: client cert
    #[clap(long, value_parser)]
    pub client_cert: Option<PathBuf>,

    /// TLS: client key
    #[clap(long, value_parser)]
    pub client_key: Option<PathBuf>,

    /// TLS: path to server certificate
    #[clap(long, value_parser)]
    pub server_cert: Option<PathBuf>,

    /// TLS: path to server private key
    #[clap(long, value_parser)]
    pub server_key: Option<PathBuf>,
}

impl TryFrom<TlsOpt> for (TlsPolicy, TlsPolicy) {
    type Error = ClusterCliError;

    /// Returns (Client TLS Policy, Server TLS Policy)
    fn try_from(opt: TlsOpt) -> Result<Self, Self::Error> {
        if !opt.tls {
            debug!("no optional tls");
            return Ok((TlsPolicy::Disabled, TlsPolicy::Disabled));
        }

        // Use self-executing closure to strip out Options nicely
        let policies = (|| -> Option<(TlsPolicy, TlsPolicy)> {
            let domain = opt.domain?;
            let ca_cert = opt.ca_cert?;
            let client_cert = opt.client_cert?;
            let client_key = opt.client_key?;
            let server_cert = opt.server_cert?;
            let server_key = opt.server_key?;

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

            Some((client_policy, server_policy))
        })();

        policies.ok_or_else(|| {
            ClusterCliError::Other(
                "Missing required args after --tls:\
  --domain, --ca-cert, --client-cert, --client-key, --server-cert, --server-key"
                    .to_string(),
            )
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::convert::TryInto;

    #[test]
    fn test_from_opt() {
        let tls_opt = TlsOpt::parse_from(vec![
            "test", // First arg is treated as binary name
            "--tls",
            "--domain",
            "fluvio.io",
            "--ca-cert",
            "/tmp/certs/ca.crt",
            "--client-cert",
            "/tmp/certs/client.crt",
            "--client-key",
            "/tmp/certs/client.key",
            "--server-cert",
            "/tmp/certs/server.crt",
            "--server-key",
            "/tmp/certs/server.key",
        ]);
        let (client, server): (TlsPolicy, TlsPolicy) = tls_opt.try_into().unwrap();

        use fluvio::config::{TlsPolicy::*, TlsConfig::*};
        match (client, server) {
            (Verified(Files(client_paths)), Verified(Files(server_paths))) => {
                // Client checks
                assert_eq!(client_paths.domain, "fluvio.io");
                assert_eq!(client_paths.ca_cert, PathBuf::from("/tmp/certs/ca.crt"));
                assert_eq!(client_paths.cert, PathBuf::from("/tmp/certs/client.crt"));
                assert_eq!(client_paths.key, PathBuf::from("/tmp/certs/client.key"));

                // Server checks
                assert_eq!(server_paths.domain, "fluvio.io");
                assert_eq!(server_paths.ca_cert, PathBuf::from("/tmp/certs/ca.crt"));
                assert_eq!(server_paths.cert, PathBuf::from("/tmp/certs/server.crt"));
                assert_eq!(server_paths.key, PathBuf::from("/tmp/certs/server.key"));
            }
            _ => panic!("Failed to parse TlsProfiles from TlsOpt"),
        }
    }

    #[test]
    fn test_missing_opts() {
        let tls_opt: TlsOpt = TlsOpt::parse_from(vec![
            "test", // First arg is treated as binary name
            "--tls",
        ]);

        let result: Result<(TlsPolicy, TlsPolicy), _> = tls_opt.try_into();
        assert!(result.is_err());
    }
}
