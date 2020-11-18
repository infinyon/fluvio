use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use structopt::StructOpt;

use fluvio::config::{TlsPolicy, TlsPaths};
use crate::CliError;

/// Optional Tls Configuration to Client
#[derive(Debug, StructOpt, Default, Clone)]
pub struct TlsClientOpt {
    /// Enable TLS
    #[structopt(long)]
    pub tls: bool,

    /// TLS: use client cert
    #[structopt(long)]
    pub enable_client_cert: bool,

    /// Required if client cert is used
    #[structopt(long)]
    pub domain: Option<String>,

    /// Path to TLS ca cert, required when client cert is enabled
    #[structopt(long, parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,

    /// Path to TLS client certificate
    #[structopt(long, parse(from_os_str))]
    pub client_cert: Option<PathBuf>,

    /// Path to TLS client private key
    #[structopt(long, parse(from_os_str))]
    pub client_key: Option<PathBuf>,
}

impl TryFrom<TlsClientOpt> for TlsPolicy {
    type Error = CliError;

    fn try_from(opt: TlsClientOpt) -> Result<Self, Self::Error> {
        if !opt.tls {
            debug!("no optional tls");
            return Ok(TlsPolicy::Disabled);
        }

        if !opt.enable_client_cert {
            debug!("using no cert verification");
            return Ok(TlsPolicy::Anonymous);
        }

        // Use self-executing closure to strip out Options nicely
        let policy = (|| -> Option<TlsPolicy> {
            let domain = opt.domain?;
            let ca_cert = opt.ca_cert?;
            let client_cert = opt.client_cert?;
            let client_key = opt.client_key?;

            let policy = TlsPolicy::from(TlsPaths {
                domain,
                ca_cert,
                cert: client_cert,
                key: client_key,
            });

            Some(policy)
        })();

        policy.ok_or_else(|| {
            CliError::Other(
                "Missing required args after --tls:\
  --domain, --ca-cert, --client-cert, --client-key"
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
        let tls_opt = TlsClientOpt::from_iter(vec![
            "test", // First arg is treated as binary name
            "--tls",
            "--enable-client-cert",
            "--domain",
            "fluvio.io",
            "--ca-cert",
            "/tmp/certs/ca.crt",
            "--client-cert",
            "/tmp/certs/client.crt",
            "--client-key",
            "/tmp/certs/client.key",
        ]);
        let policy: TlsPolicy = tls_opt.try_into().unwrap();

        use fluvio::config::{TlsPolicy::*, TlsConfig::*};
        match policy {
            Verified(Files(paths)) => {
                assert_eq!(paths.domain, "fluvio.io");
                assert_eq!(paths.ca_cert, PathBuf::from("/tmp/certs/ca.crt"));
                assert_eq!(paths.cert, PathBuf::from("/tmp/certs/client.crt"));
                assert_eq!(paths.key, PathBuf::from("/tmp/certs/client.key"));
            }
            _ => panic!("Failed to parse TlsPolicy from TlsClientOpt"),
        }
    }

    #[test]
    fn test_anonymous() {
        let tls_opt: TlsClientOpt = TlsClientOpt::from_iter(vec![
            "test", // First arg is treated as binary name
            "--tls",
        ]);

        let policy: TlsPolicy = tls_opt.try_into().unwrap();
        match policy {
            TlsPolicy::Anonymous => (),
            _ => panic!("Failed to parse TlsPolicy from TlsClientOpt"),
        }
    }

    #[test]
    fn test_missing_opts() {
        let tls_opt: TlsClientOpt = TlsClientOpt::from_iter(vec![
            "test", // First arg is treated as binary name
            "--tls",
            "--enable-client-cert",
        ]);

        let result: Result<TlsPolicy, _> = tls_opt.try_into();
        assert!(result.is_err());
    }
}
