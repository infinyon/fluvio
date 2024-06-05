use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use clap::Parser;

use crate::cli::ClusterCliError;

use fluvio::config::{TlsPolicy, TlsPaths};
use fluvio_controlplane_metadata::spg::{SpuConfig, StorageConfig};
use fluvio_types::defaults::{TLS_SERVER_SECRET_NAME, TLS_CLIENT_SECRET_NAME};

#[derive(Debug, Parser)]
pub struct SpuCliConfig {
    /// set spu storage size
    #[arg(long, default_value = "10")]
    pub spu_storage_size: u16,
}

impl SpuCliConfig {
    pub fn as_spu_config(&self) -> SpuConfig {
        SpuConfig {
            storage: Some(StorageConfig {
                size: Some(format!("{}Gi", self.spu_storage_size)),
                ..Default::default()
            }),
            ..Default::default()
        }
    }
}

#[derive(Debug, Parser)]
pub struct K8Install {
    /// k8: use specific chart version
    #[arg(long)]
    pub chart_version: Option<semver::Version>,

    /// k8: use specific image version
    #[arg(long)]
    pub image_version: Option<String>,

    /// k8: use custom docker registry
    #[arg(long)]
    pub registry: Option<String>,

    /// k8 namespace
    #[arg(long, default_value = "default")]
    pub namespace: String,

    /// k8
    #[arg(long, default_value = "main")]
    pub group_name: String,

    /// helm chart installation name
    #[arg(long, default_value = "fluvio")]
    pub install_name: String,

    /// Local path to a helm chart to install
    #[arg(long)]
    pub chart_location: Option<String>,

    /// chart values
    #[arg(long)]
    pub chart_values: Vec<PathBuf>,

    /// Uses port forwarding for connecting to SC (only during install)
    ///
    /// For connecting to a cluster during and after install, --proxy-addr <IP or DNS> is recommended
    #[arg(long)]
    pub use_k8_port_forwarding: bool,

    /// Config option used in kubernetes deployments
    #[arg(long, hide = true)]
    pub use_cluster_ip: bool,

    /// TLS: Client secret name while adding to Kubernetes
    #[arg(long, default_value = TLS_CLIENT_SECRET_NAME)]
    pub tls_client_secret_name: String,

    /// TLS: Server secret name while adding to Kubernetes
    #[arg(long, default_value = TLS_SERVER_SECRET_NAME)]
    pub tls_server_secret_name: String,
}

/// Manage and view Fluvio clusters
#[derive(Debug, Parser)]
pub struct ClusterConnectionOpts {
    /// SC public address
    #[arg(long)]
    pub sc_pub_addr: Option<String>,

    /// SC private address
    #[arg(long)]
    pub sc_priv_addr: Option<String>,

    #[clap(flatten)]
    pub tls: TlsOpt,

    #[arg(long)]
    pub authorization_config_map: Option<String>,

    /// Proxy address
    #[arg(long)]
    pub proxy_addr: Option<String>,
}

#[derive(Debug, Parser)]
pub struct TlsOpt {
    /// Whether to use TLS
    #[arg(long)]
    pub tls: bool,

    /// TLS: domain
    #[arg(long)]
    pub domain: Option<String>,

    /// TLS: ca cert
    #[arg(long)]
    pub ca_cert: Option<PathBuf>,

    /// TLS: client cert
    #[arg(long)]
    pub client_cert: Option<PathBuf>,

    /// TLS: client key
    #[arg(long)]
    pub client_key: Option<PathBuf>,

    /// TLS: path to server certificate
    #[arg(long)]
    pub server_cert: Option<PathBuf>,

    /// TLS: path to server private key
    #[arg(long)]
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
