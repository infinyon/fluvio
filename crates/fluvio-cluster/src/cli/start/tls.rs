use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use clap::Parser;

use fluvio::config::{TlsPolicy, TlsPaths};

use crate::cli::ClusterCliError;
use super::kube_config::{read_kube_config, KubeConfigError};

#[derive(Debug, Parser)]
pub struct TlsOpt {
    /// Whether to use TLS
    #[clap(long)]
    pub tls: bool,

    /// TLS: domain
    #[clap(long)]
    pub domain: Option<String>,

    /// TLS: ca cert
    #[clap(long, parse(from_os_str))]
    pub ca_cert: Option<PathBuf>,

    /// TLS: client cert
    #[clap(long, parse(from_os_str))]
    pub client_cert: Option<PathBuf>,

    /// TLS: client key
    #[clap(long, parse(from_os_str))]
    pub client_key: Option<PathBuf>,

    /// TLS: path to server certificate
    #[clap(long, parse(from_os_str))]
    pub server_cert: Option<PathBuf>,

    /// TLS: path to server private key
    #[clap(long, parse(from_os_str))]
    pub server_key: Option<PathBuf>,
}

impl TryFrom<TlsOpt> for (TlsPolicy, TlsPolicy) {
    type Error = ClusterCliError;

    /// Returns (Client TLS Policy, Server TLS Policy)
    fn try_from(opt: TlsOpt) -> Result<Self, Self::Error> {
        if !opt.tls {
            debug!("No TLS");
            return Ok((TlsPolicy::Disabled, TlsPolicy::Disabled));
        }

        // If ca_cert, client_cert or client_key were not specified, read them from the kubeconfig.
        let mut ca_cert = opt.ca_cert;
        let mut client_cert = opt.client_cert;
        let mut client_key = opt.client_key;

        // Only read the kubeconfig if at least one of the optional files is not specified.
        if [&ca_cert, &client_cert, &client_key]
            .into_iter()
            .any(|f| f.is_none())
        {
            debug!("One or more TLS files were not specified. Reading kubeconfig...");
            let kubeconfig = read_kube_config()?;
            if ca_cert.is_none() {
                debug!("CA cert was not specified. Reading CA cert from kubeconfig...");
                let first_cluster = kubeconfig.clusters.get(0);
                match first_cluster {
                    Some(cluster) => {
                        ca_cert = Some(cluster.certificate_authority_data.clone().into())
                    }
                    None => return Err(KubeConfigError::MissingClusters.into()),
                }
            }
            if client_cert.is_none() {
                debug!("Client cert was not specified. Reading client cert from kubeconfig...");
                let first_user = kubeconfig.users.get(0);
                match first_user {
                    Some(user) => client_cert = Some(user.client_certificate_data.clone().into()),
                    None => return Err(KubeConfigError::MissingUsers.into()),
                }
            }
            if client_key.is_none() {
                debug!("Client key was not specified. Reading clinet key from kubeconfig...");
                let first_user = kubeconfig.users.get(0);
                match first_user {
                    Some(user) => client_key = Some(user.client_key_data.clone().into()),
                    None => return Err(KubeConfigError::MissingUsers.into()),
                }
            }
        }
        // All None values have been replaced with Some
        let ca_cert = ca_cert.unwrap();
        let client_cert = client_cert.unwrap();
        let client_key = client_key.unwrap();

        let (domain, server_cert, server_key) =
            (|| -> Option<_> { Some((opt.domain?, opt.server_cert?, opt.server_key?)) })().ok_or(
                ClusterCliError::Other(
                    "Missing required args after --tls: --domain, --server-cert, --server-key"
                        .to_string(),
                ),
            )?;

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
        Ok((server_policy, client_policy))
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
