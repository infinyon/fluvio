use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use clap::Parser;

use fluvio::config::{TlsData, TlsItem, TlsPolicy};

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

        let mut missing_args = vec![];
        if opt.domain.is_none() {
            missing_args.push("--domain");
        }
        if opt.server_cert.is_none() {
            missing_args.push("--server-cert");
        }
        if opt.server_key.is_none() {
            missing_args.push("--server-key");
        }
        if !missing_args.is_empty() {
            return Err(ClusterCliError::Other(format!(
                "Missing required arguments after --tls: {}",
                missing_args.join(", ")
            )));
        }

        let (client_key, client_cert, ca_cert) =
            if [&opt.ca_cert, &opt.client_cert, &opt.client_key]
                .into_iter()
                .any(|f| f.is_none())
            {
                debug!("One or more TLS files were not specified. Reading kubeconfig...");
                let kubeconfig = read_kube_config()?;

                let client_key = match opt.client_key {
                    Some(key) => TlsItem::Path(key),
                    None => {
                        // Get the first user listed in the kubeconfig
                        let user = kubeconfig.users.get(0);
                        match user {
                            Some(user) => TlsItem::Inline(user.user.client_key.clone().into()),
                            None => return Err(KubeConfigError::MissingUsers.into()),
                        }
                    }
                };
                let client_cert = match opt.client_cert {
                    Some(cert) => TlsItem::Path(cert),
                    None => {
                        // Get the first user listed in the kubeconfig
                        let user = kubeconfig.users.get(0);
                        match user {
                            Some(user) => TlsItem::Inline(user.user.client_cert.clone().into()),
                            None => return Err(KubeConfigError::MissingUsers.into()),
                        }
                    }
                };
                let ca_cert = match opt.ca_cert.clone() {
                    Some(ca_cert) => TlsItem::Path(ca_cert),
                    None => {
                        // Get the first cluster listed in the kubeconfig
                        let cluster = kubeconfig.clusters.get(0);
                        match cluster {
                            Some(cluster) => {
                                TlsItem::Inline(cluster.cluster.ca_cert.clone().into())
                            }
                            None => return Err(KubeConfigError::MissingClusters.into()),
                        }
                    }
                };
                (client_key, client_cert, ca_cert)
            } else {
                // --client-key, --client-cert and --ca-cert were all given.
                (
                    TlsItem::Path(opt.client_key.unwrap()),
                    TlsItem::Path(opt.client_cert.unwrap()),
                    TlsItem::Path(opt.ca_cert.unwrap()),
                )
            };
        let client_policy = TlsPolicy::from(TlsData {
            domain: opt.domain.clone().unwrap(),
            key: client_key,
            cert: client_cert,
            ca_cert: ca_cert.clone(),
        });
        // --domain, --server-key and --server-cert were all given.
        let server_policy = TlsPolicy::from(TlsData {
            domain: opt.domain.unwrap(),
            key: TlsItem::Path(opt.server_key.unwrap()),
            cert: TlsItem::Path(opt.server_cert.unwrap()),
            ca_cert,
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
