use std::path::PathBuf;
use std::convert::TryFrom;

use tracing::debug;
use clap::Parser;
use k8_config::{KubeConfig, ConfigError as KubeConfigError};

use fluvio::config::{TlsDocs, TlsDoc, TlsPolicy};

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

        let no_optional_certs_missing = [&opt.ca_cert, &opt.client_cert, &opt.client_key]
            .into_iter()
            .all(|f| f.is_some());

        let (client_key, client_cert, ca_cert) = if no_optional_certs_missing {
            // --client-key, --client-cert and --ca-cert were all given. No need to read the kubeconfig.
            (
                TlsDoc::Path(opt.client_key.unwrap()),
                TlsDoc::Path(opt.client_cert.unwrap()),
                TlsDoc::Path(opt.ca_cert.unwrap()),
            )
        } else {
            debug!("One or more TLS files were not specified. Reading kubeconfig...");
            let kubeconfig = KubeConfig::from_home()?;

            // If --client-key was specified, use that. Otherwise try to read it from the kubeconfig.
            let client_key = match opt.client_key {
                Some(key) => TlsDoc::Path(key),
                None => {
                    let user = kubeconfig.current_user();
                    match user {
                        Some(user) => match user.user.client_key_data.clone() {
                            Some(client_key) => TlsDoc::Inline(client_key),
                            None => {
                                return Err(
                                    KubeConfigError::Other("Missing client key".to_owned()).into()
                                )
                            }
                        },
                        None => {
                            return Err(KubeConfigError::Other("Missing user".to_owned()).into())
                        }
                    }
                }
            };
            // If --client-cert was specified, use that. Otherwise try to read it from the kubeconfig.
            let client_cert = match opt.client_cert {
                Some(cert) => TlsDoc::Path(cert),
                None => {
                    let user = kubeconfig.current_user();
                    match user {
                        Some(user) => match user.user.client_certificate_data.clone() {
                            Some(client_cert) => TlsDoc::Inline(client_cert),
                            None => {
                                return Err(KubeConfigError::Other(
                                    "Missing client certificate".to_owned(),
                                )
                                .into())
                            }
                        },
                        None => {
                            return Err(KubeConfigError::Other("Missing user".to_owned()).into())
                        }
                    }
                }
            };
            // If --ca-cert was specified, use that. Otherwise try to read it from the kubeconfig.
            let ca_cert = match opt.ca_cert.clone() {
                Some(ca_cert) => TlsDoc::Path(ca_cert),
                None => {
                    let cluster = kubeconfig.current_cluster();
                    match cluster {
                        Some(cluster) => match cluster.cluster.certificate_authority_data.clone() {
                            Some(ca_cert) => TlsDoc::Inline(ca_cert),
                            None => {
                                return Err(KubeConfigError::Other(
                                    "Missing CA certificate".to_owned(),
                                )
                                .into())
                            }
                        },
                        None => {
                            return Err(KubeConfigError::Other("Missing cluster".to_owned()).into())
                        }
                    }
                }
            };
            (client_key, client_cert, ca_cert)
        };
        let client_policy = TlsPolicy::from(TlsDocs {
            domain: opt.domain.clone().unwrap(),
            key: client_key,
            cert: client_cert,
            ca_cert: ca_cert.clone(),
        });
        // --domain, --server-key and --server-cert were all given.
        let server_policy = TlsPolicy::from(TlsDocs {
            domain: opt.domain.unwrap(),
            key: TlsDoc::Path(opt.server_key.unwrap()),
            cert: TlsDoc::Path(opt.server_cert.unwrap()),
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

        use fluvio::config::TlsPolicy::*;
        match (client, server) {
            (Verified(client_paths), Verified(server_paths)) => {
                // Client checks
                let client_paths = client_paths.into_docs();
                assert_eq!(client_paths.domain, "fluvio.io");
                assert_eq!(
                    client_paths.ca_cert.unwrap_path(),
                    PathBuf::from("/tmp/certs/ca.crt")
                );
                assert_eq!(
                    client_paths.cert.unwrap_path(),
                    PathBuf::from("/tmp/certs/client.crt")
                );
                assert_eq!(
                    client_paths.key.unwrap_path(),
                    PathBuf::from("/tmp/certs/client.key")
                );

                // Server checks
                let server_paths = server_paths.into_docs();
                assert_eq!(server_paths.domain, "fluvio.io");
                assert_eq!(
                    server_paths.ca_cert.unwrap_path(),
                    PathBuf::from("/tmp/certs/ca.crt")
                );
                assert_eq!(
                    server_paths.cert.unwrap_path(),
                    PathBuf::from("/tmp/certs/server.crt")
                );
                assert_eq!(
                    server_paths.key.unwrap_path(),
                    PathBuf::from("/tmp/certs/server.key")
                );
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
