use std::sync::Arc;
use anyhow::{Context, Result};
use clap::Parser;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use fluvio_sc_schema::{
    mirror::{ClientTls, Home, MirrorSpec, MirrorType},
    remote_file::RemoteMetadataExport,
};
use anyhow::anyhow;

#[derive(Debug, Parser)]
pub struct ExportOpt {
    /// id of the remote cluster to export
    remote_id: String,
    /// name of the file where we should put the file
    #[arg(long, short = 'f')]
    file: Option<String>,
    /// override endpoint of the home cluster
    #[arg(long, short = 'e')]
    public_endpoint: Option<String>,
    /// id of the home cluster to share
    #[arg(long)]
    home_id: Option<String>,
    /// remote tls certificate
    #[arg(long)]
    cert: Option<String>,
    /// remote tls key
    #[arg(long)]
    key: Option<String>,
}

impl ExportOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let fluvio_config = cluster_target.load()?;
        let public_endpoint = if let Some(public_endpoint) = self.public_endpoint {
            public_endpoint.clone()
        } else {
            fluvio_config.endpoint.clone()
        };
        let flv = fluvio::Fluvio::connect_with_config(&fluvio_config).await?;
        let admin = flv.admin().await;

        let all_remotes = admin.all::<MirrorSpec>().await?;
        let _remote = all_remotes
            .iter()
            .find(|remote| match &remote.spec.mirror_type {
                MirrorType::Remote(remote) => remote.id == self.remote_id,
                _ => false,
            })
            .ok_or_else(|| anyhow!("remote cluster not found"))?;

        let home_id = self.home_id.clone().unwrap_or_else(|| "home".to_owned());

        let client_tls = get_tls_config(
            fluvio_config.clone(),
            self.cert.clone(),
            self.key.clone(),
            self.remote_id.clone(),
        )?;
        let home_metadata = Home {
            id: home_id,
            remote_id: self.remote_id,
            public_endpoint,
            client_tls,
        };

        let metadata = RemoteMetadataExport::new(home_metadata);

        if let Some(filename) = self.file {
            std::fs::write(filename, serde_json::to_string_pretty(&metadata)?)
                .context("failed to write output file")?;
        } else {
            out.println(&serde_json::to_string_pretty(&metadata)?);
        }

        Ok(())
    }
}

#[cfg(unix)]
fn get_tls_config(
    fluvio_config: fluvio::config::FluvioConfig,
    cert_path: Option<String>,
    key_path: Option<String>,
    remote_id: String,
) -> Result<Option<ClientTls>> {
    use fluvio::config::{TlsConfig, TlsPolicy};
    use fluvio_future::native_tls::{CertBuilder, X509PemBuilder};
    match &fluvio_config.tls {
        TlsPolicy::Verified(config) => {
            let (remote_cert, remote_key, cert_path) = match (cert_path.clone(), key_path) {
                (Some(cert), Some(key)) => (
                    std::fs::read_to_string(cert.clone())?,
                    std::fs::read_to_string(key)?,
                    cert,
                ),
                _ => {
                    return Err(anyhow!(
                        "remote cert and key are required for a cluster using TLS"
                    ));
                }
            };

            let cert_build = X509PemBuilder::from_path(cert_path)
                .map_err(|err| anyhow!("error building cert: {}", err))?;

            let cert = cert_build
                .build()
                .map_err(|err| anyhow!("error building cert: {}", err))?;

            let cert_der = cert
                .to_der()
                .map_err(|err| anyhow!("error converting cert to der: {}", err))?;

            let principal = fluvio_auth::x509::X509Authenticator::principal_from_raw_certificate(&cert_der).expect(
                "error getting principal from certificate. This should never happen as the certificate is valid",
            );

            if principal != remote_id {
                return Err(anyhow!(
                    "remote_id: \"{}\" does not match the CN in the certificate: \"{}\"",
                    remote_id,
                    principal
                ));
            }

            match config {
                TlsConfig::Inline(config) => Ok(Some(ClientTls {
                    domain: config.domain.clone(),
                    ca_cert: config.ca_cert.clone(),
                    client_cert: remote_cert,
                    client_key: remote_key,
                })),
                TlsConfig::Files(file_config) => Ok(Some(ClientTls {
                    domain: file_config.domain.clone(),
                    ca_cert: std::fs::read_to_string(&file_config.ca_cert)?,
                    client_cert: remote_cert,
                    client_key: remote_key,
                })),
            }
        }
        _ => Ok(None),
    }
}

#[cfg(not(unix))]
fn get_tls_config(
    _fluvio_config: fluvio::config::FluvioConfig,
    _cert_path: Option<String>,
    _key_path: Option<String>,
    _remote_id: String,
) -> Result<Option<ClientTls>> {
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg(unix)]
    #[test]
    fn test_get_tls_config_on_unix() {
        let fluvio_config = fluvio::config::FluvioConfig::new("localhost:9003".to_owned());
        let cert_dir = std::env::current_dir()
            .unwrap()
            .join("..")
            .join("..")
            .join("tls")
            .join("certs");

        let ca_cert = cert_dir.join("ca.crt");
        let config_tls = fluvio::config::TlsConfig::Files(fluvio::config::TlsPaths {
            domain: "localhost".to_owned(),
            ca_cert: ca_cert.clone(),
            cert: cert_dir.join("client-root.crt"),
            key: cert_dir.join("client-root.key"),
        });

        let fluvio_config_with_tls = fluvio_config.with_tls(config_tls);

        let cert_path = Some(
            cert_dir
                .join("client-user1.crt")
                .to_str()
                .unwrap()
                .to_owned(),
        );
        let key_path = Some(
            cert_dir
                .join("client-user1.key")
                .to_str()
                .unwrap()
                .to_owned(),
        );
        let remote_id = "user1".to_owned();

        let tls_result =
            get_tls_config(fluvio_config_with_tls, cert_path, key_path, remote_id).unwrap();
        assert!(tls_result.is_some());
        let client_tls = tls_result.unwrap();

        assert_eq!(client_tls.domain, "localhost");
        assert_eq!(
            client_tls.ca_cert,
            std::fs::read_to_string(&ca_cert).unwrap()
        );
        assert_eq!(
            client_tls.client_cert,
            std::fs::read_to_string(cert_dir.join("client-user1.crt")).unwrap()
        );
        assert_eq!(
            client_tls.client_key,
            std::fs::read_to_string(cert_dir.join("client-user1.key")).unwrap()
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_get_tls_config_no_cert_key_when_tls_on_unix() {
        let fluvio_config = fluvio::config::FluvioConfig::new("localhost:9003".to_owned());
        let cert_dir = std::env::current_dir()
            .unwrap()
            .join("..")
            .join("..")
            .join("tls")
            .join("certs");

        let ca_cert = cert_dir.join("ca.crt");
        let config_tls = fluvio::config::TlsConfig::Files(fluvio::config::TlsPaths {
            domain: "localhost".to_owned(),
            ca_cert: ca_cert.clone(),
            cert: cert_dir.join("client-root.crt"),
            key: cert_dir.join("client-root.key"),
        });

        let fluvio_config_with_tls = fluvio_config.with_tls(config_tls);

        let cert_path = None;
        let key_path = None;
        let remote_id = "user1".to_owned();

        let tls_result = get_tls_config(fluvio_config_with_tls, cert_path, key_path, remote_id);
        assert!(tls_result.is_err());

        let err = tls_result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "remote cert and key are required for a cluster using TLS"
        );
    }

    #[cfg(unix)]
    #[test]
    fn test_get_tls_config_wrong_cn_on_unix() {
        let fluvio_config = fluvio::config::FluvioConfig::new("localhost:9003".to_owned());
        let cert_dir = std::env::current_dir()
            .unwrap()
            .join("..")
            .join("..")
            .join("tls")
            .join("certs");

        let ca_cert = cert_dir.join("ca.crt");
        let config_tls = fluvio::config::TlsConfig::Files(fluvio::config::TlsPaths {
            domain: "localhost".to_owned(),
            ca_cert: ca_cert.clone(),
            cert: cert_dir.join("client-root.crt"),
            key: cert_dir.join("client-root.key"),
        });

        let fluvio_config_with_tls = fluvio_config.with_tls(config_tls);

        let cert_path = Some(
            cert_dir
                .join("client-user1.crt")
                .to_str()
                .unwrap()
                .to_owned(),
        );
        let key_path = Some(
            cert_dir
                .join("client-user1.key")
                .to_str()
                .unwrap()
                .to_owned(),
        );
        let remote_id = "user2".to_owned();

        let tls_result = get_tls_config(fluvio_config_with_tls, cert_path, key_path, remote_id);
        assert!(tls_result.is_err());

        let err = tls_result.unwrap_err();
        assert_eq!(
            err.to_string(),
            "remote_id: \"user2\" does not match the CN in the certificate: \"user1\""
        );
    }
}
