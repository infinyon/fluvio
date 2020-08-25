use std::convert::{TryFrom, TryInto};
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use tracing::info;
use base64::decode;
use serde::Deserialize;
use serde::Serialize;
use base64::encode;

use flv_future_aio::net::tls::AllDomainConnector;
use flv_future_aio::net::tls::TlsDomainConnector;
use flv_future_aio::net::tls::ConnectorBuilder;

/// Describes whether or not to use TLS and how
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tls_policy")]
pub enum TlsPolicy {
    /// Do not use TLS
    #[serde(rename = "disabled", alias = "disable")]
    Disabled,
    /// Use TLS, but do not verify certificates or domains
    #[serde(rename = "no_verify", alias = "no_verification")]
    NoVerify,
    /// Use TLS and verify certificates and domains
    #[serde(rename = "verify")]
    Verify(TlsConfig),
}

impl Default for TlsPolicy {
    fn default() -> Self {
        Self::Disabled
    }
}

impl From<TlsConfig> for TlsPolicy {
    fn from(tls: TlsConfig) -> Self {
        Self::Verify(tls)
    }
}

/// Describes the TLS configuration either inline or via file paths
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tls_source", content = "certs")]
pub enum TlsConfig {
    /// TLS client config with inline keys and certs
    #[serde(rename = "inline")]
    Inline(TlsCerts),
    /// TLS client config with paths to keys and certs
    #[serde(rename = "files", alias = "file")]
    Files(TlsPaths),
}

impl From<TlsCerts> for TlsConfig {
    fn from(certs: TlsCerts) -> Self {
        Self::Inline(certs)
    }
}

impl From<TlsPaths> for TlsConfig {
    fn from(paths: TlsPaths) -> Self {
        Self::Files(paths)
    }
}

/// TLS config with inline keys and certs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TlsCerts {
    /// Domain name
    pub domain: String,
    /// Client or Server private key
    pub key: String,
    /// Client or Server certificate
    pub cert: String,
    /// Certificate Authority cert
    pub ca_cert: String,
}

impl TryFrom<TlsPaths> for TlsCerts {
    type Error = IoError;

    fn try_from(paths: TlsPaths) -> Result<Self, Self::Error> {
        use std::fs::read;
        Ok(Self {
            domain: paths.domain,
            key: encode(&read(paths.key)?),
            cert: encode(&read(paths.cert)?),
            ca_cert: encode(&read(paths.ca_cert)?),
        })
    }
}

/// TLS config with paths to keys and certs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TlsPaths {
    /// Domain name
    pub domain: String,
    /// Path to client or server private key
    pub key: PathBuf,
    /// Path to client or server certificate
    pub cert: PathBuf,
    /// Path to Certificate Authority certificate
    pub ca_cert: PathBuf,
}

// TODO move this to AllDomainConnector
impl TryFrom<TlsPolicy> for AllDomainConnector {
    type Error = IoError;

    fn try_from(config: TlsPolicy) -> Result<Self, Self::Error> {
        match config {
            TlsPolicy::Disabled => Ok(AllDomainConnector::default_tcp()),
            TlsPolicy::NoVerify => {
                info!("using anonymous tls");
                Ok(AllDomainConnector::TlsAnonymous(
                    ConnectorBuilder::new()
                        .no_cert_verification()
                        .build()
                        .into(),
                ))
            }
            TlsPolicy::Verify(tls) => {
                // Convert path certs to inline if necessary
                let tls: TlsCerts = match tls {
                    TlsConfig::Inline(certs) => certs,
                    TlsConfig::Files(cert_paths) => cert_paths.try_into()?,
                };
                let ca_cert = decode(tls.ca_cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_key = decode(tls.key).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_cert = decode(tls.cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;

                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs_from_bytes(&client_cert, &client_key)?
                        .load_ca_cert_from_bytes(&ca_cert)?
                        .build(),
                    tls.domain,
                )))
            }
        }
    }
}
