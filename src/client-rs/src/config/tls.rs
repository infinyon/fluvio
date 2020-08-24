use std::convert::{TryFrom, TryInto};
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use tracing::{info, debug};
use base64::{encode, decode};
use serde::{Deserialize, Serialize};

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
    ///
    /// Server must support anonymous TLS
    #[serde(rename = "anonymous")]
    Anonymous,
    /// Use TLS and verify certificates and domains
    #[serde(rename = "verified", alias = "verify")]
    Verified(TlsConfig),
}

impl Default for TlsPolicy {
    fn default() -> Self {
        Self::Disabled
    }
}

impl From<TlsConfig> for TlsPolicy {
    fn from(tls: TlsConfig) -> Self {
        Self::Verified(tls)
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

impl TlsCerts {
    /// Attempts to write the inline TLS certs into temporary files
    ///
    /// Returns a `TlsPaths` populated with the paths where the
    /// temporary files were written.
    pub fn try_into_temp_files(&self) -> Result<TlsPaths, IoError> {
        use std::fs::write;
        let tmp = std::env::temp_dir();

        let tls_key = tmp.join("tls.key");
        let tls_cert = tmp.join("tls.cert");
        let ca_cert = tmp.join("ca.cert");

        write(&tls_key, self.key.as_bytes())?;
        write(&tls_cert, self.cert.as_bytes())?;
        write(&ca_cert, self.ca_cert.as_bytes())?;

        Ok(TlsPaths {
            domain: self.domain.clone(),
            key: tls_key,
            cert: tls_cert,
            ca_cert,
        })
    }
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

/// Converts pretty-printed base64 into actual base64.
///
/// OpenSSL generates PEM base64 in a way that has headers
/// and footers surrounding the base64 encoding, as well as
/// newlines to promote readability. This function takes
/// those out in order to allow the base64 to be directly
/// decoded.
///
/// If the given string has `-----BEGIN PRIVATE KEY-----`
/// and newlines (i.e. it is pretty-printed), then this
/// function returns the body of base64 between the headers
/// with newlines stripped.
fn try_strip_pkey_base64(pkey: &str) -> String {
    let header_stripped = pkey.strip_prefix("-----BEGIN PRIVATE KEY-----")
        .and_then(|pkey| pkey.strip_suffix("-----END PRIVATE KEY-----\n"));

    match header_stripped {
        // If there were no headers, return original string
        None => pkey.to_owned(),
        // If there were headers, then also strip newlines
        Some(stripped) => stripped.replace("\n", ""),
    }
}

/// Converts pretty-printed base64 into actual base64.
///
/// OpenSSL generates PEM base64 in a way that has headers
/// and footers surrounding the base64 encoding, as well as
/// newlines to promote readability. This function takes
/// those out in order to allow the base64 to be directly
/// decoded.
///
/// If the given string has `-----BEGIN CERTIFICATE-----`
/// and newlines (i.e. it is pretty-printed), then this
/// function returns the body of base64 between the headers
/// with newlines stripped.
fn try_strip_cert_base64(pkey: &str) -> String {
    let header_stripped = pkey.strip_prefix("-----BEGIN CERTIFICATE-----")
        .and_then(|pkey| pkey.strip_suffix("-----END CERTIFICATE-----\n"));

    match header_stripped {
        // If there were no headers, return original string
        None => pkey.to_owned(),
        // If there were headers, then also strip newlines
        Some(stripped) => stripped.replace("\n", ""),
    }
}

// TODO move this to AllDomainConnector
impl TryFrom<TlsPolicy> for AllDomainConnector {
    type Error = IoError;

    fn try_from(config: TlsPolicy) -> Result<Self, Self::Error> {
        match config {
            TlsPolicy::Disabled => Ok(AllDomainConnector::default_tcp()),
            TlsPolicy::Anonymous => {
                info!("using anonymous tls");
                Ok(AllDomainConnector::TlsAnonymous(
                    ConnectorBuilder::new()
                        .no_cert_verification()
                        .build()
                        .into(),
                ))
            }
            TlsPolicy::Verified(tls) => {
                // Convert path certs to inline if necessary
                let tls: TlsCerts = match tls {
                    TlsConfig::Inline(certs) => certs,
                    TlsConfig::Files(cert_paths) => cert_paths.try_into()?,
                };
                // TODO remove this
                debug!("Using TLS certs: {:#?}", tls);
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
