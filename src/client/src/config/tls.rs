use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;

use tracing::info;
use serde::{Deserialize, Serialize};
use fluvio_future::net::{DomainConnector, DefaultTcpDomainConnector};
use fluvio_future::native_tls::{
    TlsDomainConnector, ConnectorBuilder, IdentityBuilder, X509PemBuilder, PrivateKeyBuilder,
    CertBuilder, TlsAnonymousConnector,
};

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

impl From<TlsCerts> for TlsPolicy {
    fn from(certs: TlsCerts) -> Self {
        Self::Verified(certs.into())
    }
}

impl From<TlsPaths> for TlsPolicy {
    fn from(paths: TlsPaths) -> Self {
        Self::Verified(paths.into())
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

impl TlsConfig {
    /// Returns the domain which this TLS configuration is valid for
    pub fn domain(&self) -> &str {
        match self {
            TlsConfig::Files(TlsPaths { domain, .. }) => &**domain,
            TlsConfig::Inline(TlsCerts { domain, .. }) => &**domain,
        }
    }
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
///
/// Keys and certs stored in the `TlsCerts` type should be PEM PKCS1
/// encoded, with text headers and a base64 encoded body. The
/// stringified contents of a `TlsCerts` should have text resembling
/// the following:
///
/// ```text
/// -----BEGIN RSA PRIVATE KEY-----
/// MIIJKAIBAAKCAgEAsqV4GUKER1wy4sbNvd6gHMp745L4x+ilVElk1ucWGT2akzA6
/// TEvDiAKFF4txkEaLTECh1dUev6rB5HnboWxd5gdg1K4ck2wrZ3Jv2OTA0unXAkoA
/// ...
/// Jh/5Lo8/sj0GmoM6hZyrBZUWI4Q1/l8rgIyu0Lj8okoCmHwZiMrJDDsvdHqET8/n
/// dyIzkH0j11JkN5EJR+U65PJHWPpU3WCAV+0tFzctmiB83e6O9iahZ3OflWs=
/// -----END RSA PRIVATE KEY-----
/// ```
///
/// And certificates should look something like this:
///
/// ```text
/// -----BEGIN CERTIFICATE-----
/// MIIGezCCBGOgAwIBAgIUTYr3REzVKe5JZl2JzLR+rKbv05UwDQYJKoZIhvcNAQEL
/// BQAwYTELMAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMRIwEAYDVQQHDAlTdW5ueXZh
/// ...
/// S6shmu+0il4xqv7pM82iYlaauEfcy0cpjimSQySKDA4S0KB3X8oe7SZqStTJEvtb
/// IuH6soJvn4Mpk5MpTwBw1raCOoKSz2H4oE0B1dBAmQ==
/// -----END CERTIFICATE-----
/// ```
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
        let tls_cert = tmp.join("tls.crt");
        let ca_cert = tmp.join("ca.crt");

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
            key: String::from_utf8(read(paths.key)?).map_err(|e| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("key should be UTF-8: {}", e),
                )
            })?,
            cert: String::from_utf8(read(paths.cert)?).map_err(|e| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("cert should be UTF-8: {}", e),
                )
            })?,
            ca_cert: String::from_utf8(read(paths.ca_cert)?).map_err(|e| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("CA cert should be UTF-8: {}", e),
                )
            })?,
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

impl TryFrom<TlsPolicy> for DomainConnector {
    type Error = IoError;

    fn try_from(config: TlsPolicy) -> Result<Self, Self::Error> {
        match config {
            TlsPolicy::Disabled => Ok(Box::new(DefaultTcpDomainConnector::new())),
            TlsPolicy::Anonymous => {
                info!("Using anonymous TLS");
                let connector: TlsAnonymousConnector = ConnectorBuilder::anonymous().build().into();
                Ok(Box::new(connector))
            }
            TlsPolicy::Verified(TlsConfig::Files(tls)) => {
                info!(
                    domain = &*tls.domain,
                    "Using verified TLS with certificates from paths"
                );

                let builder = ConnectorBuilder::identity(IdentityBuilder::from_x509(
                    X509PemBuilder::from_path(&tls.cert)?,
                    PrivateKeyBuilder::from_path(&tls.key)?,
                )?)?
                .add_root_certificate(X509PemBuilder::from_path(&tls.ca_cert)?)?;

                // disable certificate verification for mac only!
                let builder = if cfg!(target_os = "macos") {
                    builder.no_cert_verification()
                } else {
                    builder
                };
                Ok(Box::new(TlsDomainConnector::new(
                    builder.build(),
                    tls.domain,
                )))
            }
            TlsPolicy::Verified(TlsConfig::Inline(tls)) => {
                info!(
                    domain = &*tls.domain,
                    "Using verified TLS with inline certificates"
                );

                let builder = ConnectorBuilder::identity(IdentityBuilder::from_x509(
                    X509PemBuilder::from_reader(&mut tls.cert.as_bytes())?,
                    PrivateKeyBuilder::from_reader(&mut tls.key.as_bytes())?,
                )?)?
                .add_root_certificate(X509PemBuilder::from_reader(&mut tls.ca_cert.as_bytes())?)?;

                // disable certificate verification for mac only!
                let builder = if cfg!(target_os = "macos") {
                    builder.no_cert_verification()
                } else {
                    builder
                };

                Ok(Box::new(TlsDomainConnector::new(
                    builder.build(),
                    tls.domain,
                )))
            }
        }
    }
}
