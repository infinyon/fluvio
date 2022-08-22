use std::convert::TryFrom;
use std::io::Error as IoError;
use std::path::PathBuf;
use std::fmt::{Debug};

use tracing::info;
use serde::{Deserialize, Serialize};
use fluvio_future::net::{DomainConnector, DefaultDomainConnector};

/// Describes whether or not to use TLS and how
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

// impl From<TlsCerts> for TlsPolicy {
//     fn from(certs: TlsCerts) -> Self {
//         Self::Verified(certs.into())
//     }
// }

impl From<TlsPaths> for TlsPolicy {
    fn from(paths: TlsPaths) -> Self {
        Self::Verified(paths.into())
    }
}

impl From<TlsData> for TlsPolicy {
    fn from(data: TlsData) -> Self {
        Self::Verified(data.into())
    }
}

/// Describes the TLS configuration either inline or via file paths
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "tls_source", content = "certs")]
pub enum TlsConfig {
    /// TLS client config with paths to keys and certs
    #[serde(rename = "files", alias = "file")]
    Files(TlsPaths),
    /// Tls client config with either inline keys and certs or paths to them
    #[serde(rename = "mixed")]
    Mixed(TlsData),
}

impl TlsConfig {
    /// Returns the domain which this TLS configuration is valid for
    pub fn domain(&self) -> &str {
        match self {
            TlsConfig::Files(TlsPaths { domain, .. }) => &**domain,
            // TlsConfig::Inline(TlsCerts { domain, .. }) => &**domain,
            TlsConfig::Mixed(TlsData { domain, .. }) => &**domain,
        }
    }
}

impl From<TlsPaths> for TlsConfig {
    fn from(paths: TlsPaths) -> Self {
        Self::Files(paths)
    }
}

impl From<TlsData> for TlsConfig {
    fn from(config: TlsData) -> Self {
        Self::Mixed(config)
    }
}

/// TLS config with paths to keys and certs
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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

/// Either the path to, or the contents of, a key or cert
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub enum TlsItem {
    Inline(String),
    Path(PathBuf),
}

/// TLS config with either inline keys and certs, or paths to them
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TlsData {
    /// Domain name
    pub domain: String,
    /// A client or server private key
    pub key: TlsItem,
    /// A client or server certificate
    pub cert: TlsItem,
    /// A certificate authority certificate
    pub ca_cert: TlsItem,
}

impl TlsData {
    pub fn is_all_paths(&self) -> bool {
        match (&self.key, &self.cert, &self.ca_cert) {
            (TlsItem::Path(_), TlsItem::Path(_), TlsItem::Path(_)) => true,
            _ => false,
        }
    }
    pub fn is_all_inline(&self) -> bool {
        match (&self.key, &self.cert, &self.ca_cert) {
            (TlsItem::Inline(_), TlsItem::Inline(_), TlsItem::Inline(_)) => true,
            _ => false,
        }
    }
}

impl TlsItem {
    /// Returns the item if it is a path. Panics if it is an inline string.
    pub fn unwrap_path(self) -> PathBuf {
        match self {
            TlsItem::Path(path) => path,
            TlsItem::Inline(_) => panic!("Failed to unwrap TlsItem. Item is not a path."),
        }
    }

    /// Returns the item if it is an inline string. Panics if it is a path.
    pub fn unwrap_inline(self) -> String {
        match self {
            TlsItem::Inline(inline) => inline,
            TlsItem::Path(_) => panic!("Failed to unwrap TlsItem. Item is not an inline string."),
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(target_arch = "wasm32")] {

        impl TryFrom<TlsPolicy> for DomainConnector {
            type Error = IoError;

            fn try_from(_config: TlsPolicy) -> Result<Self, Self::Error> {
                info!("Using Default Domain connector for wasm");
                Ok(Box::new(DefaultDomainConnector::new()))
            }
        }

    } else {

        impl TryFrom<TlsPolicy> for DomainConnector {
            type Error = IoError;

            fn try_from(config: TlsPolicy) -> Result<Self, Self::Error> {
                use std::io::ErrorKind as IoErrorKind;


                use fluvio_future::net::certs::CertBuilder;
                use fluvio_future::openssl:: {TlsDomainConnector,TlsConnector,TlsAnonymousConnector};
                use fluvio_future::openssl::certs::{IdentityBuilder,X509PemBuilder,PrivateKeyBuilder};


                match config {
                    TlsPolicy::Disabled => Ok(Box::new(DefaultDomainConnector::new())),
                    TlsPolicy::Anonymous => {
                        info!("Using anonymous TLS");
                        let builder = TlsConnector::builder()
                                .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                                .with_hostname_vertification_disabled()
                                .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?;

                        let connector: TlsAnonymousConnector = builder.build().into();
                        Ok(Box::new(connector))

                    }
                    TlsPolicy::Verified(TlsConfig::Files(tls)) => {
                        info!(
                            domain = &*tls.domain,
                            "Using verified TLS with certificates from paths"
                        );

                        let builder = TlsConnector::builder()
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .with_identity(
                                IdentityBuilder::from_x509(
                                    X509PemBuilder::from_path(&tls.cert)?,
                                    PrivateKeyBuilder::from_path(&tls.key)?
                                )?
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .add_root_certificate(
                                X509PemBuilder::from_path(&tls.ca_cert)?
                                .build()?
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?;


                        Ok(Box::new(TlsDomainConnector::new(
                            builder.build(),
                            tls.domain,
                        )))
                    }
                    TlsPolicy::Verified(TlsConfig::Mixed(tls)) => {
                        info!(
                            domain = &*tls.domain,
                            "Using verified TLS with mixed inline certificates and paths"
                        );
                        let builder = TlsConnector::builder()
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .with_identity(
                                IdentityBuilder::from_x509(
                                    match tls.cert {
                                        TlsItem::Inline(cert) => X509PemBuilder::from_reader(&mut cert.as_bytes())?,
                                        TlsItem::Path(cert) => X509PemBuilder::from_path(&cert)?,
                                    },
                                    match tls.key {
                                        TlsItem::Inline(key) => PrivateKeyBuilder::from_reader(&mut key.as_bytes())?,
                                        TlsItem::Path(key) => PrivateKeyBuilder::from_path(&key)?,
                                    }
                                )?
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .add_root_certificate(
                                match tls.ca_cert {
                                    TlsItem::Inline(ca_cert) => X509PemBuilder::from_reader(&mut ca_cert.as_bytes())?.build()?,
                                    TlsItem::Path(ca_cert) => X509PemBuilder::from_path(&ca_cert)?.build()?,
                                }
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?;

                        Ok(Box::new(TlsDomainConnector::new(builder.build(), tls.domain)))
                    }
                }
            }
        }
    }

}
