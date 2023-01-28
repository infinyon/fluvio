use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::PathBuf;
use std::fmt::{Debug, self};

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
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
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
            TlsConfig::Files(TlsPaths { domain, .. }) => domain,
            TlsConfig::Inline(TlsCerts { domain, .. }) => domain,
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
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize)]
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

impl Debug for TlsCerts {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "TlsCerts {{ domain: {} }}", self.domain)
    }
}

impl TryFrom<TlsPaths> for TlsCerts {
    type Error = IoError;

    fn try_from(paths: TlsPaths) -> Result<Self, Self::Error> {
        use std::fs::read;
        Ok(Self {
            domain: paths.domain,
            key: String::from_utf8(read(paths.key)?).map_err(|e| {
                IoError::new(ErrorKind::InvalidData, format!("key should be UTF-8: {e}"))
            })?,
            cert: String::from_utf8(read(paths.cert)?).map_err(|e| {
                IoError::new(ErrorKind::InvalidData, format!("cert should be UTF-8: {e}"))
            })?,
            ca_cert: String::from_utf8(read(paths.ca_cert)?).map_err(|e| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("CA cert should be UTF-8: {e}"),
                )
            })?,
        })
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

cfg_if::cfg_if! {
    if #[cfg(target_arch = "wasm32")] {

        impl TryFrom<TlsPolicy> for DomainConnector {
            type Error = IoError;

            fn try_from(_config: TlsPolicy) -> Result<Self, Self::Error> {
                info!("Using Default Domain connector for wasm");
                Ok(Box::new(DefaultDomainConnector::new()))
            }
        }

    } else if #[cfg(feature = "openssl")] {

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
                    TlsPolicy::Verified(TlsConfig::Inline(tls)) => {
                        info!(
                            domain = &*tls.domain,
                            "Using verified TLS with inline certificates"
                        );
                        let builder = TlsConnector::builder()
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .with_identity(
                                IdentityBuilder::from_x509(
                                    X509PemBuilder::from_reader(&mut tls.cert.as_bytes())?,
                                    PrivateKeyBuilder::from_reader(&mut tls.key.as_bytes())?
                                )?
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?
                            .add_root_certificate(
                                X509PemBuilder::from_reader(&mut tls.ca_cert.as_bytes())?
                                .build()?
                            )
                            .map_err(|err| IoError::new(IoErrorKind::InvalidData, err))?;


                        Ok(Box::new(TlsDomainConnector::new(
                            builder.build(),
                            tls.domain,
                        )))
                    }
                }
            }
        }
    }  else if #[cfg(feature = "rustls")] {

        impl TryFrom<TlsPolicy> for DomainConnector {
            type Error = IoError;

            fn try_from(config: TlsPolicy) -> Result<Self, Self::Error> {


                use fluvio_future::rust_tls:: ConnectorBuilder;
                use fluvio_future::rust_tls::TlsAnonymousConnector;
                use fluvio_future::rust_tls::TlsDomainConnector;

                match config {
                    TlsPolicy::Disabled => Ok(Box::new(DefaultDomainConnector::new())),
                    TlsPolicy::Anonymous => {
                        info!("Using anonymous TLS");
                        let rust_tls_connnector: TlsAnonymousConnector = ConnectorBuilder::with_safe_defaults()
                        .no_cert_verification()
                        .build().into();
                        Ok(Box::new(rust_tls_connnector))

                    }
                    TlsPolicy::Verified(TlsConfig::Files(tls)) => {
                        info!(
                            domain = &*tls.domain,
                            ca_cert_path = ?tls.ca_cert,
                            client.cert = ?tls.cert,
                            client.key = ?tls.key,
                            "Using verified TLS with certificates from paths"
                        );

                        let connector = ConnectorBuilder::with_safe_defaults()
                        .load_ca_cert(&tls.ca_cert)?
                        .load_client_certs(&tls.cert, &tls.key)?
                        .build();


                        Ok(Box::new(TlsDomainConnector::new(
                            connector,
                            tls.domain
                        )))
                    }
                    TlsPolicy::Verified(TlsConfig::Inline(tls)) => {
                        info!(
                            domain = &*tls.domain,
                            "Using verified TLS with inline certificates"
                        );
                        let connector = ConnectorBuilder::with_safe_defaults()
                        .load_ca_cert_from_bytes(tls.ca_cert.as_bytes())?
                        .load_client_certs_from_bytes(tls.cert.as_bytes(),tls.key.as_bytes())?
                        .build();


                        Ok(Box::new(TlsDomainConnector::new(
                            connector,
                            tls.domain
                        )))
                    }
                }
            }
        }
    } else {
        // by default, no TLS
        impl TryFrom<TlsPolicy> for DomainConnector {
            type Error = IoError;

            fn try_from(_config: TlsPolicy) -> Result<Self, Self::Error> {
                info!("Using Default Domain connector for wasm");
                Ok(Box::new(DefaultDomainConnector::new()))
            }
        }

    }

}
