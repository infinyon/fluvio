use std::convert::TryFrom;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::path::PathBuf;
use std::fmt::Debug;
use std::borrow::Cow;

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

/// Describes the TLS configuration
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
pub struct TlsConfig {
    pub domain: String,
    pub key: TlsItem,
    pub cert: TlsItem,
    pub ca_cert: TlsItem,
}

impl TlsConfig {
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

    /// Writes any inline items to disk and returns a `TlsConfig` that is all paths.
    /// If all items were already paths, returns `Cow::Borrowed(self)`. Otherwise, returns
    /// `Cow::Owned` of a new `TlsConfig` consisting only of paths.
    pub fn write_inline_to_disk(&self) -> Result<TlsConfigPaths, IoError> {
        let domain = &self.domain;

        // Only create a temporary directory if there is at least one inline item.
        if let (TlsItem::Path(key), TlsItem::Path(cert), TlsItem::Path(ca_cert)) =
            (&self.key, &self.cert, &self.ca_cert)
        {
            Ok(TlsConfigPaths {
                domain,
                key: Cow::Borrowed(key),
                cert: Cow::Borrowed(cert),
                ca_cert: Cow::Borrowed(ca_cert),
            })
        } else {
            let temp_dir = create_temp_dir()?;

            let key = Cow::Owned(
                self.key
                    .write_if_inline(temp_dir.join("tls.key"), CertKind::Key)?,
            );
            let cert = Cow::Owned(
                self.cert
                    .write_if_inline(temp_dir.join("tls.crt"), CertKind::Cert)?,
            );
            let ca_cert = Cow::Owned(
                self.ca_cert
                    .write_if_inline(temp_dir.join("ca.crt"), CertKind::Cert)?,
            );
            Ok(TlsConfigPaths {
                domain,
                key,
                cert,
                ca_cert,
            })
        }
    }
}

#[derive(Debug)]
pub struct TlsConfigPaths<'a> {
    pub domain: &'a str,
    pub key: Cow<'a, PathBuf>,
    pub cert: Cow<'a, PathBuf>,
    pub ca_cert: Cow<'a, PathBuf>,
}

/// Create a temporary directory to store TLS certs in.
fn create_temp_dir() -> Result<PathBuf, IoError> {
    use rand::distributions::Alphanumeric;
    use std::iter;
    use rand::Rng;

    // Generate a random 12 digit alphanmueric string
    const NUM_RAND_DIR_CHARS: usize = 12;

    let mut rng = rand::thread_rng();
    let rand_dir_name: String = iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .map(char::from)
        .take(NUM_RAND_DIR_CHARS)
        .collect();

    let tmp_dir = std::env::temp_dir().join(rand_dir_name);

    std::fs::create_dir(&tmp_dir)?;
    Ok(tmp_dir)
}

/// Either the path to, or the contents of, a key or cert
#[derive(Debug, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
pub enum TlsItem {
    Inline(String),
    Path(PathBuf),
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

    /// If the TLS item is inline, write it to the file at the specified path.
    /// If the item is a path, return it.
    fn write_if_inline(&self, path: PathBuf, cert_kind: CertKind) -> Result<PathBuf, IoError> {
        use std::fs::write;

        Ok(match self {
            TlsItem::Path(p) => p.clone(),
            TlsItem::Inline(data) => {
                write(&path, format_cert_data(data, cert_kind)?.as_bytes())?;
                path
            }
        })
    }
}

/// Formats cert data according to PEM requirements.
///
/// PEM formatted certs require a newline every 64 characters.
fn format_cert_data(data: &String, kind: CertKind) -> Result<String, IoError> {
    let prefix = match kind {
        CertKind::Key => "-----BEGIN RSA PRIVATE KEY-----\n",
        CertKind::Cert => "-----BEGIN CERTIFICATE-----\n",
    };
    let postfix = match kind {
        CertKind::Key => "-----END RSA PRIVATE KEY-----",
        CertKind::Cert => "-----END CERTIFICATE-----",
    };
    let data = data.as_bytes();
    let chunks = data.chunks(64);
    // Allocate enough space for the original data, plus one newline every 64 chars, plus the pre and postfix.
    let mut formatted =
        String::with_capacity(data.len() + data.len() / 64 + prefix.len() + postfix.len());
    formatted.push_str(prefix);
    for chunk in chunks {
        formatted.push_str(
            std::str::from_utf8(chunk).map_err(|e| IoError::new(IoErrorKind::InvalidData, e))?,
        );
        formatted.push_str("\n");
    }
    formatted.push_str(postfix);
    Ok(formatted)
}

enum CertKind {
    Key,
    Cert,
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
                    TlsPolicy::Verified(tls) => {
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
