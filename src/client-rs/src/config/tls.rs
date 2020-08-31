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
///
/// Keys and certs stored in the `TlsCerts` type should be PEM
/// encoded, with text headers and a base64 encoded body. The
/// stringified contents of a `TlsCerts` should have text resembling
/// the following:
///
/// ```ignore
/// -----BEGIN RSA PRIVATE KEY-----
/// MIIJKAIBAAKCAgEAsqV4GUKER1wy4sbNvd6gHMp745L4x+ilVElk1ucWGT2akzA6
/// TEvDiAKFF4txkEaLTECh1dUev6rB5HnboWxd5gdg1K4ck2wrZ3Jv2OTA0unXAkoA
/// ...
/// Jh/5Lo8/sj0GmoM6hZyrBZUWI4Q1/l8rgIyu0Lj8okoCmHwZiMrJDDsvdHqET8/n
/// dyIzkH0j11JkN5EJR+U65PJHWPpU3WCAV+0tFzctmiB83e6O9iahZ3OflWs=
/// -----END RSA PRIVATE KEY-----
///
/// And certificates should look something like this:
///
/// ```ignore
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

        let key = String::from_utf8(read(paths.key)?)
            .map_err(|e| IoError::new(ErrorKind::Other, format!("failed to read key to string")))?;
        let cert = String::from_utf8(read(paths.cert)?).map_err(|e| {
            IoError::new(ErrorKind::Other, format!("failed to read cert to string"))
        })?;
        let ca_cert = String::from_utf8(read(paths.ca_cert)?).map_err(|e| {
            IoError::new(
                ErrorKind::Other,
                format!("failed to read ca_cert to string"),
            )
        })?;

        Ok(Self {
            key,
            cert,
            ca_cert,
            domain: paths.domain,
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
            TlsPolicy::Anonymous => {
                info!("using anonymous tls");
                Ok(AllDomainConnector::TlsAnonymous(
                    ConnectorBuilder::new()
                        .no_cert_verification()
                        .build()
                        .into(),
                ))
            }
            TlsPolicy::Verified(TlsConfig::Files(tls)) => {
                println!("Using TLS files: {:#?}", tls);
                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs(tls.cert, tls.key)?
                        .load_ca_cert(tls.ca_cert)?
                        .build(),
                    tls.domain,
                )))
            }
            TlsPolicy::Verified(TlsConfig::Inline(tls)) => {
                // TODO remove this
                println!("Using TLS certs: {:#?}", tls);
                let ca_cert = tls.ca_cert.as_bytes();
                let client_key = tls.key.as_bytes();
                let client_cert = tls.cert.as_bytes();

                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs_from_bytes(client_cert, client_key)?
                        .load_ca_cert_from_bytes(ca_cert)?
                        .build(),
                    tls.domain,
                )))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_convert_path_to_inline() {
        let root_path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let certs_path = root_path.parent().unwrap().parent().unwrap().join("tls/certs");

        let tls_paths = TlsPaths {
            domain: "example.com".to_string(),
            cert: certs_path.join("client.crt").to_owned(),
            key: certs_path.join("client.key").to_owned(),
            ca_cert: certs_path.join("ca.crt").to_owned(),
        };
        let tls_inline: TlsCerts = tls_paths.try_into().expect("should get certs");

        assert!(tls_inline.key.starts_with("-----BEGIN RSA PRIVATE KEY-----"));
        assert!(tls_inline.key.ends_with("-----END RSA PRIVATE KEY-----\n"));
        assert!(tls_inline.cert.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(tls_inline.cert.ends_with("-----END CERTIFICATE-----\n"));
        assert!(tls_inline.ca_cert.starts_with("-----BEGIN CERTIFICATE-----"));
        assert!(tls_inline.ca_cert.ends_with("-----END CERTIFICATE-----\n"));
    }
}
