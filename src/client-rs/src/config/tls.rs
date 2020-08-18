use std::convert::TryFrom;
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "cert_type", content = "cert")]
pub enum TlsConfig {
    /// Do not use TLS. Server must allow anonymous authentication
    NoVerification,

    /// TLS client config with inline keys and certs
    WithCerts {
        /// Client private key
        client_key: String,
        /// Client certificate
        client_cert: String,
        /// Certificate Authority cert
        ca_cert: String,
        /// Domain name
        domain: String,
    },
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::NoVerification
    }
}

impl TryFrom<TlsConfigPaths> for TlsConfig {
    type Error = IoError;

    fn try_from(value: TlsConfigPaths) -> Result<Self, Self::Error> {
        use std::fs::read;
        match value {
            TlsConfigPaths::NoVerification => Ok(Self::NoVerification),
            TlsConfigPaths::WithPaths {
                client_key,
                client_cert,
                ca_cert,
                domain,
            } => Ok(Self::WithCerts {
                client_key: encode(&read(client_key)?),
                client_cert: encode(&read(client_cert)?),
                ca_cert: encode(&read(ca_cert)?),
                domain,
            }),
        }
    }
}

/// TLS client config with paths to keys and certs
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum TlsConfigPaths {
    /// Do not use TLS. Server must allow anonymous authentication
    NoVerification,

    /// Paths to TLS client configs
    WithPaths {
        /// Path to client private key
        client_key: PathBuf,

        /// Path to client certificate
        client_cert: PathBuf,

        /// Path to Certificate Authority certificate
        ca_cert: PathBuf,

        /// Domain name
        domain: String,
    },
}

impl TryFrom<TlsConfig> for AllDomainConnector {
    type Error = IoError;

    fn try_from(config: TlsConfig) -> Result<Self, Self::Error> {
        match config {
            TlsConfig::NoVerification => {
                info!("using anonymous tls");
                Ok(AllDomainConnector::TlsAnonymous(
                    ConnectorBuilder::new()
                        .no_cert_verification()
                        .build()
                        .into(),
                ))
            }
            TlsConfig::WithCerts {
                client_key,
                client_cert,
                ca_cert,
                domain,
                ..
            } => {
                info!("using inline cert");
                let ca_cert = decode(ca_cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_key = decode(client_key).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_cert = decode(client_cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;

                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs_from_bytes(&client_cert, &client_key)?
                        .load_ca_cert_from_bytes(&ca_cert)?
                        .build(),
                    domain,
                )))
            }
        }
    }
}
