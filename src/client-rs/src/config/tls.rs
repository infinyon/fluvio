use std::convert::TryFrom;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::path::Path;

use log::info;
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
    /// only if server allow anonymous authentication
    NoVerification,

    /// Client certs from file path
    File(TlsClientConfig),

    // Client certs from inline data
    Inline(TlsClientConfig),
}

impl Default for TlsConfig {
    fn default() -> Self {
        Self::NoVerification
    }
}

/// client config generated from either path to string
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TlsClientConfig {
    /// private key or path
    pub client_key: String,

    /// client key or path
    pub client_cert: String,

    /// ca cert or path
    pub ca_cert: String,

    pub domain: String,
}

impl TlsClientConfig {
    /// set ca cert data from external certificate
    pub fn set_ca_cert_from<P: AsRef<Path>>(&mut self, path: P) -> Result<(), IoError> {
        self.set_ca_cert(&read_from_file(path)?);
        Ok(())
    }

    pub fn set_ca_cert(&mut self, cert: &Vec<u8>) {
        self.ca_cert = encode(cert);
    }

    pub fn set_client_cert_from<P: AsRef<Path>>(&mut self, path: P) -> Result<(), IoError> {
        self.set_client_cert(&read_from_file(path)?);
        Ok(())
    }

    pub fn set_client_cert(&mut self, cert: &Vec<u8>) {
        self.client_cert = encode(cert);
    }

    pub fn set_client_key_from<P: AsRef<Path>>(&mut self, path: P) -> Result<(), IoError> {
        self.set_client_key(&read_from_file(path)?);
        Ok(())
    }

    pub fn set_client_key(&mut self, key: &Vec<u8>) {
        self.client_key = encode(key);
    }
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
            TlsConfig::File(file_config) => {
                info!("using client cert");
                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs(file_config.client_cert, file_config.client_key)?
                        .load_ca_cert(file_config.ca_cert)?
                        .build(),
                    file_config.domain,
                )))
            }
            TlsConfig::Inline(inline_config) => {
                info!("using inline cert");
                let ca_cert = decode(inline_config.ca_cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_key = decode(inline_config.client_key).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;
                let client_cert = decode(inline_config.client_cert).map_err(|err| {
                    IoError::new(ErrorKind::InvalidInput, format!("base 64 decode: {}", err))
                })?;

                Ok(AllDomainConnector::TlsDomain(TlsDomainConnector::new(
                    ConnectorBuilder::new()
                        .load_client_certs_from_bytes(&client_cert, &client_key)?
                        .load_ca_cert_from_bytes(&ca_cert)?
                        .build(),
                    inline_config.domain,
                )))
            }
        }
    }
}

/// set ca cert data from external certificate
fn read_from_file<P: AsRef<Path>>(path: P) -> Result<Vec<u8>, IoError> {
    use std::fs::File;
    use std::io::Read;

    let mut file = File::open(path)?;
    let mut buffer = vec![];
    file.read_to_end(&mut buffer)?;

    Ok(buffer)
}
