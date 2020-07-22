use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use structopt::StructOpt;

use flv_client::config::TlsConfig as TlsProfileConfig;
use flv_client::config::TlsClientConfig;

/// Optional Tls Configuration to Client
#[derive(Debug, StructOpt, Default)]
pub struct TlsConfig {
    /// enable tls
    #[structopt(long)]
    pub tls: bool,

    /// required if client cert is used
    #[structopt(long, required_if("tls", "true"))]
    pub domain: Option<String>,

    /// TLS: path to client certificate
    #[structopt(long)]
    pub client_cert: Option<String>,
    #[structopt(long)]
    /// TLS: path to client private key
    pub client_key: Option<String>,
    /// TLS: enable client cert
    #[structopt(long)]
    pub enable_client_cert: bool,
    /// TLS: path to ca cert, required when client cert is enabled
    #[structopt(long)]
    pub ca_cert: Option<String>,
}

impl TlsConfig {
    pub fn try_into_file_config(self) -> Result<Option<TlsProfileConfig>, IoError> {
        if self.tls {
            debug!("using tls");
            if self.enable_client_cert {
                debug!("using client cert");
                if self.client_cert.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "client cert is missing".to_owned(),
                    ))
                } else if self.client_key.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "client private key is missing".to_owned(),
                    ))
                } else if self.ca_cert.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "CA cert is missing".to_owned(),
                    ))
                } else if self.domain.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "domain is missing".to_owned(),
                    ))
                } else {
                    Ok(Some(TlsProfileConfig::File(TlsClientConfig {
                        client_cert: self.client_cert.unwrap(),
                        client_key: self.client_key.unwrap(),
                        ca_cert: self.ca_cert.unwrap(),
                        domain: self.domain.unwrap(),
                    })))
                }
            } else {
                debug!("using no cert verification");
                Ok(Some(TlsProfileConfig::NoVerification))
            }
        } else {
            debug!("no tls detected");
            Ok(None)
        }
    }

    pub fn try_into_inline(self) -> Result<Option<TlsProfileConfig>, IoError> {
        if self.tls {
            debug!("using tls");
            if self.enable_client_cert {
                debug!("using client cert");
                if self.client_cert.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "client cert is missing".to_owned(),
                    ))
                } else if self.client_key.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "client private key is missing".to_owned(),
                    ))
                } else if self.ca_cert.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "CA cert is missing".to_owned(),
                    ))
                } else if self.domain.is_none() {
                    Err(IoError::new(
                        ErrorKind::InvalidInput,
                        "domain is missing".to_owned(),
                    ))
                } else {
                    let mut config = TlsClientConfig::default();
                    config.set_ca_cert_from(self.ca_cert.unwrap())?;
                    config.set_client_cert_from(self.client_cert.unwrap())?;
                    config.set_client_key_from(self.client_key.unwrap())?;
                    config.domain = self.domain.unwrap();
                    Ok(Some(TlsProfileConfig::Inline(config)))
                }
            } else {
                debug!("using no cert verification");
                Ok(Some(TlsProfileConfig::NoVerification))
            }
        } else {
            debug!("no tls detected");
            Ok(None)
        }
    }
}
