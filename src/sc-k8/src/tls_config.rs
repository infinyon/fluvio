//!
//! # Streaming Controller Configurations
//!
//! Stores configuration parameter used by Streaming Controller module.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;

use structopt::StructOpt;

use flv_future_aio::net::tls::AcceptorBuilder;
use flv_future_aio::net::tls::TlsAcceptor;


#[derive(Debug, StructOpt, Default )]
pub struct TlsConfig {

    /// enable tls 
    #[structopt(long)]
    pub tls: bool,

    /// path to server certificate, required when tls is enabled
    #[structopt(long)]
    pub server_cert: Option<String>,
    #[structopt(long)]
    /// path to server private key, required when tls is enabled
    pub server_key: Option<String>,
    /// enable client cert, valid when tls is enabled
    #[structopt(long)]
    pub enable_client_cert: bool,
    /// path to ca cert, required when client cert is enabled
    #[structopt(long)]
    pub ca_cert: Option<String>
}


impl TryFrom<TlsConfig> for TlsAcceptor {
    type Error = IoError;

    fn try_from(config: TlsConfig) -> Result<Self, IoError> {

        let server_crt_path = config.server_cert.ok_or_else(|| IoError::new(ErrorKind::NotFound, "missing ca cert"))?;
        let server_key_pat = config.server_key.ok_or_else(|| IoError::new(ErrorKind::NotFound,"missing ca key"))?;

        if config.enable_client_cert {
            Ok(AcceptorBuilder::new_no_client_authentication()
                .load_server_certs(server_crt_path,server_key_pat)?
                .build())
        } else {
            if let Some(ca_cert_path) = config.ca_cert {
                Ok(AcceptorBuilder::new_client_authenticate(ca_cert_path)?
                .load_server_certs(server_crt_path,server_key_pat)?
                .build())
            } else {
                return Err(IoError::new(ErrorKind::NotFound, "missing ca_cert"));
            }
        }

    }
}
