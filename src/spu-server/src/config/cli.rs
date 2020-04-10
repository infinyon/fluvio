//!
//! # CLI for Streaming Processing Unit (SPU)
//!
//! Command line interface to provision SPU id and configure various
//! system parameters.
//!
use std::io::Error as IoError;
use std::process;
use std::io::ErrorKind;

use log::debug;
use structopt::StructOpt;

use types::print_cli_err;
use flv_future_aio::net::tls::TlsAcceptor;
use flv_future_aio::net::tls::AcceptorBuilder;

use super::SpuConfig;

/// cli options
#[derive(Debug, Default, StructOpt)]
#[structopt(name = "spu-server", about = "Streaming Processing Unit")]
pub struct SpuOpt {
    /// SPU unique identifier
    #[structopt(short = "i", long = "id", value_name = "integer")]
    pub id: i32,


    #[structopt(short = "p", long = "public-server", value_name = "host:port")]
    /// Spu server for external communication
    pub bind_public: Option<String>,

    #[structopt(short = "v", long = "private-server", value_name = "host:port")]
    /// Spu server for internal cluster communication
    pub bind_private: Option<String>,

    /// Address of the SC Server
    #[structopt(long,value_name = "host:port")]
    pub sc_addr: Option<String>,

    #[structopt(flatten)]
    tls: TlsConfig,

}

impl SpuOpt  {


    /// Validate SPU (Streaming Processing Unit) cli inputs and generate SpuConfig
    pub fn get_spu_config() -> Result<(SpuConfig,Option<(TlsAcceptor,String)>), IoError> {


        let opt = SpuOpt::from_args();
    
        let tls_acceptor = opt.try_build_tls_acceptor()?;
        let (spu_config,tls_addr_opt) = opt.as_spu_config()?;
        let tls_config = match tls_acceptor {
            Some(acceptor) => Some((acceptor,tls_addr_opt.unwrap())),
            None => None
        };


        Ok((spu_config,tls_config))
    }

    fn as_spu_config(self) -> Result<(SpuConfig,Option<String>),IoError>  {

        let mut config = SpuConfig::default();

        config.id = self.id;

        if let Some(public_addr) = self.bind_public {
            config.public_endpoint = public_addr.clone();
        }
        let mut tls_port: Option<String> = None;

        if self.tls.tls {
            let proxy_addr = config.public_endpoint.clone();
            debug!("using tls proxy addr: {}",proxy_addr);
            tls_port = Some(proxy_addr);
            config.public_endpoint = self.tls.bind_non_tls_public.ok_or_else(|| 
                IoError::new(ErrorKind::NotFound, "non tls addr for public must be specified"))?;
            
        } 

        if let Some(private_addr) = self.bind_private {
            config.private_endpoint = private_addr.clone();
        }

        Ok((config,tls_port))

    }

    fn try_build_tls_acceptor(&self) -> Result<Option<TlsAcceptor>,IoError> {

        let tls_config = &self.tls;
        if !tls_config.tls {
            return Ok(None)
        }

        let server_crt_path = tls_config.server_cert.as_ref().ok_or_else(|| IoError::new(ErrorKind::NotFound, "missing server cert"))?;
        let server_key_pat = tls_config.server_key.as_ref().ok_or_else(|| IoError::new(ErrorKind::NotFound,"missing server key"))?;

        let builder = (if tls_config.enable_client_cert {
            let ca_path = tls_config.ca_cert.as_ref().ok_or_else(|| IoError::new(ErrorKind::NotFound,"missing ca cert"))?;
            AcceptorBuilder::new_client_authenticate(ca_path)?
        } else {
            AcceptorBuilder::new_no_client_authentication()
        })
            .load_server_certs(server_crt_path,server_key_pat)?;

        Ok(Some(builder.build()))

    }


}

/// Run SPU Cli and return SPU configuration. Errors are consider fatal
/// and the program exits.
pub fn process_spu_cli_or_exit() -> (SpuConfig,Option<(TlsAcceptor,String)>) {

    match SpuOpt::get_spu_config() {
        Err(err) => {
            print_cli_err!(err);
            process::exit(0x0100);
        }
        Ok(config) => config,
    }
}


/// same in the SC
#[derive(Debug, StructOpt, Default )]
struct TlsConfig {

    /// enable tls 
    #[structopt(long)]
    pub tls: bool,

    /// TLS: path to server certificate
    #[structopt(long)]
    pub server_cert: Option<String>,
    #[structopt(long)]
    /// TLS: path to server private key
    pub server_key: Option<String>,
    /// TLS: enable client cert
    #[structopt(long)]
    pub enable_client_cert: bool,
    /// TLS: path to ca cert, required when client cert is enabled
    #[structopt(long)]
    pub ca_cert: Option<String>,

    #[structopt(long)]
    /// TLS: address of non tls public service, required
    pub bind_non_tls_public: Option<String>,
}

