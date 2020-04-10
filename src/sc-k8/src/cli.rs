//!
//! # CLI for Streaming Controller
//!
//! Command line interface to provision SC id and bind-to server/port.
//! Parameters are overwritten in the following sequence:
//!     1) default values
//!     2) custom configuration if provided, or default configuration (if not)
//!     3) cli parameters
//!
use std::process;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::info;
use log::debug;
use structopt::StructOpt;

use types::print_cli_err;
use k8_client::K8Config;
use flv_sc_core::config::ScConfig;
use flv_future_aio::net::tls::TlsAcceptor;
use flv_future_aio::net::tls::AcceptorBuilder;

use crate::ScK8Error;


/// cli options
#[derive(Debug, StructOpt, Default)]
#[structopt(name = "sc-server", about = "Streaming Controller")]
pub struct ScOpt {
    #[structopt(long)]
    /// Address for external service
    bind_public: Option<String>,

    #[structopt(long)]
    /// Address for internal service
    bind_private: Option<String>,

    // k8 namespace
    #[structopt(short = "n", long = "namespace", value_name = "namespace")]
    namespace: Option<String>,

    #[structopt(flatten)]
    tls: TlsConfig,
}

impl ScOpt {


    fn get_sc_and_k8_config() -> Result<(ScConfig, K8Config, Option<(TlsAcceptor,String)>), ScK8Error> {
        let mut sc_opt = ScOpt::from_args();

        let k8_config = K8Config::load().expect("no k8 config founded");

        // if name space is specified, use one from k8 config
        if sc_opt.namespace.is_none() {
            let k8_namespace = k8_config.namespace().to_owned();
            info!(
                "using {} as namespace from kubernetes config",
                k8_namespace
            );
            sc_opt.namespace = Some(k8_namespace);
        }

        let tls_acceptor = sc_opt.try_build_tls_acceptor()?;
        let (sc_config,tls_addr_opt) = sc_opt.as_sc_config()?;
        let tls_config = match tls_acceptor {
            Some(acceptor) => Some((acceptor,tls_addr_opt.unwrap())),
            None => None
        };

        Ok((sc_config, k8_config,tls_config))
    }

    fn as_sc_config(self) -> Result<(ScConfig,Option<String>),IoError> {

        let mut config = ScConfig::default();

        // apply our option
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
        config.namespace = self.namespace.unwrap().clone();
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

/// return SC configuration or exist program.
pub fn parse_cli_or_exit() -> (ScConfig, K8Config,Option<(TlsAcceptor,String)>) {
    match ScOpt::get_sc_and_k8_config() {
        Err(err) => {
            print_cli_err!(err);
            process::exit(0x0100);
        }
        Ok(config) => config,
    }
}



#[derive(Debug, StructOpt, Default )]
struct TlsConfig {

    /// enable tls 
    #[structopt(long)]
    tls: bool,

    /// TLS: path to server certificate
    #[structopt(long)]
    server_cert: Option<String>,

    #[structopt(long)]
    /// TLS: path to server private key
    server_key: Option<String>,

    /// TLS: enable client cert
    #[structopt(long)]
    enable_client_cert: bool,

    /// TLS: path to ca cert, required when client cert is enabled
    #[structopt(long)]
    ca_cert: Option<String>,

    #[structopt(long)]
    /// TLS: address of non tls public service, required
    bind_non_tls_public: Option<String>,
}
