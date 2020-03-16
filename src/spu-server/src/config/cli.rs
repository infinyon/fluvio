//!
//! # CLI for Streaming Processing Unit (SPU)
//!
//! Command line interface to provision SPU id and configure various
//! system parameters.
//!
use std::io::Error as IoError;
use std::process;
use std::path::PathBuf;

use log::trace;
use log::debug;
use log::warn;
use log::info;
use structopt::StructOpt;

use types::print_cli_err;

use super::{SpuConfig, SpuConfigFile};

/// cli options
#[derive(Debug, Default, StructOpt)]
#[structopt(name = "spu-server", about = "Streaming Processing Unit")]
pub struct SpuOpt {
    /// SPU unique identifier
    #[structopt(short = "i", long = "id", value_name = "integer")]
    pub id: Option<i32>,

 

    #[structopt(short = "p", long = "public-server", value_name = "host:port")]
    /// Spu server for external communication
    pub public_server: Option<String>,

    #[structopt(short = "v", long = "private-server", value_name = "host:port")]
    /// Spu server for internal cluster communication
    pub private_server: Option<String>,

    /// Address of the SC Server
    #[structopt(short = "c", long = "sc-controller", value_name = "host:port")]
    pub sc_server: Option<String>,

    #[structopt(short = "f", long = "conf", value_name = "file")]
    /// Configuration file
    pub config_file: Option<PathBuf>,

    /// Reset base directory 
    #[structopt(short,long)]
    pub reset: bool
}

/// Run SPU Cli and return SPU configuration. Errors are consider fatal
/// and the program exits.
pub fn process_spu_cli_or_exit() -> SpuConfig {
    match get_spu_config() {
        Err(err) => {
            print_cli_err!(err);
            process::exit(0x0100);
        }
        Ok(config) => config,
    }
}

/// Validate SPU (Streaming Processing Unit) cli inputs and generate SpuConfig
pub fn get_spu_config() -> Result<SpuConfig, IoError> {
    let cfg = SpuOpt::from_args();

    let reset = cfg.reset;

    // generate config from file from user-file or default (if exists)
    let spu_config_file = match &cfg.config_file {
        Some(cfg_file) => Some(SpuConfigFile::from_file(&cfg_file)?),
        None => SpuConfigFile::from_default_file()?,
    };

    trace!("spu cli: {:#?}, file: {:#?}",cfg,spu_config_file);
    // send config file and cli parameters to generate final config.
    let config = SpuConfig::new_from_all(cfg, spu_config_file)?;
    debug!("config: {:#?}",config);

    if reset {
        warn!("reset base directory: {:#?}",config.log.base_dir);
        if let Err(err) =  config.log.reset_base_dir() {
            info!("unable to reset base directory: {}",err);
        }
    }

    Ok(config)
}
