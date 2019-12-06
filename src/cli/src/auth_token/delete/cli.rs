//!
//! # Delete Auth Tokens
//!
//! CLI tree to generate Delete Auth Tokens
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::process_sc_delete_auth_token;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct DeleteAuthTokenOpt {
    /// Token name
    #[structopt(short = "n", long = "token-name", value_name = "string")]
    token_name: String,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct DeleteAuthTokenConfig {
    pub auth_token_name: String,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process delete auth-token cli request
pub fn process_delete_auth_token(opt: DeleteAuthTokenOpt) -> Result<(), CliError> {
    let (target_server, delete_auth_token_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_sc_delete_auth_token(server_addr, delete_auth_token_cfg)
        }
        TargetServer::Kf(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "Kafka does not support delete auth-tokens",
        ))),
        TargetServer::Spu(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "SPU does not implement delete auth-tokens",
        ))),
    }
}

/// Validate cli options. Generate target-server and delete auth-token.
fn parse_opt(opt: DeleteAuthTokenOpt) -> Result<(TargetServer, DeleteAuthTokenConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let delete_auth_token_cfg = DeleteAuthTokenConfig {
        auth_token_name: opt.token_name,
    };

    // return server separately from config
    Ok((target_server, delete_auth_token_cfg))
}
