//!
//! # List Auth Tokens CLI
//!
//! CLI tree to fetch and list one or more Auth Tokens
//!
use std::io::Error as IoError;
use std::io::ErrorKind;

use structopt::StructOpt;

use crate::error::CliError;
use crate::common::OutputType;
use crate::profile::{ProfileConfig, TargetServer};

use super::process_sc_list_auth_tokens;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListAuthTokensOpt {
    /// Token names
    #[structopt(short = "n", long = "token-name", value_name = "string")]
    token_names: Vec<String>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    ///Profile name
    #[structopt(short = "P", long = "profile")]
    pub profile: Option<String>,

    /// Output
    #[structopt(
        short = "O",
        long = "output",
        value_name = "type",
        raw(possible_values = "&OutputType::variants()", case_insensitive = "true")
    )]
    output: Option<OutputType>,
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct ListAuthTokensConfig {
    pub auth_tokens: Option<Vec<String>>,
    pub output: OutputType,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Parse CLI, build SC address, query server and display result.
pub fn process_list_auth_tokens(opt: ListAuthTokensOpt) -> Result<(), CliError> {
    let (target_server, list_tokens_cfg) = parse_opt(opt)?;

    match target_server {
        TargetServer::Sc(server_addr) => process_sc_list_auth_tokens(server_addr, list_tokens_cfg),
        TargetServer::Kf(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "Kafka does not support list auth-tokens",
        ))),
        TargetServer::Spu(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "SPU does not implement list auth-tokens",
        ))),
    }
}

/// Validate cli options, parse profile, generate target server and create result object.
fn parse_opt(opt: ListAuthTokensOpt) -> Result<(TargetServer, ListAuthTokensConfig), CliError> {
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;
    let auth_tokens = if opt.token_names.len() > 0 {
        Some(opt.token_names.clone())
    } else {
        None
    };

    // transfer config parameters
    let config = ListAuthTokensConfig {
        auth_tokens: auth_tokens,
        output: opt.output.unwrap_or(OutputType::default()),
    };

    // return server separately from config result
    Ok((target_server, config))
}
