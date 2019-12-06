//!
//! # Create Auth Tokens
//!
//! CLI tree to generate Create Auth Tokens
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use regex::Regex;
use structopt::clap::arg_enum;
use structopt::StructOpt;
use types::{TokenName, TokenSecret, SpuId};
use utils::generators::generate_secret;
use types::print_ok_msg;

use crate::error::CliError;
use crate::profile::{ProfileConfig, TargetServer};

use super::process_sc_create_auth_token;

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct CreateAuthTokenOpt {
    /// Token name
    #[structopt(short = "n", long = "token-name", value_name = "string")]
    token_name: String,

    /// Token secret of 16 characters in length
    #[structopt(
        short = "s",
        long = "secret",
        value_name = "alpha-numeric",
        required_unless = "generate_secret",
        parse(try_from_str = "parse_token_secret")
    )]
    token_secret: Option<String>,

    /// Generate a random secret
    #[structopt(short = "g", long = "generate-secret", conflicts_with = "secret")]
    generate_secret: bool,

    /// First SPU id in the match range (inclusive)
    #[structopt(short = "m", long = "min-spu", value_name = "integer")]
    min_spu: i32,

    /// Last SPU id in the match range (inclusive)
    #[structopt(short = "x", long = "max-spu", value_name = "integer")]
    max_spu: i32,

    /// Types
    #[structopt(
        short = "t",
        long = "token-type",
        value_name = "token-type",
        raw(
            possible_values = "&CliTokenType::variants()",
            case_insensitive = "true"
        )
    )]
    token_type: Option<CliTokenType>,

    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Profile name
    #[structopt(short = "P", long = "profile")]
    profile: Option<String>,
}

arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum CliTokenType {
        Custom,
        Managed,
        Any
    }
}

/// Parse Token secret (should math regex )
fn parse_token_secret(s: &str) -> Result<String, IoError> {
    let token_name = s.to_lowercase();

    let re = Regex::new(r"^[a-z0-9]{16}$").unwrap();
    if re.is_match(&token_name) {
        Ok(token_name)
    } else {
        Err(IoError::new(ErrorKind::InvalidData, format!("{}", s)))
    }
}

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct CreateAuthTokenConfig {
    pub token_name: TokenName,
    pub token_secret: TokenSecret,
    pub token_type: CliTokenType,
    pub min_spu: SpuId,
    pub max_spu: SpuId,
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process create topic cli request
pub fn process_create_auth_token(opt: CreateAuthTokenOpt) -> Result<(), CliError> {
    let (target_server, create_auth_token_cfg) = parse_opt(opt)?;
    let token_secret = create_auth_token_cfg.token_secret.clone();

    match target_server {
        TargetServer::Sc(server_addr) => {
            process_sc_create_auth_token(server_addr, create_auth_token_cfg)?;
            print_auth_token_secret(token_secret);
            Ok(())
        }
        TargetServer::Kf(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "Kafka does not support create auth-tokens",
        ))),
        TargetServer::Spu(_) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "SPU does not implement create auth-tokens",
        ))),
    }
}

/// Validate cli options. Generate target-server and create auth-token.
fn parse_opt(opt: CreateAuthTokenOpt) -> Result<(TargetServer, CreateAuthTokenConfig), CliError> {
    // profile specific configurations (target server)
    let profile_config = ProfileConfig::new(&opt.sc, &None, &opt.profile)?;
    let target_server = profile_config.target_server()?;

    // assign auth-token
    let mut token_secret = opt.token_secret.unwrap_or("".to_owned()).to_lowercase();
    if opt.generate_secret {
        token_secret = generate_secret();
    }

    // create auth-token config
    let create_auth_token_cfg = CreateAuthTokenConfig {
        token_name: opt.token_name,
        token_secret: token_secret,
        token_type: opt.token_type.unwrap_or(CliTokenType::Any),
        min_spu: opt.min_spu,
        max_spu: opt.max_spu,
    };

    // return server separately from config
    Ok((target_server, create_auth_token_cfg))
}

/// Print token secret
fn print_auth_token_secret(token_secret: String) {
    println!("Secret: {}", token_secret);
    print_ok_msg!("Important", "secret will not be displayed again!");
}
