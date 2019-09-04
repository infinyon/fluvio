//!
//! # Fluvio SC - Query Auth Tokens
//!
//! Communicates with Fluvio Streaming Controller to retrieve Auth Tokens and convert
//! them to ScAuthTokenMetadata
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use serde::Serialize;
use log::trace;
use types::{TokenName, SpuId};

use future_helper::run_block_on;

use sc_api::apis::ScApiKey;
use sc_api::auth_tokens::{FlvFetchAuthTokensRequest, FlvFetchAuthTokensResponse};
use sc_api::auth_tokens::FetchAuthTokenResponse;
use sc_api::auth_tokens::FetchAuthToken;
use sc_api::auth_tokens::FlvTokenType;
use sc_api::auth_tokens::FlvTokenResolution;
use sc_api::errors::FlvErrorCode;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::sc_get_api_versions;
use crate::common::sc_lookup_version;

// -----------------------------------
// ScAuthTokenMetadata (Serializable)
// -----------------------------------

#[derive(Serialize, Debug)]
pub struct ScAuthTokenMetadata {
    pub name: TokenName,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub auth_token: Option<AuthToken>,

    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<FlvErrorCode>,
}

#[derive(Serialize, Debug)]
pub struct AuthToken {
    pub token_type: TokenType,
    pub min_spu: SpuId,
    pub max_spu: SpuId,
    pub resolution: TokenResolution,
    pub reason: String,
}

#[derive(Serialize, Debug)]
pub enum TokenType {
    Any,
    Custom,
    Managed,
}

#[derive(Serialize, Debug)]
pub enum TokenResolution {
    Ok,
    Init,
    Invalid,
}

// -----------------------------------
// Implementation
// -----------------------------------

impl ScAuthTokenMetadata {
    pub fn new(fetched_auth_token: &FetchAuthTokenResponse) -> Self {
        let auth_token = if let Some(token) = &fetched_auth_token.auth_token {
            Some(AuthToken::new(token))
        } else {
            None
        };

        // if error is present, convert it
        let error_code = if fetched_auth_token.error_code.is_error() {
            Some(fetched_auth_token.error_code)
        } else {
            None
        };

        // topic metadata with all parameters converted
        ScAuthTokenMetadata {
            name: fetched_auth_token.name.clone(),
            auth_token: auth_token,
            error: error_code,
        }
    }
}

impl AuthToken {
    pub fn new(fetched_token: &FetchAuthToken) -> Self {
        let token_type = TokenType::new(&fetched_token.token_type);
        let resolution = TokenResolution::new(&fetched_token.resolution);

        AuthToken {
            token_type: token_type,
            min_spu: fetched_token.min_spu,
            max_spu: fetched_token.max_spu,
            resolution: resolution,
            reason: fetched_token.reason.clone(),
        }
    }
}

impl TokenType {
    pub fn new(fetch_token_type: &FlvTokenType) -> Self {
        match fetch_token_type {
            FlvTokenType::Any => TokenType::Any,
            FlvTokenType::Managed => TokenType::Managed,
            FlvTokenType::Custom => TokenType::Custom,
        }
    }

    pub fn type_label(token_type: &TokenType) -> &'static str {
        match token_type {
            TokenType::Any => "any",
            TokenType::Managed => "managed",
            TokenType::Custom => "custom",
        }
    }
}

impl TokenResolution {
    pub fn new(fetch_token_resolution: &FlvTokenResolution) -> Self {
        match fetch_token_resolution {
            FlvTokenResolution::Ok => TokenResolution::Ok,
            FlvTokenResolution::Init => TokenResolution::Init,
            FlvTokenResolution::Invalid => TokenResolution::Invalid,
        }
    }

    pub fn resolution_label(resolution: &TokenResolution) -> &'static str {
        match resolution {
            TokenResolution::Ok => "ok",
            TokenResolution::Init => "initializing",
            TokenResolution::Invalid => "invalid",
        }
    }
}

// -----------------------------------
// Query Server & Convert to Metadata
// -----------------------------------

/// Query Fluvio SC server for Auth Topics and output to screen
pub fn query_sc_list_auth_tokens(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<Vec<ScAuthTokenMetadata>, CliError> {
    match run_block_on(sc_fetch_auth_tokens(server_addr, names)) {
        Err(err) => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            format!("cannot retrieve auth topics: {}", err),
        ))),
        Ok(fetch_token_res) => {
            let mut auth_tokens: Vec<ScAuthTokenMetadata> = vec![];
            for auth_token_res in &fetch_token_res.auth_tokens {
                auth_tokens.push(ScAuthTokenMetadata::new(auth_token_res));
            }
            Ok(auth_tokens)
        }
    }
}

// Query SC Auth Tokens
async fn sc_fetch_auth_tokens<'a>(
    server_addr: SocketAddr,
    names: Option<Vec<String>>,
) -> Result<FlvFetchAuthTokensResponse, CliError> {
    // look-up version
    let mut conn = Connection::new(&server_addr).await?;
    let versions = sc_get_api_versions(&mut conn).await?;
    let version = sc_lookup_version(ScApiKey::FlvFetchAuthTokens, &versions);

    // generate request
    let mut request = FlvFetchAuthTokensRequest::default();
    request.names = names;

    trace!(
        "fetch auth-tokens req '{}': {:#?}",
        conn.server_addr(),
        request
    );

    let response = conn.send_request(request, version).await?;

    trace!(
        "fetch auth-tokens '{}': {:#?}",
        conn.server_addr(),
        response
    );

    Ok(response)
}