//!
//! # Fluvio Sc -- List Auth Tokens Processing
//!
//! Retrieve all Auth Tokens and print to screen
//!

use std::net::SocketAddr;

use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use crate::error::CliError;
use crate::common::OutputType;
use crate::common::{EncoderOutputHandler, TableOutputHandler};

use crate::auth_token::query_metadata::ScAuthTokenMetadata;
use crate::auth_token::query_metadata::TokenType;
use crate::auth_token::query_metadata::TokenResolution;
use crate::auth_token::query_metadata::query_sc_list_auth_tokens;

use super::cli::ListAuthTokensConfig;

// -----------------------------------
// ListTopics Data Structure
// -----------------------------------

#[derive(Debug)]
struct ListAuthTokens {
    auth_tokens: Vec<ScAuthTokenMetadata>,
}

// -----------------------------------
// Process Request
// -----------------------------------

/// Query Fluvio SC server for Auth Topics and output to screen
pub fn process_sc_list_auth_tokens(
    server_addr: SocketAddr,
    list_tokens_cfg: ListAuthTokensConfig,
) -> Result<(), CliError> {
    let names = list_tokens_cfg.auth_tokens.clone();
    let auth_tokens = query_sc_list_auth_tokens(server_addr, names)?;
    let list_auth_tokens = ListAuthTokens { auth_tokens };

    format_response_output(&list_auth_tokens, &list_tokens_cfg.output)
}

/// Process server based on output type
fn format_response_output(
    list_auth_tokens: &ListAuthTokens,
    output_type: &OutputType,
) -> Result<(), CliError> {
    // expecting array with one or more elements
    if list_auth_tokens.auth_tokens.len() > 0 {
        if output_type.is_table() {
            list_auth_tokens.display_errors();
            list_auth_tokens.display_table(false);
        } else {
            list_auth_tokens.display_encoding(output_type)?;
        }
    } else {
        println!("No auth-tokens found");
    }
    Ok(())
}

// -----------------------------------
// Format Output
// -----------------------------------

impl TableOutputHandler for ListAuthTokens {
    /// table header implementation
    fn header(&self) -> Row {
        row!["TOKEN-NAME", "STATUS", "SPU-MIN", "SPU-MAX", "TYPE", "REASON"]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        let mut errors = vec![];
        for token_result in &self.auth_tokens {
            if let Some(error) = &token_result.error {
                errors.push(format!(
                    "AuthToken '{}': {}",
                    token_result.name,
                    error.to_sentence()
                ));
            }
        }
        errors
    }

    /// table content implementation
    fn content(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = vec![];
        for token_result in &self.auth_tokens {
            if let Some(auth_token) = &token_result.auth_token {
                rows.push(row![
                    l -> token_result.name,
                    l -> TokenResolution::resolution_label(&auth_token.resolution),
                    c -> auth_token.min_spu,
                    c -> auth_token.max_spu,
                    l -> TokenType::type_label(&auth_token.token_type),
                    l -> auth_token.reason,
                ]);
            }
        }
        rows
    }
}

impl EncoderOutputHandler for ListAuthTokens {
    /// serializable data type
    type DataType = Vec<ScAuthTokenMetadata>;

    /// serializable data to be encoded
    fn data(&self) -> &Vec<ScAuthTokenMetadata> {
        &self.auth_tokens
    }
}
