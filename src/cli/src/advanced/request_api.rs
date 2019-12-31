//!
//! # Request API
//!
//! Defines supported APIs and provides Request fuctionality
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fs::read_to_string;
use std::path::Path;

use structopt::clap::arg_enum;
use serde_json::to_string_pretty;
use serde::Serialize;
use serde::de::DeserializeOwned;

use kf_protocol::api::Request;
use fluvio_client::Client;

use crate::error::CliError;
use crate::Terminal;
use crate::t_println;

// -----------------------------------
// Request Api
// -----------------------------------

arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum RequestApi {
        ApiVersions,
        ListOffset,
        Metadata,
        LeaderAndIsr,
        FindCoordinator,
        JoinGroup,
        SyncGroup,
        LeaveGroup,
        ListGroups,
        DescribeGroups,
        DeleteGroups,
        Heartbeat,
        OffsetFetch,
    }
}

// -----------------------------------
// Implementation - Generics
// -----------------------------------

/// Parse from file and return Request object
pub async fn parse_and_pretty_from_file<P,R,O>(out: std::sync::Arc<O>,client: &mut Client<String>,file_path: P) -> Result<(), CliError>
where
    P: AsRef<Path>,
    R: DeserializeOwned + Request,
    R::Response: Serialize,
    O: Terminal
{
    let file_str: String = read_to_string(file_path)?;
    let request: R = serde_json::from_str(&file_str)
        .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
    
    pretty_pretty_spu(out,client,request).await
}

// Connect to Kafka Controller and process request
async fn pretty_pretty_spu<O,R>(out: std::sync::Arc<O>,client: &mut Client<String>, request: R) -> Result<(), CliError>
where
    R: Request,
    R::Response: Serialize,
    O: Terminal
{
    // Send request without a version number, max_version will be used

    let response = client.send_receive(request).await?;
    let result = to_string_pretty(&response)
        .map_err(|err| IoError::new(ErrorKind::InvalidData, format!("{}", err)))?;
    t_println!(out,"{}", result);
    Ok(())
}


