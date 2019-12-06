//!
//! # Fluvio SC Consume Log
//!
//! Looks-up the SPU responsible for the topic/partition, connect to the server
//! and read logs.
//!

use std::net::SocketAddr;

use future_helper::run_block_on;
use kf_protocol::message::fetch::DefaultKfFetchResponse;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::find_spu_leader_for_topic_partition;
use crate::common::sc_get_api_versions;

use super::{FetchLogParams, ReponseLogParams};

use super::logs_fetch::fetch_log_msgs;
use super::logs_output::process_fetch_topic_reponse;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

/// Connect to Fluvio Streaming Controller to look-up SPU and read log
pub fn sc_consume_log_from_topic_partition(
    server_addr: SocketAddr,
    log_params: FetchLogParams,
    response_params: ReponseLogParams,
) -> Result<(), CliError> {
    let response = run_block_on(find_spu_consume_log_server(server_addr, log_params))?;

    // process logs response
    process_fetch_topic_reponse(&response, &response_params)
}

/// Connect to SC Controller, find spu, and send log
async fn find_spu_consume_log_server(
    sc_addr: SocketAddr,
    log_params: FetchLogParams,
) -> Result<DefaultKfFetchResponse, CliError> {
    let mut conn = Connection::new(&sc_addr).await?;
    let sc_vers = sc_get_api_versions(&mut conn).await?;

    // find spu
    let spu_addr = find_spu_leader_for_topic_partition(
        &mut conn,
        log_params.topic.clone(),
        log_params.partition,
        &sc_vers,
    )
    .await?;

    // fetch logs
    fetch_log_msgs(spu_addr, log_params).await
}
