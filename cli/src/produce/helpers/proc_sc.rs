//!
//! # Fluvio SC Produce Log
//!
//! Looks-up the SPU responsible for the topic, connects to the server
//! and sends the log.
//!

use std::net::SocketAddr;

use log::debug;
use future_helper::run_block_on;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::find_spu_leader_for_topic_partition;
use crate::common::sc_get_api_versions;

use crate::produce::cli::RecordTouples;

use super::process_spu_produce_record;

// -----------------------------------
//  Fluvio SC - Process Request
// -----------------------------------

/// Dispatch records based on the content of the record_touples variable
pub fn process_sc_produce_record(
    server_addr: SocketAddr,
    topic: String,
    partition: i32,
    record_touples: RecordTouples,
    continous: bool
) -> Result<(), CliError> {
    // look-up lookup spu for topic/partition leader
    let spu_addr = run_block_on(spu_leader_for_topic_partition(
        server_addr,
        topic.clone(),
        partition,
    ))?;

    // send records to SPU
    process_spu_produce_record(spu_addr, topic, partition, record_touples,continous)
}

// Connect to SC Controller, find spu, and send log
async fn spu_leader_for_topic_partition(
    sc_addr: SocketAddr,
    topic: String,
    partition: i32,
) -> Result<SocketAddr, CliError> {
    let mut conn = Connection::new(&sc_addr).await?;
    let sc_vers = sc_get_api_versions(&mut conn).await?;

    debug!("got sc version: {:#?}", sc_vers);
    find_spu_leader_for_topic_partition(&mut conn, topic.clone(), partition, &sc_vers).await
}
