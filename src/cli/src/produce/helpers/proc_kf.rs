//!
//! # Kafka Produce Log
//!
//! Connects to Kafka server, identify Brooker and sends the log.
//!

use std::io;
use std::io::prelude::*;
use std::net::SocketAddr;

use types::{print_cli_err, print_cli_ok};
use future_helper::run_block_on;

use kf_protocol::message::KfApiVersions;
use kf_protocol::api::AllKfApiKey;

use crate::error::CliError;
use crate::common::Connection;
use crate::common::{kf_lookup_version, kf_get_api_versions};
use crate::common::find_broker_leader_for_topic_partition;

use crate::produce::cli::RecordTouples;

use super::send_log_record_to_server;

// -----------------------------------
//  Kafka - Process Request
// -----------------------------------

/// Dispatch records based on the content of the record_touples variable
pub fn process_kf_produce_record(
    server_addr: SocketAddr,
    topic: String,
    partition: i32,
    record_touples: RecordTouples,
    continous: bool
) -> Result<(), CliError> {
    // look-up lookup spu for topic/partition leader
    let broker_addr = run_block_on(find_leader_broker(server_addr, topic.clone(), partition))?;

    // get versions
    let versions = run_block_on(get_server_versions(server_addr))?;

    // send records to Broker
    send_records_to_broker(broker_addr, topic, partition, record_touples, versions,continous)
}

// Connect to server, get versions and find broker
async fn find_leader_broker(
    kf_ctrl_addr: SocketAddr,
    topic: String,
    partition: i32,
) -> Result<SocketAddr, CliError> {
    let mut conn = Connection::new(&kf_ctrl_addr).await?;
    let versions = kf_get_api_versions(&mut conn).await?;

    // find broker
    find_broker_leader_for_topic_partition(&mut conn, topic, partition, &versions).await
}

// Connect to server, get versions
async fn get_server_versions(socket_addr: SocketAddr) -> Result<KfApiVersions, CliError> {
    let mut conn = Connection::new(&socket_addr).await?;
    kf_get_api_versions(&mut conn).await
}

/// Dispatch records based on the content of the record_touples variable
fn send_records_to_broker(
    broker_addr: SocketAddr,
    topic: String,
    partition: i32,
    record_touples: RecordTouples,
    versions: KfApiVersions,
    continous: bool
) -> Result<(), CliError> {
    let version = kf_lookup_version(AllKfApiKey::Produce, &versions);

    // in both cases, exit loop on error
    if record_touples.len() > 0 {
        // records from files
        for r_touple in record_touples {
            println!("{}", r_touple.0);
            process_record(broker_addr, topic.clone(), partition, r_touple.1, version);
        }
    } else {
        let stdin = io::stdin();
        for line in stdin.lock().lines() {
            let text = line?;
            let record = text.as_bytes().to_vec();
            process_record(broker_addr, topic.clone(), partition, record, version);
            if !continous {
                return Ok(())
            }
        }
    }

    Ok(())
}

/// Process record and print success or error
fn process_record(
    broker_addr: SocketAddr,
    topic: String,
    partition: i32,
    record: Vec<u8>,
    version: Option<i16>,
) {
    match run_block_on(send_log_record_to_server(
        broker_addr,
        topic,
        partition,
        record,
        version,
    )) {
        Ok(()) => print_cli_ok!(),
        Err(err) => print_cli_err!(format!("{}", err)),
    }
}

/*
#[cfg(test)]
mod tests {

    extern crate test;
    use std::net::SocketAddr;
    use std::io::Error as IoError;
    use std::io::ErrorKind;

    use test::Bencher;
    use super::process_record;
    fn create_sample_record() -> Vec<u8> {
        let mut record: Vec<u8> = vec![];
        for _ in 0..10000 {
            record.push(0x10);
        }
        record

    }


    #[bench]
    fn bench_kf_send_record(b: &mut Bencher) {
        let server_addr = "127.0.0.1:9092".to_owned();
        let socket_addr = server_addr
                .parse::<SocketAddr>()
                    .map_err(|err| IoError::new(ErrorKind::InvalidInput, format!("{}", err))).expect("kf server");
        b.iter(move || process_record(socket_addr,"test".to_owned(),0,create_sample_record()));
    }
}
*/
