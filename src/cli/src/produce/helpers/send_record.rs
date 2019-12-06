//!
//! # Fluvio Produce Log
//!
//! Takes user input and sends to the SPU, SC, or KF servers.
//!

use std::net::SocketAddr;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::{debug, trace};

use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::DefaultKfPartitionRequest;
use kf_protocol::message::produce::DefaultKfTopicRequest;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;

use crate::error::CliError;
use crate::common::Connection;

/// Sends record to a target server (Kf, SPU, or SC)
pub async fn send_log_record_to_server(
    addr: SocketAddr,
    topic: String,
    partition: i32,
    record: Vec<u8>,
    version: Option<i16>,
) -> Result<(), CliError> {
    let mut conn = Connection::new(&addr).await?;
    let topic_name = topic.clone();

    // build produce log request message
    let mut request = DefaultKfProduceRequest::default();
    let mut topic_request = DefaultKfTopicRequest::default();
    let mut partition_request = DefaultKfPartitionRequest::default();

    debug!("send record {} bytes to: {}", record.len(), addr);

    let record_msg: DefaultRecord = record.into();
    let mut batch = DefaultBatch::default();
    batch.records.push(record_msg);

    partition_request.partition_index = partition;
    partition_request.records.batches.push(batch);
    topic_request.name = topic;
    topic_request.partitions.push(partition_request);

    request.acks = 1;
    request.timeout_ms = 1500;
    request.topics.push(topic_request);

    trace!("produce request: {:#?}", request);

    let response = conn.send_request(request, version).await?;

    trace!("received response: {:?}", response);

    // process response
    match response.find_partition_response(&topic_name, partition) {
        Some(partition_response) => {
            if partition_response.error_code.is_error() {
                return Err(CliError::IoError(IoError::new(
                    ErrorKind::Other,
                    format!("{}", partition_response.error_code.to_sentence()),
                )));
            }
            Ok(())
        }
        None => Err(CliError::IoError(IoError::new(
            ErrorKind::Other,
            "unknown error",
        ))),
    }
}
