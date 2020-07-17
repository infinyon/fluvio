use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use log::trace;
use async_trait::async_trait;
use futures::stream::BoxStream;

use kf_protocol::message::fetch::FetchablePartitionResponse;
use kf_protocol::api::RecordSet;
use kf_protocol::api::PartitionOffset;
use kf_protocol::api::Isolation;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::DefaultKfPartitionRequest;
use kf_protocol::message::produce::DefaultKfTopicRequest;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;
use kf_protocol::api::MAX_BYTES;

use crate::ReplicaLeaderConfig;
use crate::client::*;
use crate::ClientError;

#[derive(Clone)]
pub struct FetchLogOption {
    pub max_bytes: i32,
    pub isolation: Isolation,
}

impl Default for FetchLogOption {
    fn default() -> Self {
        Self {
            max_bytes: MAX_BYTES,
            isolation: Isolation::default(),
        }
    }
}

#[derive(Debug)]
pub enum FetchOffset {
    Earliest(Option<i64>),
    /// earliest + offset
    Latest(Option<i64>),
    /// latest - offset
    Offset(i64),
}

/// Replica Leader (topic,partition)
#[async_trait]
pub trait ReplicaLeader: Send + Sync {
    type OffsetPartitionResponse: PartitionOffset;
    type Client: Client;

    fn config(&self) -> &ReplicaLeaderConfig;

    
    fn topic(&self) -> &str {
        &self.config().topic()
    }

    fn partition(&self) -> i32 {
        self.config().partition()
    }

    fn addr(&self) -> &str;

    fn mut_client(&mut self) -> &mut Self::Client;

    // fetch offsets for
    async fn fetch_offsets(&mut self) -> Result<Self::OffsetPartitionResponse, ClientError>;

    /// fetch log once
    async fn fetch_logs_once(
        &mut self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, ClientError>;

    /// fetch log as stream
    fn fetch_logs<'a>(
        &'a mut self,
        offset: FetchOffset,
        config: FetchLogOption,
    ) -> BoxStream<'a, FetchablePartitionResponse<RecordSet>>;

    /// Sends record to a target server (Kf, SPU, or SC)
    async fn send_record(&mut self, record: Vec<u8>) -> Result<(), ClientError> {
        // build produce log request message
        let mut request = DefaultKfProduceRequest::default();
        let mut topic_request = DefaultKfTopicRequest::default();
        let mut partition_request = DefaultKfPartitionRequest::default();

        debug!("send record {} bytes to: {}", record.len(), self.addr());

        let record_msg: DefaultRecord = record.into();
        let mut batch = DefaultBatch::default();
        batch.records.push(record_msg);

        partition_request.partition_index = self.partition();
        partition_request.records.batches.push(batch);
        topic_request.name = self.topic().to_owned();
        topic_request.partitions.push(partition_request);

        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);

        trace!("produce request: {:#?}", request);

        let response = self.mut_client().send_receive(request).await?;

        trace!("received response: {:?}", response);

        // process response
        match response.find_partition_response(self.topic(), self.partition()) {
            Some(partition_response) => {
                if partition_response.error_code.is_error() {
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::Other,
                        format!("{}", partition_response.error_code.to_sentence()),
                    )));
                }
                Ok(())
            }
            None => Err(ClientError::IoError(IoError::new(
                ErrorKind::Other,
                "unknown error",
            ))),
        }
    }
}
