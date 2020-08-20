use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::debug;
use tracing::trace;


use kf_protocol::api::RecordSet;
use kf_protocol::api::PartitionOffset;
use kf_protocol::api::ReplicaKey;
use kf_protocol::message::fetch::FetchablePartitionResponse;
use fluvio_dataplane_api::server::fetch_offset::{FlvFetchOffsetsRequest};
use fluvio_dataplane_api::server::fetch_offset::FetchOffsetPartitionResponse;

use crate::ClientError;
use crate::client::*;
use crate::params::*;


// connection to SPU
pub struct SpuClient {
    client: RawClient
}

impl SpuClient {
    pub(crate) fn new(client: RawClient) -> Self {
        Self { client}
    }

    

    fn client(&self) -> &RawClient {
        &self.client
    }

    
    
    async fn fetch_offsets(&mut self,replica: &ReplicaKey) -> Result<FetchOffsetPartitionResponse, ClientError> {
        debug!("fetching offset for replica: {}", replica);

        let response = self
            .client
            .send_receive(FlvFetchOffsetsRequest::new(
                replica.topic.to_owned(),
                replica.partition,
            ))
            .await?;

        trace!(
            "receive fetch response replica: {}, {:#?}",
            replica,
            response
        );

        match response.find_partition(&replica) {
            Some(partition_response) => Ok(partition_response),
            None => Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "no replica offset for: {}",
                    replica
                ),
            )
            .into()),
        }
    }

    /// depends on offset option, calculate offset
    
    async fn calc_offset(&mut self, replica: &ReplicaKey,offset: FetchOffset) -> Result<i64, ClientError> {
        Ok(match offset {
            FetchOffset::Offset(inner_offset) => inner_offset,
            FetchOffset::Earliest(relative_offset) => {
                let offsets = self.fetch_offsets(replica).await?;
                offsets.start_offset() + relative_offset.unwrap_or(0)
            }
            FetchOffset::Latest(relative_offset) => {
                let offsets = self.fetch_offsets(replica).await?;
                offsets.last_stable_offset() - relative_offset.unwrap_or(0)
            }
        })
    }

    
    async fn fetch_logs_once(
        &mut self,
        replica: &ReplicaKey,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, ClientError> {
        use kf_protocol::message::fetch::DefaultKfFetchRequest;
        use kf_protocol::message::fetch::FetchPartition;
        use kf_protocol::message::fetch::FetchableTopic;

        debug!(
            "starting fetch log once: {:#?} {} partition to {}",
            offset_option,
            replica,
            self.client
        );

        let offset = self.calc_offset(replica,offset_option).await?;

        let partition = FetchPartition {
            partition_index: replica.partition,
            fetch_offset: offset,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let topic_request = FetchableTopic {
            name: replica.topic.to_owned(),
            fetch_partitions: vec![partition],
            ..Default::default()
        };

        let fetch_request = DefaultKfFetchRequest {
            topics: vec![topic_request],
            isolation_level: option.isolation,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let response = self.client.send_receive(fetch_request).await?;

        debug!(
            "received fetch logs for {}",
            replica
        );

        if let Some(partition_response) = response.find_partition(&replica.topic,replica.partition) {
            debug!(
                "found partition response with: {} batches",
                partition_response.records.batches.len()
            );
            Ok(partition_response)
        } else {
            Err(ClientError::PartitionNotFound(
                replica.clone()
            ))
        }
    }

    /// Sends record to a target server (Kf, SPU, or SC)
    pub async fn send_record(&mut self, replica: &ReplicaKey, record: Vec<u8>) -> Result<(), ClientError> {

        use kf_protocol::message::produce::DefaultKfProduceRequest;
        use kf_protocol::message::produce::DefaultKfPartitionRequest;
        use kf_protocol::message::produce::DefaultKfTopicRequest;
        use kf_protocol::api::DefaultBatch;
        use kf_protocol::api::DefaultRecord;


        // build produce log request message
        let mut request = DefaultKfProduceRequest::default();
        let mut topic_request = DefaultKfTopicRequest::default();
        let mut partition_request = DefaultKfPartitionRequest::default();

        debug!("send record {} bytes to: replica: {}, {}", record.len(), replica, self.client);

        let record_msg: DefaultRecord = record.into();
        let mut batch = DefaultBatch::default();
        batch.records.push(record_msg);

        partition_request.partition_index = replica.partition;
        partition_request.records.batches.push(batch);
        topic_request.name = replica.topic.to_owned();
        topic_request.partitions.push(partition_request);

        request.acks = 1;
        request.timeout_ms = 1500;
        request.topics.push(topic_request);

        trace!("produce request: {:#?}", request);

        let response = self.client.send_receive(request).await?;

        trace!("received response: {:?}", response);

        // process response
        match response.find_partition_response(&replica.topic,replica.partition) {
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

    /*
    /// Fetch log records from a target server
    fn fetch_logs<'a>(
        &'a mut self,
        _offset: FetchOffset,
        _config: FetchLogOption,
    ) -> BoxStream<'a, FetchablePartitionResponse<RecordSet>> {
        empty().boxed()
    }
    */
}

/*
pub struct FetchStream(SpuReplicaLeader);

impl Stream for FetchStream {
    type Item = FetchablePartitionResponse<RecordSet>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
*/