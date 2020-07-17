use core::pin::Pin;
use core::task::Poll;
use core::task::Context;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use log::trace;
use futures::stream::Stream;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::stream::empty;
use async_trait::async_trait;

use kf_protocol::api::RecordSet;
use kf_protocol::api::PartitionOffset;
use kf_protocol::message::fetch::FetchablePartitionResponse;
use spu_api::server::fetch_offset::{FlvFetchOffsetsRequest};
use spu_api::server::fetch_offset::FetchOffsetPartitionResponse;

use crate::ClientError;
use crate::client::*;
use crate::ReplicaLeaderConfig;
use crate::ReplicaLeader;
use crate::FetchLogOption;
use crate::FetchOffset;

// Access specific replica leader of the SPU
// for now, we use string to store address, later, we might store address natively
pub struct SpuReplicaLeader {
    client: RawClient,
    config: ReplicaLeaderConfig,
}

impl SpuReplicaLeader {
    pub(crate) fn new(config: ReplicaLeaderConfig, client: RawClient) -> Self {
        Self { client, config }
    }

    pub fn config(&self) -> &ReplicaLeaderConfig {
        &self.config
    }

    


    /// depends on offset option, calculate offset
    async fn calc_offset(&mut self, offset: FetchOffset) -> Result<i64, ClientError> {
        Ok(match offset {
            FetchOffset::Offset(inner_offset) => inner_offset,
            FetchOffset::Earliest(relative_offset) => {
                let offsets = self.fetch_offsets().await?;
                offsets.start_offset() + relative_offset.unwrap_or(0)
            }
            FetchOffset::Latest(relative_offset) => {
                let offsets = self.fetch_offsets().await?;
                offsets.last_stable_offset() - relative_offset.unwrap_or(0)
            }
        })
    }

    

    fn client(&self) -> &RawClient {
        &self.client
    }

    
}

#[async_trait]
impl ReplicaLeader for SpuReplicaLeader {
    type OffsetPartitionResponse = FetchOffsetPartitionResponse;
    type Client = RawClient;

    fn config(&self) -> &ReplicaLeaderConfig {
        &self.config
    }

    fn addr(&self) -> &str {
        &self.client.config().addr()
    }

    fn mut_client(&mut self) -> &mut Self::Client {
        &mut self.client
    }
    
    async fn fetch_offsets(&mut self) -> Result<FetchOffsetPartitionResponse, ClientError> {
        debug!("fetching offset for: {}:{}", self.topic(), self.partition());
        let response = self
            .client
            .send_receive(FlvFetchOffsetsRequest::new(
                self.topic().to_owned(),
                self.partition(),
            ))
            .await?;

        trace!(
            "receive topic {}:{}  offset: {:#?}",
            self.topic(),
            self.partition(),
            response
        );

        match response.find_partition(self.topic(), self.partition()) {
            Some(partition_response) => Ok(partition_response),
            None => Err(IoError::new(
                ErrorKind::InvalidData,
                format!(
                    "no topic: {}, partition: {} founded in offset",
                    self.topic(),
                    self.partition()
                ),
            )
            .into()),
        }
    }

    async fn fetch_logs_once(
        &mut self,
        offset_option: FetchOffset,
        option: FetchLogOption,
    ) -> Result<FetchablePartitionResponse<RecordSet>, ClientError> {
        use kf_protocol::message::fetch::DefaultKfFetchRequest;
        use kf_protocol::message::fetch::FetchPartition;
        use kf_protocol::message::fetch::FetchableTopic;

        debug!(
            "starting fetch log once: {:#?} '{}' ({}) partition to {}",
            offset_option,
            self.topic(),
            self.partition(),
            self.addr(),
        );

        let offset = self.calc_offset(offset_option).await?;

        let partition = FetchPartition {
            partition_index: self.partition(),
            fetch_offset: offset,
            max_bytes: option.max_bytes,
            ..Default::default()
        };

        let topic_request = FetchableTopic {
            name: self.topic().into(),
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
            "received fetch logs for {}-{}",
            self.topic(),
            self.partition()
        );

        if let Some(partition_response) = response.find_partition(&self.topic(), self.partition()) {
            debug!(
                "found partition response with: {} batches",
                partition_response.records.batches.len()
            );
            Ok(partition_response)
        } else {
            Err(ClientError::PartitionNotFound(
                self.topic().to_owned(),
                self.partition(),
            ))
        }
    }

    /// Fetch log records from a target server
    fn fetch_logs<'a>(
        &'a mut self,
        _offset: FetchOffset,
        _config: FetchLogOption,
    ) -> BoxStream<'a, FetchablePartitionResponse<RecordSet>> {
        empty().boxed()
    }
}

pub struct FetchStream(SpuReplicaLeader);

impl Stream for FetchStream {
    type Item = FetchablePartitionResponse<RecordSet>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}
