use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use log::trace;
use async_trait::async_trait;

use types::socket_helpers::ServerAddress;
use kf_protocol::message::fetch::FetchablePartitionResponse;
use kf_protocol::api::DefaultRecords;
use kf_protocol::api::PartitionOffset;
use kf_protocol::message::fetch::DefaultKfFetchRequest;
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::FetchableTopic;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::DefaultKfPartitionRequest;
use kf_protocol::message::produce::DefaultKfTopicRequest;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;
use kf_protocol::api::Isolation;
use kf_protocol::api::ErrorCode as KfErrorCode;
use kf_socket::KfSocketError;

use crate::LeaderConfig;
use crate::ClientError;
use crate::Client;




/// features for Replica Leader (topic,partition)
#[async_trait]
pub trait ReplicaLeader: Send + Sync {

    type OffsetPartitionResponse: PartitionOffset;

    
    fn config(&self) -> &LeaderConfig;

    fn client(&mut self) -> &mut Client<String>;


    fn topic(&self) -> &str {
        &self.config().topic
    }

    fn partition(&self) -> i32 {
        self.config().partition
    }
    
    fn client_id(&self) -> &str {
        &self.config().client_id
    }

    fn addr(&self) -> &ServerAddress {
        &self.config().addr
    }


     // fetch offsets for 
    async fn fetch_offsets(&mut self) -> Result<Self::OffsetPartitionResponse, ClientError >;

    /// Fetch log records from a target server
    async fn fetch_logs(
        &mut self,
        offset: i64,
        max_bytes: i32,
    ) -> Result<FetchablePartitionResponse<DefaultRecords>, KfSocketError> {

        let topic_request = FetchableTopic {
            name: self.topic().to_owned(),
            fetch_partitions: vec![
                FetchPartition {
                    partition_index: self.partition(),
                    current_leader_epoch:  -1,
                    fetch_offset: offset,
                    log_start_offset: -1,
                    max_bytes
                }]};

        let request = DefaultKfFetchRequest {
            replica_id: -1,
            max_wait: 500,
            min_bytes: 1,
            max_bytes,
            isolation_level: Isolation::ReadCommitted,
            session_id: 0,
            epoch: -1,
            topics: vec![topic_request],
            ..Default::default()
        };
        

        debug!(
            "fetch logs '{}' ({}) partition to {}",
            self.topic(),
            self.partition(),
            self.addr()
        );

        trace!("fetch logs req {:#?}", request);

        let response = self.client().send_receive(request).await?;


        if response.error_code != KfErrorCode::None {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                format!("fetch: {}", response.error_code.to_sentence())
            ).into());
        }

        match response.find_partition(self.topic(),self.partition()) {
            None => Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!("no topic: {}, partition: {} founded",self.topic(),self.partition())
                ).into()),
            Some(partition_response) => {
                if partition_response.error_code != KfErrorCode::None {
                    return Err(IoError::new(
                        ErrorKind::InvalidData,
                        format!("fetch: {}", partition_response.error_code.to_sentence())
                    ).into());
                }
                Ok(partition_response)
            }
        }
        
    }

    /// Sends record to a target server (Kf, SPU, or SC)
    async fn send_record(
        &mut self,
        record: Vec<u8>,
    ) -> Result<(), ClientError> {

       
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

        let response = self.client().send_receive(request).await?;

        trace!("received response: {:?}", response);

        // process response
        match response.find_partition_response(self.topic(),self.partition()) {
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

