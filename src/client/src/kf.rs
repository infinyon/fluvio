use std::default::Default;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;
use core::pin::Pin;
use core::task::Poll;
use core::task::Context;

use log::debug;
use log::trace;
use async_trait::async_trait;
use futures::Stream;
use futures::stream::StreamExt;
use futures::stream::empty;
use futures::stream::BoxStream;

use flv_future_aio::net::ToSocketAddrs;

use kf_protocol::message::fetch::DefaultKfFetchRequest;
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::FetchableTopic;
use kf_protocol::message::fetch::FetchablePartitionResponse;
use kf_protocol::message::fetch::DefaultKfFetchResponse;
use kf_protocol::message::fetch::KfFetchRequest;
use kf_protocol::message::metadata::{KfMetadataRequest, KfMetadataResponse};
use kf_protocol::message::metadata::MetadataRequestTopic;
use kf_protocol::message::metadata::MetadataResponseTopic;
use kf_protocol::message::offset::{KfListOffsetRequest, KfListOffsetResponse};
use kf_protocol::message::offset::ListOffsetTopic;
use kf_protocol::message::offset::ListOffsetPartition;
use kf_protocol::message::offset::{KfOffsetFetchRequest, KfOffsetFetchResponse};
use kf_protocol::message::offset::OffsetFetchRequestTopic;
use kf_protocol::message::group::KfFindCoordinatorRequest;
use kf_protocol::message::group::KfFindCoordinatorResponse;
use kf_protocol::message::group::KfHeartbeatRequest;
use kf_protocol::message::group::KfHeartbeatResponse;
use kf_protocol::message::group::KfJoinGroupRequest;
use kf_protocol::message::group::KfJoinGroupResponse;
use kf_protocol::message::group::JoinGroupRequestProtocol;
use kf_protocol::message::group::KfLeaveGroupRequest;
use kf_protocol::message::group::KfLeaveGroupResponse;
use kf_protocol::api::{ProtocolMetadata, Metadata};
use kf_protocol::message::group::KfSyncGroupRequest;
use kf_protocol::message::group::KfSyncGroupResponse;
use kf_protocol::message::group::SyncGroupRequestAssignment;
use kf_protocol::message::offset::ListOffsetPartitionResponse;
use kf_protocol::message::topic::CreatableTopic;
use kf_protocol::message::topic::{KfCreateTopicsRequest};
use kf_protocol::api::{GroupAssignment, Assignment};
use kf_protocol::api::Isolation;
use kf_protocol::api::ErrorCode as KfErrorCode;
use kf_protocol::api::DefaultRecords;
use kf_protocol::message::topic::{KfDeleteTopicsRequest};
use types::defaults::KF_REQUEST_TIMEOUT_MS;
use kf_socket::KfSocketError;

use crate::ClientError;
use crate::ClientConfig;
use crate::Client;
use crate::SpuController;
use crate::LeaderConfig;
use crate::ReplicaLeader;
use crate::query_params::LeaderParam;
use crate::query_params::TopicPartitionParam;
use crate::query_params::FetchLogsParam;
use crate::query_params::ReplicaConfig;

pub struct KfClient<A>(Client<A>);

impl <A>KfClient<A> {

    fn new(client: Client<A>) -> Self {
        Self(client)
    }

    pub fn mut_client(&mut self) -> &mut Client<A> {
        &mut self.0
    }
}

impl <A>KfClient<A> where A: ToSocketAddrs + Display {

    pub async fn connect(config: ClientConfig<A>) -> Result<Self,ClientError>
    {
        let client = Client::connect(config).await?;
        Ok(Self::new(client))
    }

    // Query Kafka server for Brokers & Topic Metadata
    pub async fn query_metadata(
        &mut self,
        topics: Option<Vec<String>>,
    ) -> Result<KfMetadataResponse, KfSocketError> {
        let mut request = KfMetadataRequest::default();
        // request topics metadata
        let request_topics = if let Some(topics) = topics {
            let mut req_topics: Vec<MetadataRequestTopic> = vec![];
            for name in topics {
                req_topics.push(MetadataRequestTopic { name });
            }
            Some(req_topics)
        } else {
            None
        };
        request.topics = request_topics;

        self.0.send_receive(request).await
    }

   

    
    pub async fn list_offsets(
        &mut self,
        topic_name: &str,
        leader: &LeaderParam,
    ) -> Result<KfListOffsetResponse, KfSocketError> {

        let mut request = KfListOffsetRequest::default();
      
        // collect partition index & epoch information from leader
        let mut offset_partitions: Vec<ListOffsetPartition> = vec![];
        for partition in &leader.partitions {
            offset_partitions.push(ListOffsetPartition {
                partition_index: partition.partition_idx,
                current_leader_epoch: partition.epoch,
                timestamp: -1,
            });
        }

        // update request
        request.replica_id = -1;
        request.topics = vec![ListOffsetTopic {
            name: topic_name.to_owned(),
            partitions: offset_partitions,
        }];

        self.0.send_receive(request).await
    }

    // Query Group Coordinator for offsets.
    pub async fn offsets_fetch(
        &mut self,
        group_id: &str,
        topic_name: &str,
        tp_param: &TopicPartitionParam,

    ) -> Result<KfOffsetFetchResponse, KfSocketError> {

        let mut request = KfOffsetFetchRequest::default();
      
        // collect partition indexes
        let mut partition_indexes: Vec<i32> = vec![];
        for leader in &tp_param.leaders {
            for partition in &leader.partitions {
                partition_indexes.push(partition.partition_idx);
            }
        }

        // topics
        let topics = vec![OffsetFetchRequestTopic {
            name: topic_name.to_owned(),
            partition_indexes: partition_indexes,
        }];

        // request
        request.group_id = group_id.to_owned();
        request.topics = Some(topics);

        self.0.send_receive(request).await
    }

    // Group Coordinator
    pub async fn group_coordinator(
        &mut self,
        group_id: &str,
    ) -> Result<KfFindCoordinatorResponse, KfSocketError> {

        let mut request = KfFindCoordinatorRequest::default();
        request.key = group_id.to_owned();

        self.0.send_receive(request).await
    }

    // Send Heartbeat to group coordinator
    pub async fn send_heartbeat(
        &mut self,
        group_id: &str,
        member_id: &str,
        generation_id: i32
    ) -> Result<KfHeartbeatResponse, KfSocketError> {

        let mut request = KfHeartbeatRequest::default();

        // request with protocol
        request.group_id = group_id.to_owned();
        request.member_id = member_id.to_owned();
        request.generationid = generation_id;

        self.0.send_receive(request).await
    }

    // Ask Group Coordinator to join the group
    pub async fn join_group(
        &mut self,
        topic_name: &str,
        group_id: &str,
        member_id: &str,
    ) -> Result<KfJoinGroupResponse, KfSocketError > {

        let mut request = KfJoinGroupRequest::default();
       
        // metadata
        let mut metadata = Metadata::default();
        metadata.topics = vec![topic_name.to_owned()];

        // protocol metadata
        let mut protocol_metadata = ProtocolMetadata::default();
        protocol_metadata.content = Some(metadata);

        // join group protocol
        let join_group_protocol = JoinGroupRequestProtocol {
            name: "range".to_owned(),
            metadata: protocol_metadata,
        };

        // request with protocol
        request.group_id = group_id.to_owned();
        request.session_timeout_ms = 10000;
        request.rebalance_timeout_ms = 300000;
        request.member_id = member_id.to_owned();
        request.protocol_type = "consumer".to_owned();
        request.protocols = vec![join_group_protocol];

        self.0.send_receive(request).await
    }

    // Ask Group Coordinator to leave the group
    pub async fn leave_group(
        &mut self,
        group_id: &str,
        member_id: &str,
    ) -> Result<KfLeaveGroupResponse, KfSocketError> {

        let mut request = KfLeaveGroupRequest::default();
      
        // request with protocol
        request.group_id = group_id.to_owned();
        request.member_id = member_id.to_owned();

        self.0.send_receive(request).await
    }


    /// Fetch log records from a target server
    pub async fn kf_fetch_logs(
        &mut self,
        fetch_log_param: &FetchLogsParam,
    ) -> Result<DefaultKfFetchResponse, KfSocketError > {

        let mut fetch_partitions = vec![];
        for partition_param in &fetch_log_param.partitions {
            let mut fetch_part = FetchPartition::default();
            fetch_part.partition_index = partition_param.partition_idx;
            fetch_part.current_leader_epoch = partition_param.epoch;
            fetch_part.fetch_offset = partition_param.offset;
            fetch_part.log_start_offset = -1;
            fetch_part.max_bytes = fetch_log_param.max_bytes;

            fetch_partitions.push(fetch_part);
        }

        let mut topic_request = FetchableTopic::default();
        topic_request.name = fetch_log_param.topic.clone();
        topic_request.fetch_partitions = fetch_partitions;

        let mut request: DefaultKfFetchRequest = KfFetchRequest::default();
        request.replica_id = -1;
        request.max_wait = 500;
        request.min_bytes = 1;
        request.max_bytes = fetch_log_param.max_bytes;
        request.isolation_level = Isolation::ReadCommitted;
        request.session_id = 0;
        request.epoch = -1;
        request.topics.push(topic_request);

        self.0.send_receive(request).await
    }

    // Query Kafka server for Group Coordinator
    pub async fn sync_group(
        &mut self,
        topic_name: &str,
        group_id: &str,
        member_id: &str,
        generation_id: i32
    ) -> Result<KfSyncGroupResponse, KfSocketError> {

        let mut request = KfSyncGroupRequest::default();
        
        // assignment
        let mut assignment = Assignment::default();
        assignment.topics = vec![topic_name.to_owned()];
        assignment.reserved_i32 = 1;

        // group assignment
        let mut group_assignment = GroupAssignment::default();
        group_assignment.content = Some(assignment);

        // sync group assignment reqeust
        let sync_group_assignment = SyncGroupRequestAssignment {
            member_id: member_id.to_owned(),
            assignment: group_assignment,
        };

        // sync group request
        request.group_id = group_id.to_owned();
        request.generation_id = generation_id;
        request.member_id = member_id.to_owned();
        request.assignments = vec![sync_group_assignment];

        self.0.send_receive(request).await
    }



}



#[async_trait]
impl <A>SpuController for KfClient<A>

    where A: ToSocketAddrs + Display + Send + Sync
{

    type Leader = KfLeader;

    type TopicMetadata = MetadataResponseTopic;

    /// Find address of the Broker leader for a topic/partition
    async fn find_leader_for_topic_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Self::Leader, ClientError> {

        
        let kf_metadata = self.query_metadata(Some(vec![topic.to_owned()])).await?;
        let brokers = &kf_metadata.brokers;
        let topics = kf_metadata.topics;
        
        for response_topic in topics {
            
            if response_topic.name == topic {
                for response_partition in response_topic.partitions {
                    if response_partition.partition_index == partition {
                        let leader_id = response_partition.leader_id;

                        // traverse brokers and find leader
                        for broker in brokers {
                            if broker.node_id == leader_id {
                                debug!("broker {}/{} is leader", broker.host, broker.port);
                                let config = LeaderConfig::new(broker.into(),topic.to_owned(),partition)
                                        .spu_id(leader_id)
                                        .client_id(self.0.client_id());
                                return KfLeader::connect(config).await
                            }
                        }
                    }
                }
            }
            
        }
        
    
        Err(ClientError::IoError(IoError::new(
            ErrorKind::Other,
            format!(
                "topic '{}/{}': unknown topic or partition",
                topic, partition
            ),
        )))
        
    }

    async fn delete_topic(&mut self, topic: &str) -> Result<String,ClientError> {

        let request = KfDeleteTopicsRequest {
            topic_names: vec![topic.to_owned()],
            timeout_ms: KF_REQUEST_TIMEOUT_MS,
        };

        let response = self.0.send_receive(request).await?;

        for topic_response in response.responses {
            if topic_response.name == topic {
                if topic_response.error_code.is_ok() {
                    return Ok(topic_response.name)
                } else {
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::Other,
                        format!("topic error '{}' {}", topic, topic_response.error_code.to_sentence()),
                    )))
                }
            }
        }

        Err(
            ClientError::IoError(
                IoError::new(
                    ErrorKind::Other,
                    format!("kf topic response: {}", topic)
        )))

    }


    async fn create_topic(&mut self,topic: String, replica: ReplicaConfig,validate_only: bool) -> Result<String,ClientError> {

        let topic_request = match replica {

            ReplicaConfig::Computed(partitions, replicas, _) => CreatableTopic {
                name: topic.clone(),
                num_partitions: partitions,
                replication_factor: replicas,
                assignments: vec![],
                configs: vec![],
            },
            ReplicaConfig::Assigned(partitions) => CreatableTopic {
                name: topic.clone(),
                num_partitions: -1,
                replication_factor: -1,
                assignments: partitions.kf_encode(),
                configs: vec![],
            }
        };

        let request = KfCreateTopicsRequest {
            topics: vec![topic_request],
            timeout_ms: KF_REQUEST_TIMEOUT_MS,
            validate_only
        };

        let response = self.0.send_receive(request).await?;

        for topic_response in response.topics {
            if topic_response.name == topic {
                if topic_response.error_code.is_ok() {
                    return Ok(topic)
                } else {
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::Other,
                        format!("topic error '{}' {}", topic, topic_response.error_code.to_sentence()),
                    )))
                }
            }
        }

        Err(
            ClientError::IoError(
                IoError::new(
                    ErrorKind::Other,
                    format!("kf topic response: {}", topic)
        )))
        
    }

    async fn topic_metadata(&mut self,topics: Option<Vec<String>>) -> Result<Vec<Self::TopicMetadata>,ClientError> {

        let topics = if let Some(topics) = topics {
            let mut req_topics: Vec<MetadataRequestTopic> = vec![];
            for name in topics {
                req_topics.push(MetadataRequestTopic { name });
            }
            Some(req_topics)
        } else {
            None
        };

        let request = KfMetadataRequest {
            topics,
            ..Default::default()
        };
       
    
        let response = self.0.send_receive(request).await?;
    
        Ok(response.topics)
    }




}


pub struct KfLeader {
    client: Client<String>,
    config: LeaderConfig
}

impl KfLeader {


    pub async fn connect(config: LeaderConfig) -> Result<Self,ClientError> {
        let inner_client = Client::connect(config.as_client_config()).await?;
        Ok(Self {
            client: inner_client,
            config 
        })
    }

     /// fetch logs
     async fn fetch_logs_inner(
        &mut self,
        offset: i64,
        max_bytes: i32,
    ) ->  Result<FetchablePartitionResponse<DefaultRecords>, KfSocketError> { 

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

}


#[async_trait]
impl ReplicaLeader for KfLeader
{

    type OffsetPartitionResponse = ListOffsetPartitionResponse;

    fn config(&self) -> &LeaderConfig {
        &self.config
    }

    fn client(&mut self) -> &mut Client<String> {
        &mut self.client
    }

    async fn fetch_offsets(&mut self) -> Result<Self::OffsetPartitionResponse, ClientError > {

        let offset_partitions = vec![
            ListOffsetPartition {
                partition_index: self.partition(),
                current_leader_epoch: 0,
                timestamp: -1
            }];

        let request = KfListOffsetRequest {
            topics: vec![ListOffsetTopic {
                name: self.topic().to_owned(),
                partitions: offset_partitions,
            }],
            replica_id: -1,
            ..Default::default()
        };
    
        let response = self.client.send_receive(request).await?;

        match response.find_partition(self.topic(),self.partition()) {
            Some(partition_response) => Ok(partition_response),
            None => Err(IoError::new(
                ErrorKind::InvalidData,
                format!("no topic: {}, partition: {} founded",self.topic(),self.partition())
            ).into())
        }
    
    }

     /// Fetch log records from a target server
    fn fetch_logs<'a>(
        &'a mut self,
        _offset: i64,
        _max_bytes: i32,
        _isolation: Isolation
    ) -> BoxStream<'a,FetchablePartitionResponse<DefaultRecords>> {

        empty().boxed()

    /*
    et offsets = leader.fetch_offsets().await?;
    let last_offset = offsets.last_stable_offset();

    let mut current_offset = if opt.from_beginning {
        offsets.start_offset()
    } else {
        offsets.last_stable_offset()
    };

    debug!("entering loop");

    loop {
        debug!("fetching with offset: {}", current_offset);

        let fetch_logs_res = leader.fetch_logs(current_offset, opt.max_bytes).await?;

        current_offset = fetch_logs_res.last_stable_offset;

        debug!("fetching last offset: {} and response", last_offset);

        // process logs response

        process_fetch_topic_response(out.clone(), &topic, fetch_logs_res, &opt).await?;
        let read_lock = end.read().unwrap();
        if *read_lock {
            debug!("detected end by ctrl-c, exiting loop");
            break;
        }

        /*
        if !opt.continuous {
            if last_offset > last_offset {
                debug!("finishing fetch loop");
                break;
            }
        }
        */

        
        if !opt.continuous {
            debug!("finishing fetch loop");
            break;
        }
        

        debug!("sleeping 200 ms between next fetch");
        sleep(Duration::from_millis(200)).await;
    
        */


    }
        
        
}


/// Implement simple polling stream for kafka
/// it just iterates with simple delay
pub struct FetchStream {
    leader: KfLeader
}

impl Stream for FetchStream 
{

    type Item = FetchablePartitionResponse<DefaultRecords>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
       Poll::Ready(None)
    }

}











