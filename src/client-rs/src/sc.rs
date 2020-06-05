use std::default::Default;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::iter::Iterator;

use log::debug;
use log::trace;
use async_trait::async_trait;

use flv_util::socket_helpers::ServerAddress;
use sc_api::errors::FlvErrorCode;
use sc_api::topic::{FlvTopicCompositionRequest, FlvTopicCompositionResponse};
use sc_api::topic::{FlvDeleteTopicsRequest};
use sc_api::topic::{FlvCreateTopicRequest, FlvCreateTopicsRequest};
use sc_api::topic::FlvTopicSpecMetadata;
use sc_api::topic::FlvFetchTopicsRequest;
use sc_api::spu::FlvRegisterCustomSpusRequest;
use sc_api::spu::{FlvRegisterCustomSpuRequest, FlvEndPointMetadata};
use sc_api::spu::FlvUnregisterCustomSpusRequest;
use sc_api::spu::FlvFetchSpusRequest;
use sc_api::spu::FlvRequestSpuType;
use sc_api::spu::FlvCreateSpuGroupRequest;
use sc_api::spu::FlvCreateSpuGroupsRequest;
use sc_api::spu::FlvDeleteSpuGroupsRequest;
use sc_api::spu::FlvFetchSpuGroupsRequest;
use sc_api::spu::FlvFetchSpuGroupsResponse;
use sc_api::spu::FlvCustomSpu;
use kf_socket::KfSocketError;

use crate::ClientError;
use crate::Client;
use crate::ReplicaLeaderConfig;
use crate::SpuController;
use crate::SpuReplicaLeader;
use crate::query_params::ReplicaConfig;
use crate::metadata::topic::TopicMetadata;
use crate::metadata::spu::SpuMetadata;

pub struct ScClient(Client);

impl ScClient {
    pub fn new(client: Client) -> Self {
        Self(client)
    }

    pub fn inner(&self) -> &Client {
        &self.0
    }

    /// Connect to server, get version, and for topic composition: Replicas and SPUs
    pub async fn get_topic_composition(
        &mut self,
        topic: &str,
    ) -> Result<FlvTopicCompositionResponse, KfSocketError> {
        debug!("request topic metadata for topic: {}", topic);
        let mut request = FlvTopicCompositionRequest::default();
        request.topic_names = vec![topic.to_owned()];

        self.0.send_receive(request).await
    }

    pub async fn register_custom_spu(
        &mut self,
        id: i32,
        name: String,
        public_server: ServerAddress,
        private_server: ServerAddress,
        rack: Option<String>,
    ) -> Result<(), ClientError> {
        let spu = FlvRegisterCustomSpuRequest {
            id,
            name: name.to_owned(),
            public_server: FlvEndPointMetadata {
                host: public_server.host,
                port: public_server.port,
            },
            private_server: FlvEndPointMetadata {
                host: private_server.host.clone(),
                port: private_server.port,
            },
            rack,
        };
        // generate request with 1 custom spu
        let request = FlvRegisterCustomSpusRequest {
            custom_spus: vec![spu],
        };

        let responses = self.0.send_receive(request).await?;

        responses.validate(&name).map_err(|err| err.into())
    }

    pub async fn unregister_custom_spu(&mut self, spu: FlvCustomSpu) -> Result<(), ClientError> {
        let request = FlvUnregisterCustomSpusRequest {
            custom_spus: vec![spu],
        };

        let responses = self.0.send_receive(request).await?;

        responses.validate().map_err(|err| err.into())
    }

    pub async fn list_spu(
        &mut self,
        only_custom_spu: bool,
    ) -> Result<Vec<SpuMetadata>, ClientError> {
        let request = FlvFetchSpusRequest {
            req_spu_type: match only_custom_spu {
                true => FlvRequestSpuType::Custom,
                false => FlvRequestSpuType::All,
            },
            ..Default::default()
        };

        let responses = self.0.send_receive(request).await?;

        Ok(responses.spus.into_iter().map(|spu| spu.into()).collect())
    }

    pub async fn create_group(
        &mut self,
        group: FlvCreateSpuGroupRequest,
    ) -> Result<(), ClientError> {
        let request: FlvCreateSpuGroupsRequest = group.into();

        let responses = self.0.send_receive(request).await?;

        responses.validate().map_err(|err| err.into())
    }

    pub async fn delete_group(&mut self, group: &str) -> Result<(), ClientError> {
        let request = FlvDeleteSpuGroupsRequest {
            spu_groups: vec![group.to_owned()],
        };

        let responses = self.0.send_receive(request).await?;
        responses.validate().map_err(|err| err.into())
    }

    pub async fn list_group(&mut self) -> Result<FlvFetchSpuGroupsResponse, ClientError> {
        let request = FlvFetchSpuGroupsRequest::default();
        self.0.send_receive(request).await.map_err(|err| err.into())
    }
}

macro_rules! topic_error {
    ($topic:expr,$topic_response:expr,$msg:expr) => {{
        use crate::ClientError;
        use std::io::Error as IoError;

        if let Some(msg) = &($topic_response.error_message) {
            ClientError::IoError(IoError::new(ErrorKind::Other, msg.to_owned()))
        } else {
            ClientError::IoError(IoError::new(
                ErrorKind::Other,
                format!($msg, $topic, $topic_response.error_code.to_sentence()),
            ))
        }
    }};
}

#[async_trait]
impl SpuController for ScClient {
    type Leader = SpuReplicaLeader;

    type TopicMetadata = TopicMetadata;

    /// Connect to replica leader for a topic/partition
    async fn find_replica_for_topic_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Self::Leader, ClientError> {
        debug!(
            "trying to find replica for topic: {}, partition: {}",
            topic, partition
        );

        let topic_comp_resp = self.get_topic_composition(topic).await?;

        trace!("topic composition: {:#?}", topic_comp_resp);

        let mut topics_resp = topic_comp_resp.topics;
        let spus_resp = topic_comp_resp.spus;

        // there must be one topic in reply
        if topics_resp.len() != 1 {
            return Err(ClientError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("topic error: expected 1 topic, found {}", topics_resp.len()),
            )));
        }

        let topic_resp = topics_resp.remove(0);

        if topic_resp.error_code != FlvErrorCode::None {
            if topic_resp.error_code == FlvErrorCode::TopicNotFound {
                return Err(ClientError::TopicNotFound(topic.to_owned()));
            } else {
                return Err(ClientError::IoError(IoError::new(
                    ErrorKind::InvalidData,
                    format!(
                        "error during topic lookup: {}",
                        topic_resp.error_code.to_sentence()
                    ),
                )));
            }
        }
        // lookup leader
        for partition_resp in topic_resp.partitions {
            if partition_resp.partition_idx == partition {
                // check for errors
                if partition_resp.error_code != FlvErrorCode::None {
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::InvalidData,
                        format!(
                            "topic-composition partition error: {}",
                            topic_resp.error_code.to_sentence()
                        ),
                    )));
                }

                // traverse spus and find leader
                let leader_id = partition_resp.leader_id;
                for spu_resp in &spus_resp {
                    if spu_resp.spu_id == leader_id {
                        // check for errors
                        if spu_resp.error_code != FlvErrorCode::None {
                            return Err(ClientError::IoError(IoError::new(
                                ErrorKind::InvalidData,
                                format!(
                                    "problem with partition look up {}:{} error: {}",
                                    topic,
                                    partition,
                                    topic_resp.error_code.to_sentence()
                                ),
                            )));
                        }

                        debug!("spu {}/{}: is leader", spu_resp.host, spu_resp.port);

                        let mut leader_client_config = self.0.clone_config();
                        let addr: ServerAddress = spu_resp.into();
                        leader_client_config.set_addr(addr.to_string());

                        let client = leader_client_config.connect().await?;
                        let leader_config = ReplicaLeaderConfig::new(topic.to_owned(), partition);
                        return Ok(SpuReplicaLeader::new(leader_config, client));
                    }
                }
            }
        }

        Err(ClientError::PartitionNotFound(topic.to_owned(), partition))
    }

    async fn delete_topic(&mut self, topic: &str) -> Result<String, ClientError> {
        let request = FlvDeleteTopicsRequest {
            topics: vec![topic.to_owned()],
        };

        let response = self.0.send_receive(request).await?;

        for topic_response in response.results {
            if topic_response.name == topic {
                if topic_response.error_code.is_ok() {
                    return Ok(topic_response.name);
                } else {
                    return Err(topic_error!(
                        topic,
                        topic_response,
                        "topic deletion error '{}' {}"
                    ));
                }
            }
        }

        Err(ClientError::IoError(IoError::new(
            ErrorKind::Other,
            format!("kf topic response: {}", topic),
        )))
    }

    async fn create_topic(
        &mut self,
        topic: String,
        replica: ReplicaConfig,
        validate_only: bool,
    ) -> Result<String, ClientError> {
        let topic_metadata = match replica {
            // Computed Replicas
            ReplicaConfig::Computed(partitions, replicas, ignore_rack) => {
                FlvTopicSpecMetadata::Computed((partitions, replicas as i32, ignore_rack).into())
            }
            // Assigned (user defined)  Replicas
            ReplicaConfig::Assigned(partitions) => {
                FlvTopicSpecMetadata::Assigned(partitions.sc_encode().into())
            }
        };
        let request = FlvCreateTopicsRequest {
            topics: vec![FlvCreateTopicRequest {
                name: topic.clone(),
                topic: topic_metadata,
            }],
            validate_only: validate_only,
        };

        let response = self.0.send_receive(request).await?;

        for topic_response in response.results {
            if topic_response.name == topic {
                if topic_response.error_code.is_ok() {
                    return Ok(topic);
                } else {
                    return Err(topic_error!(
                        topic,
                        topic_response,
                        "topic creation error '{}' {}"
                    ));
                }
            }
        }

        Err(ClientError::IoError(IoError::new(
            ErrorKind::Other,
            format!("kf topic response: {}", topic),
        )))
    }

    async fn topic_metadata(
        &mut self,
        topics: Option<Vec<String>>,
    ) -> Result<Vec<Self::TopicMetadata>, ClientError> {
        let request = FlvFetchTopicsRequest { names: topics };

        let response = self.0.send_receive(request).await?;
        let topics: Vec<Self::TopicMetadata> = response
            .topics
            .into_iter()
            .map(|t| TopicMetadata::new(t))
            .collect();
        Ok(topics)
    }
}
