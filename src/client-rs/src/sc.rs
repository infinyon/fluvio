use std::default::Default;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;

use log::debug;
use async_trait::async_trait;

use flv_future_aio::net::ToSocketAddrs;
use types::socket_helpers::ServerAddress;
use sc_api::errors::FlvErrorCode;
use sc_api::topic::{FlvTopicCompositionRequest, FlvTopicCompositionResponse};
use sc_api::topic::{FlvDeleteTopicsRequest};
use sc_api::topic::{FlvCreateTopicRequest, FlvCreateTopicsRequest};
use sc_api::topic::FlvTopicSpecMetadata;
use sc_api::topic::FlvFetchTopicResponse;
use sc_api::topic::FlvFetchTopicsRequest;
use sc_api::spu::FlvCreateCustomSpusRequest;
use sc_api::spu::{FlvCreateCustomSpuRequest, FlvEndPointMetadata};
use sc_api::spu::FlvDeleteCustomSpusRequest;
use sc_api::spu::FlvFetchSpusRequest;
use sc_api::spu::FlvRequestSpuType;
use sc_api::spu::FlvFetchSpuResponse;
use sc_api::spu::FlvCreateSpuGroupRequest;
use sc_api::spu::FlvCreateSpuGroupsRequest;
use sc_api::spu::FlvDeleteSpuGroupsRequest;
use sc_api::spu::FlvFetchSpuGroupsRequest;
use sc_api::spu::FlvFetchSpuGroupsResponse;
use sc_api::spu::FlvCustomSpu;
use kf_socket::KfSocketError;

use crate::ClientError;
use crate::Client;
use crate::ClientConfig;
use crate::LeaderConfig;
use crate::SpuController;
use crate::SpuLeader;
use crate::query_params::ReplicaConfig;

pub struct ScClient<A>(Client<A>);

impl<A> ScClient<A> {
    fn new(client: Client<A>) -> Self {
        Self(client)
    }
}

impl<A> ScClient<A> {
    pub fn inner(&self) -> &Client<A> {
        &self.0
    }
}

impl<A> ScClient<A>
where
    A: ToSocketAddrs + Display,
{
    /// Connect to server, get version, and for topic composition: Replicas and SPUs
    pub async fn get_topic_composition(
        &mut self,
        topic: &str,
    ) -> Result<FlvTopicCompositionResponse, KfSocketError> {
        let mut request = FlvTopicCompositionRequest::default();
        request.topic_names = vec![topic.to_owned()];

        self.0.send_receive(request).await
    }

    pub async fn connect(config: ClientConfig<A>) -> Result<Self, ClientError> {
        let client = Client::connect(config).await?;
        Ok(Self::new(client))
    }

    pub async fn create_custom_spu(
        &mut self,
        id: i32,
        name: String,
        public_server: ServerAddress,
        private_server: ServerAddress,
        rack: Option<String>,
    ) -> Result<(), ClientError> {
        let spu = FlvCreateCustomSpuRequest {
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
        let request = FlvCreateCustomSpusRequest {
            custom_spus: vec![spu],
        };

        let responses = self.0.send_receive(request).await?;

        responses.validate(&name).map_err(|err| err.into())
    }

    pub async fn delete_custom_spu(&mut self, spu: FlvCustomSpu) -> Result<(), ClientError> {
        let request = FlvDeleteCustomSpusRequest {
            custom_spus: vec![spu],
        };

        let responses = self.0.send_receive(request).await?;

        responses.validate().map_err(|err| err.into())
    }

    pub async fn list_spu(
        &mut self,
        only_custom_spu: bool,
    ) -> Result<Vec<FlvFetchSpuResponse>, ClientError> {
        let request = FlvFetchSpusRequest {
            req_spu_type: match only_custom_spu {
                true => FlvRequestSpuType::Custom,
                false => FlvRequestSpuType::All,
            },
            ..Default::default()
        };

        let responses = self.0.send_receive(request).await?;

        Ok(responses.spus)
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

#[async_trait]
impl<A> SpuController for ScClient<A>
where
    A: ToSocketAddrs + Display + Send + Sync,
{
    type Leader = SpuLeader;

    type TopicMetadata = FlvFetchTopicResponse;
    /// Find address of the SPU leader for a topic/partition
    async fn find_leader_for_topic_partition(
        &mut self,
        topic: &str,
        partition: i32,
    ) -> Result<Self::Leader, ClientError> {
        let topic_comp_resp = self.get_topic_composition(topic).await?;
        let mut topics_resp = topic_comp_resp.topics;
        let spus_resp = topic_comp_resp.spus;

        // there must be one topic in reply
        if topics_resp.len() != 1 {
            return Err(ClientError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("topic error: expected 1 topic, found {}", topics_resp.len()),
            )));
        }

        // check for errors
        let topic_resp = topics_resp.remove(0);
        if topic_resp.error_code != FlvErrorCode::None {
            return Err(ClientError::IoError(IoError::new(
                ErrorKind::InvalidData,
                format!("topic error: {}", topic_resp.error_code.to_sentence()),
            )));
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
                                    "topic-composition spu error: {}",
                                    topic_resp.error_code.to_sentence()
                                ),
                            )));
                        }

                        debug!("spu {}/{}: is leader", spu_resp.host, spu_resp.port);

                        let leader_config =
                            LeaderConfig::new(spu_resp.into(), topic.to_owned(), partition)
                                .spu_id(leader_id)
                                .client_id(self.0.client_id());
                        return SpuLeader::connect(leader_config).await;
                    }
                }
            }
        }

        Err(ClientError::IoError(IoError::new(
            ErrorKind::Other,
            format!(
                "topic-composition '{}/{}': unknown topic or partition",
                topic, partition
            ),
        )))
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
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::Other,
                        format!(
                            "topic error '{}' {}",
                            topic,
                            topic_response.error_code.to_sentence()
                        ),
                    )));
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
                    return Err(ClientError::IoError(IoError::new(
                        ErrorKind::Other,
                        format!(
                            "topic error '{}' {}",
                            topic,
                            topic_response.error_code.to_sentence()
                        ),
                    )));
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
        Ok(response.topics)
    }
}
