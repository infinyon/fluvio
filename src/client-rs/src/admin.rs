use std::convert::TryInto;
use std::fmt::Display;

use flv_api_sc::objects::*;
use flv_api_sc::AdminRequest;
use kf_socket::*;

use crate::client::*;
use crate::ClientError;


/// adminstration interface
pub struct AdminClient(SerialClient);

impl AdminClient {
    pub(crate) fn new(client: SerialClient) -> Self {
        Self(client)
    }

    async fn send_receive<R>(&mut self, request: R) -> Result<R::Response, KfSocketError>
    where
        R: AdminRequest + Send + Sync,
    {
        self.0.send_receive(request).await
    }

    /// create new object
    pub async fn create<S>(
        &mut self,
        name: String,
        dry_run: bool,
        spec: S,
    ) -> Result<(), ClientError>
    where
        S: Into<AllCreatableSpec>,
    {
        let create_request = CreateRequest {
            name,
            dry_run,
            spec: spec.into(),
        };

        self.send_receive(create_request).await?.as_result()?;

        Ok(())
    }

    /// delete object by key
    /// key is depend on spec, most are string but some allow multiple types
    pub async fn delete<S,K>(&mut self, key: K) -> Result<(),ClientError>
        where
            S: DeleteSpec,
            K: Into<S::DeleteKey>
    {
        let delete_request = S::into_request(key);
        self.send_receive(delete_request).await?.as_result()?;
        Ok(())
    }

    pub async fn list<S,F>(&mut self,filters: F)  -> Result<Vec<Metadata<S>>,ClientError>
        where
            S: ListSpec,
            F: Into<Vec<S::Filter>>,
            ListResponse: TryInto<Vec<Metadata<S>>>,
            <ListResponse as TryInto<Vec<Metadata<S>>>>::Error: Display
    {
        use std::io::Error;
        use std::io::ErrorKind;

        let list_request = S::into_list_request(filters.into());

        let response = self.send_receive(list_request).await?;

        response.try_into()
            .map_err(|err| Error::new(ErrorKind::Other,format!("can't convert: {}",err)).into())
    }

    

    /*
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

                        let mut leader_client_config = self.0.config().clone();
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
    */
}
