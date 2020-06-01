use core::pin::Pin;
use core::task::Poll;
use core::task::Context;
use std::io::Error as IoError;
use std::io::ErrorKind;


use log::error;
use log::debug;
use log::trace;
use futures::stream::Stream;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::future::FutureExt;
use futures::stream::empty;
use async_trait::async_trait;


use kf_socket::KfSocketError;
use kf_protocol::api::DefaultRecords;
use kf_protocol::api::PartitionOffset;
use kf_protocol::message::fetch::FetchablePartitionResponse;
use spu_api::fetch::DefaultFlvContinuousFetchRequest;
use spu_api::offsets::{FlvFetchOffsetsRequest};
use spu_api::offsets::FetchOffsetPartitionResponse;
use spu_api::spus::{FlvFetchLocalSpuRequest, FlvFetchLocalSpuResponse};


use crate::ClientError;
use crate::Client;
use crate::ReplicaLeaderConfig;
use crate::ReplicaLeader;
use crate::FetchLogOption;
use crate::FetchOffset;

/// Full access to SPU
pub struct Spu(Client);

impl Spu  {

    #[allow(unused)]
    fn new(client: Client) -> Self {
        Self(client)
    }

    pub fn mut_client(&mut self) -> &mut Client {
        &mut self.0
    }
}


// Access specific replica leader of the SPU
// for now, we use string to store address, later, we might store address natively
pub struct SpuReplicaLeader
{
    client: Client,
    config: ReplicaLeaderConfig,
}


impl SpuReplicaLeader  {

    pub fn new(config: ReplicaLeaderConfig, client: Client) -> Self {
        Self {
            client,
            config
        }
    }

    pub fn config(&self) -> &ReplicaLeaderConfig {
        &self.config
    }

    pub fn addr(&self) -> &str {
        &self.client.config().addr()
    }


    /// fetch local spu
    pub async fn fetch_local_spu(&mut self) -> Result<FlvFetchLocalSpuResponse, KfSocketError> {
        let request = FlvFetchLocalSpuRequest::default();
        self.client.send_receive(request).await
    }

    /// depends on offset option, calculate offset
    async fn calc_offset(&mut self,offset: FetchOffset) -> Result<i64,ClientError> {

        Ok(match offset {
            FetchOffset::Offset(inner_offset) => inner_offset,
            FetchOffset::Earliest(relative_offset) =>  {
                let offsets = self.fetch_offsets().await?;
                offsets.start_offset() + relative_offset.unwrap_or(0)
            },
            FetchOffset::Latest(relative_offset) => {
                let offsets = self.fetch_offsets().await?;
                offsets.last_stable_offset() - relative_offset.unwrap_or(0)
            }
        })
    }
}

#[async_trait]
impl ReplicaLeader for SpuReplicaLeader {

    type OffsetPartitionResponse = FetchOffsetPartitionResponse;

    fn config(&self) -> &ReplicaLeaderConfig {
        &self.config
    }

    fn mut_client(&mut self) -> &mut Client {
        &mut self.client
    }

    fn client(&self) -> &Client {
        &self.client
    }

    async fn fetch_offsets(&mut self) -> Result<FetchOffsetPartitionResponse, ClientError> {

        debug!("fetching offset for: {}:{}",self.topic(),self.partition());
        let response = self.client
            .send_receive(FlvFetchOffsetsRequest::new(
            self.topic().to_owned(),
            self.partition(),
            ))
            .await?;

        trace!("receive topic {}:{}  offset: {:#?}",self.topic(),self.partition(),response);

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
        option: FetchLogOption
    ) -> Result<FetchablePartitionResponse<DefaultRecords>,ClientError>  {

  
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
            ..Default::default()
        };

        let response = self.client.send_receive(fetch_request).await?;

        if let Some(partition_response) = response.find_partition(&self.topic(),self.partition()) {
            Ok(partition_response)
        } else {
            Err(ClientError::PartitionNotFound(self.topic().to_owned(),self.partition()))
        }
    }

    /// fetch logs as stream
    fn fetch_logs<'a>(
        &'a mut  self,
        offset_option: FetchOffset,
        option: FetchLogOption
    ) -> BoxStream<'a,FetchablePartitionResponse<DefaultRecords>>  {

        debug!(
            "starting continuous fetch logs: {:#?} '{}' ({}) partition to {}",
            offset_option,
            self.topic(),
            self.partition(),
            self.addr(),
        );

        let log_stream_ft = async move {

            let offset = match self.calc_offset(offset_option).await {
                Ok(offset) => offset,
                Err(err) => {
                    error!("error getting offset: {}",err);
                    return empty().right_stream()
                }
            };

            let request = DefaultFlvContinuousFetchRequest {
                topic: self.topic().to_owned(),
                partition: self.partition(),
                fetch_offset: offset,
                max_bytes: option.max_bytes,
                isolation: option.isolation,
                ..Default::default()
            };

            match self.client.send_request(request).await {
                Ok(req_msg) => 
                    self.client.mut_socket().get_mut_stream().response_stream::<DefaultFlvContinuousFetchRequest>(req_msg)
                        .map(|response| response.partition)
                        .left_stream(),
                Err(err) => {
                    error!("error retrieving continuous fetch log: {}",err);
                    empty().right_stream()
                }
            }
            
        };

        log_stream_ft.flatten_stream().boxed()
        
        
    }


}



pub struct FetchStream(SpuReplicaLeader);



impl Stream for FetchStream
{

    type Item = FetchablePartitionResponse<DefaultRecords>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
       Poll::Ready(None)
    }

}




