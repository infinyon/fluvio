use core::pin::Pin;
use core::task::Poll;
use core::task::Context;
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;

use log::error;
use log::debug;
use futures::stream::Stream;
use futures::stream::BoxStream;
use futures::stream::StreamExt;
use futures::future::FutureExt;
use futures::stream::empty;
use async_trait::async_trait;

use flv_future_aio::net::ToSocketAddrs;
use types::socket_helpers::ServerAddress;

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
use crate::ClientConfig;
use crate::FetchLogOption;
use crate::FetchOffset;

/// Full access to SPU
pub struct Spu<A>(Client<A>);

impl<A> Spu<A> {
    fn new(client: Client<A>) -> Self {
        Self(client)
    }

    pub fn mut_client(&mut self) -> &mut Client<A> {
        &mut self.0
    }
}

impl<A> Spu<A>
where
    A: ToSocketAddrs + Display,
{
    pub async fn connect(config: ClientConfig<A>) -> Result<Self, ClientError> {
        let client = Client::connect(config).await?;
        Ok(Self::new(client))
    }
}

// Access specific replica leader of the SPU
// for now, we use string to store address, later, we might store address natively
pub struct SpuReplicaLeader {
    client: Client<String>,
    config: ReplicaLeaderConfig,
}

impl SpuReplicaLeader {

    pub fn config(&self) -> &ReplicaLeaderConfig {
        &self.config
    }

    pub fn addr(&self) -> &ServerAddress {
        &self.config.addr()
    }


    pub async fn connect(config: ReplicaLeaderConfig) -> Result<Self, ClientError> {
        let inner_client = Client::connect(config.as_client_config()).await?;
        Ok(Self {
            client: inner_client,
            config,
        })
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
            FetchOffset::Earliest =>  {
                let offsets = self.fetch_offsets().await?;
                offsets.start_offset()
            },
            FetchOffset::Latest => {
                let offsets = self.fetch_offsets().await?;
                offsets.last_stable_offset()
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

    fn client(&mut self) -> &mut Client<String> {
        &mut self.client
    }

    async fn fetch_offsets(&mut self) -> Result<FetchOffsetPartitionResponse, ClientError> {
        let response = self
            .client
            .send_receive(FlvFetchOffsetsRequest::new(
                self.topic().to_owned(),
                self.partition(),
            ))
            .await?;

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

    /// stream of partition response
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

            match self.client().send_request(request).await {
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

impl FetchStream {

}

impl Stream for FetchStream
{

    type Item = FetchablePartitionResponse<DefaultRecords>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
       Poll::Ready(None)
    }

}




