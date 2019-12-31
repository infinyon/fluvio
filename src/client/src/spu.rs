use std::io::Error as IoError;
use std::io::ErrorKind;
use std::fmt::Display;

use async_trait::async_trait;

use flv_future_aio::net::ToSocketAddrs;
use types::socket_helpers::ServerAddress;

use kf_socket::KfSocketError;
use spu_api::offsets::{FlvFetchOffsetsRequest};
use spu_api::offsets::FetchOffsetPartitionResponse;

use spu_api::spus::{FlvFetchLocalSpuRequest, FlvFetchLocalSpuResponse};

use crate::ClientError;
use crate::Client;
use crate::LeaderConfig;
use crate::ReplicaLeader;
use crate::ClientConfig;

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
pub struct SpuLeader {
    client: Client<String>,
    config: LeaderConfig,
}

impl SpuLeader {
    pub fn addr(&self) -> &ServerAddress {
        &self.config.addr
    }
    pub fn client_id(&self) -> &str {
        &self.config.client_id
    }

    pub fn topic(&self) -> &str {
        &self.config.topic
    }

    pub fn partition(&self) -> i32 {
        self.config.partition
    }

    pub async fn connect(config: LeaderConfig) -> Result<Self, ClientError> {
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
}

#[async_trait]
impl ReplicaLeader for SpuLeader {
    type OffsetPartitionResponse = FetchOffsetPartitionResponse;

    fn config(&self) -> &LeaderConfig {
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
}
