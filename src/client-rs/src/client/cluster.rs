use tracing::debug;

use kf_socket::AllMultiplexerSocket;
use kf_protocol::api::ReplicaKey;

use crate::admin::AdminClient;
use crate::{Producer, ClusterConfig};
use crate::Consumer;
use crate::ClientError;
use crate::sync::MetadataStores;
use crate::spu::SpuPool;

use super::*;
use flv_future_aio::net::tls::AllDomainConnector;
use std::convert::TryFrom;

/// Client connection to cluster
pub struct ClusterClient {
    socket: AllMultiplexerSocket,
    config: ClientConfig,
    versions: Versions,
    spu_pool: Option<SpuPool>,
}

impl ClusterClient {
    pub(crate) fn new(client: RawClient) -> Self {
        let (socket, config, versions) = client.split();
        Self {
            socket: AllMultiplexerSocket::new(socket),
            config,
            versions,
            spu_pool: None,
        }
    }

    pub async fn connect(config: ClusterConfig) -> Result<ClusterClient, ClientError> {
        let connector = match config.tls {
            None => AllDomainConnector::default_tcp(),
            Some(tls) => TryFrom::try_from(tls)?,
        };
        let config = ClientConfig::new(config.addr, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());
        let cluster = ClusterClient::new(inner_client);
        //cluster.start_metadata_watch().await?;
        Ok(cluster)
    }

    async fn create_serial_client(&mut self) -> SerialClient {
        SerialClient::new(
            self.socket.create_serial_socket().await,
            self.config.clone(),
            self.versions.clone(),
        )
    }

    /// create new admin client
    pub async fn admin(&mut self) -> AdminClient {
        AdminClient::new(self.create_serial_client().await)
    }

    /// create new producer for topic/partition
    pub async fn producer(&mut self, replica: ReplicaKey) -> Result<Producer, ClientError> {
        debug!("creating producer, replica: {}", replica);
        if let Some(pool) = &self.spu_pool {
            Ok(Producer::new(replica, pool.clone()))
        } else {
            let pool = self.init_spu_pool().await?;
            Ok(Producer::new(replica, pool))
        }
    }

    /// initialize spu pool and return clone of the pool
    async fn init_spu_pool(&mut self) -> Result<SpuPool, ClientError> {
        debug!("init metadata store");
        let metadata = MetadataStores::new(&mut self.socket).await?;
        let pool = SpuPool::new(self.config.clone(), metadata);
        self.spu_pool.replace(pool.clone());
        Ok(pool)
    }

    /// create new consumer for topic/partition
    pub async fn consumer(&mut self, replica: ReplicaKey) -> Result<Consumer, ClientError> {
        debug!("creating consumer, replica: {}", replica);

        if let Some(pool) = &self.spu_pool {
            Ok(Consumer::new(replica, pool.clone()))
        } else {
            let pool = self.init_spu_pool().await?;
            Ok(Consumer::new(replica, pool))
        }
    }
}
