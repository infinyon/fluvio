use std::sync::Arc;
use std::collections::HashMap;
use anyhow::Result;

use fluvio_sc_schema::partition::PartitionSpec;
use fluvio_sc_schema::topic::TopicSpec;
use tracing::{debug, trace, instrument};
use async_lock::Mutex;
use async_trait::async_trait;

use fluvio_protocol::record::ReplicaKey;
use fluvio_protocol::api::Request;
use fluvio_types::SpuId;
use fluvio_socket::{
    AsyncResponse, ClientConfig, MultiplexerSocket, SocketError, StreamSocket,
    VersionedSerialSocket,
};
use crate::FluvioError;
use crate::sync::{MetadataStores, StoreContext};

/// used for connecting to spu
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SpuDirectory {
    /// Create request/response socket to SPU for a replica
    ///
    /// All sockets to same SPU use a single TCP connection.
    /// First this looks up SPU address in SPU metadata and try to see if there is an existing TCP connection.
    /// If not, it will create a new connection and creates socket to it
    async fn create_serial_socket(
        &self,
        replica: &ReplicaKey,
    ) -> Result<VersionedSerialSocket, FluvioError>;

    /// create stream to leader replica
    async fn create_stream_with_version<R: Request>(
        &self,
        replica: &ReplicaKey,
        request: R,
        version: i16,
    ) -> Result<AsyncResponse<R>, FluvioError>
    where
        R: Sync + Send;
}

/// connection pool to spu
pub struct SpuSocketPool {
    config: Arc<ClientConfig>,
    pub(crate) metadata: MetadataStores,
    spu_clients: Arc<Mutex<HashMap<SpuId, StreamSocket>>>,
}

impl Drop for SpuSocketPool {
    fn drop(&mut self) {
        trace!("dropping spu pool");
        self.shutdown();
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub trait SpuPool {
    /// start synchronize based on pool
    fn start(config: Arc<ClientConfig>, metadata: MetadataStores) -> Result<Self, SocketError>
    where
        Self: std::marker::Sized;

    /// create new spu socket
    async fn connect_to_leader(&self, leader: SpuId) -> Result<StreamSocket, FluvioError>;

    async fn create_serial_socket_from_leader(
        &self,
        leader_id: SpuId,
    ) -> Result<VersionedSerialSocket, FluvioError>;

    async fn topic_exists(&self, topic: String) -> Result<bool, FluvioError>;

    fn shutdown(&mut self);

    fn topics(&self) -> &StoreContext<TopicSpec>;

    fn partitions(&self) -> &StoreContext<PartitionSpec>;
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SpuPool for SpuSocketPool {
    /// start synchronize based on pool
    fn start(config: Arc<ClientConfig>, metadata: MetadataStores) -> Result<Self, SocketError> {
        debug!("starting spu pool");
        Ok(Self {
            metadata,
            config,
            spu_clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// create new spu socket
    #[instrument(skip(self))]
    async fn connect_to_leader(&self, leader: SpuId) -> Result<StreamSocket, FluvioError> {
        let spu = self.metadata.spus().look_up_by_id(leader).await?;

        let mut client_config = self.config.with_prefix_sni_domain(spu.key());

        let spu_addr = match spu.spec.public_endpoint_local {
            Some(local) if self.config.use_spu_local_address() => {
                let host = local.host;
                let port = local.port;
                format!("{host}:{port}")
            }
            _ => spu.spec.public_endpoint.addr(),
        };

        debug!(leader = spu.spec.id,addr = %spu_addr,"try connecting to spu");
        client_config.set_addr(spu_addr);
        let versioned_socket = client_config.connect().await?;
        let (socket, config, versions) = versioned_socket.split();
        Ok(StreamSocket::new(
            config,
            MultiplexerSocket::shared(socket),
            versions,
        ))
    }

    #[instrument(skip(self))]
    async fn create_serial_socket_from_leader(
        &self,
        leader_id: SpuId,
    ) -> Result<VersionedSerialSocket, FluvioError> {
        // check if already have existing connection to same SPU
        let mut client_lock = self.spu_clients.lock().await;

        if let Some(spu_socket) = client_lock.get_mut(&leader_id) {
            if !spu_socket.is_stale() {
                return Ok(spu_socket.create_serial_socket().await);
            } else {
                client_lock.remove(&leader_id);
            }
        }

        let mut spu_socket = self.connect_to_leader(leader_id).await?;
        let serial_socket = spu_socket.create_serial_socket().await;
        client_lock.insert(leader_id, spu_socket);

        Ok(serial_socket)
    }

    async fn topic_exists(&self, topic: String) -> Result<bool, FluvioError> {
        let replica = ReplicaKey::new(topic, 0u32);
        Ok(self.partitions().lookup_by_key(&replica).await?.is_some())
    }

    fn shutdown(&mut self) {
        self.metadata.shutdown();
    }

    fn topics(&self) -> &StoreContext<TopicSpec> {
        self.metadata.topics()
    }

    fn partitions(&self) -> &StoreContext<PartitionSpec> {
        self.metadata.partitions()
    }
}

#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
impl SpuDirectory for SpuSocketPool {
    /// Create request/response socket to SPU for a replica
    ///
    /// All sockets to same SPU use a single TCP connection.
    /// First this looks up SPU address in SPU metadata and try to see if there is an existing TCP connection.
    /// If not, it will create a new connection and creates socket to it
    #[instrument(skip(self, replica))]
    async fn create_serial_socket(
        &self,
        replica: &ReplicaKey,
    ) -> Result<VersionedSerialSocket, FluvioError> {
        let partition_search = self.metadata.partitions().lookup_by_key(replica).await?;
        let partition = if let Some(partition) = partition_search {
            partition
        } else {
            return Err(FluvioError::PartitionNotFound(
                replica.topic.to_string(),
                replica.partition,
            ));
        };

        let leader_id = partition.spec.leader;
        let socket = self.create_serial_socket_from_leader(leader_id).await?;
        Ok(socket)
    }

    #[instrument(skip(self, replica, request, version))]
    async fn create_stream_with_version<R: Request>(
        &self,
        replica: &ReplicaKey,
        request: R,
        version: i16,
    ) -> Result<AsyncResponse<R>, FluvioError>
    where
        R: Sync + Send,
    {
        let partition_search = self.metadata.partitions().lookup_by_key(replica).await?;

        let partition = if let Some(partition) = partition_search {
            partition
        } else {
            return Err(FluvioError::PartitionNotFound(
                replica.topic.to_owned(),
                replica.partition,
            ));
        };

        let leader_id = partition.spec.leader;

        // check if already have existing leader or create new connection to leader
        let mut client_lock = self.spu_clients.lock().await;

        if let Some(spu_socket) = client_lock.get_mut(&leader_id) {
            return spu_socket
                .create_stream_with_version(request, version)
                .await
                .map_err(|err| err.into());
        }

        let mut spu_socket = self.connect_to_leader(leader_id).await?;
        let stream = spu_socket
            .create_stream_with_version(request, version)
            .await?;
        client_lock.insert(leader_id, spu_socket);

        Ok(stream)
    }
}
