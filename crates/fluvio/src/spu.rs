use std::sync::Arc;
use std::collections::HashMap;

use tracing::{debug, trace, instrument};
use async_lock::Mutex;

use dataplane::ReplicaKey;
use dataplane::api::Request;
use dataplane::api::RequestMessage;
use fluvio_types::SpuId;
use fluvio_socket::{MultiplexerSocket, SharedMultiplexerSocket, SocketError, AsyncResponse};
use crate::FluvioError;
use crate::sockets::ClientConfig;
use crate::sync::MetadataStores;
use crate::sockets::VersionedSerialSocket;
use crate::sockets::Versions;

const DEFAULT_STREAM_QUEUE_SIZE: usize = 10;

struct SpuSocket {
    config: Arc<ClientConfig>,
    socket: SharedMultiplexerSocket,
    versions: Versions,
}

impl SpuSocket {
    async fn create_serial_socket(&mut self) -> VersionedSerialSocket {
        VersionedSerialSocket::new(
            self.socket.clone(),
            self.config.clone(),
            self.versions.clone(),
        )
    }

    async fn create_stream_with_version<R: Request>(
        &mut self,
        request: R,
        version: i16,
    ) -> Result<AsyncResponse<R>, FluvioError> {
        let mut req_msg = RequestMessage::new_request(request);
        req_msg.header.set_api_version(version);
        self.socket
            .create_stream(req_msg, DEFAULT_STREAM_QUEUE_SIZE)
            .await
            .map_err(|err| err.into())
    }
}

/// connection pool to spu
pub struct SpuPool {
    config: Arc<ClientConfig>,
    pub(crate) metadata: MetadataStores,
    spu_clients: Arc<Mutex<HashMap<SpuId, SpuSocket>>>,
}

impl Drop for SpuPool {
    fn drop(&mut self) {
        trace!("dropping spu pool");
        self.shutdown();
    }
}

impl SpuPool {
    /// start synchronize based on pool
    pub fn start(config: Arc<ClientConfig>, metadata: MetadataStores) -> Result<Self, SocketError> {
        debug!("starting spu pool");
        Ok(Self {
            metadata,
            config,
            spu_clients: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    /// create new spu socket
    #[instrument(skip(self))]
    async fn connect_to_leader(&self, leader: SpuId) -> Result<SpuSocket, FluvioError> {
        let spu = self.metadata.spus().look_up_by_id(leader).await?;

        debug!("connecting to spu: {}", spu.spec);
        let mut client_config = self.config.with_prefix_sni_domain(spu.key());

        let spu_addr =
            if self.config.use_spu_local_address && spu.spec.public_endpoint_local.is_some() {
                let public_endpoint_local = spu.spec.public_endpoint_local.unwrap();
                let host = public_endpoint_local.host;
                let port = public_endpoint_local.port;
                format!("{}:{}", host, port)
            } else {
                spu.spec.public_endpoint.addr()
            };

        debug!("spu addr: {}", spu_addr);
        client_config.set_addr(spu_addr);
        let versioned_socket = client_config.connect().await?;
        let (socket, config, versions) = versioned_socket.split();
        Ok(SpuSocket {
            socket: MultiplexerSocket::shared(socket),
            config,
            versions,
        })
    }

    /// Create request/response socket to SPU for a replica
    ///
    /// All sockets to same SPU use a single TCP connection.
    /// First this looks up SPU address in SPU metadata and try to see if there is an existing TCP connection.
    /// If not, it will create a new connection and creates socket to it
    #[instrument(skip(self, replica))]
    pub async fn create_serial_socket(
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

    #[instrument(skip(self))]
    pub async fn create_serial_socket_from_leader(
        &self,
        leader_id: SpuId,
    ) -> Result<VersionedSerialSocket, FluvioError> {
        // check if already have existing connection to same SPU
        let mut client_lock = self.spu_clients.lock().await;

        if let Some(spu_socket) = client_lock.get_mut(&leader_id) {
            return Ok(spu_socket.create_serial_socket().await);
        }

        let mut spu_socket = self.connect_to_leader(leader_id).await?;
        let serial_socket = spu_socket.create_serial_socket().await;
        client_lock.insert(leader_id, spu_socket);

        Ok(serial_socket)
    }

    /// create stream to leader replica
    #[instrument(skip(self, replica, request, version))]
    pub async fn create_stream_with_version<R: Request>(
        &self,
        replica: &ReplicaKey,
        request: R,
        version: i16,
    ) -> Result<AsyncResponse<R>, FluvioError> {
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
                .await;
        }

        let mut spu_socket = self.connect_to_leader(leader_id).await?;
        let stream = spu_socket
            .create_stream_with_version(request, version)
            .await?;
        client_lock.insert(leader_id, spu_socket);

        Ok(stream)
    }

    pub async fn topic_exists<S: Into<String>>(&self, topic: S) -> Result<bool, FluvioError> {
        let replica = ReplicaKey::new(topic, 0);
        Ok(self
            .metadata
            .partitions()
            .lookup_by_key(&replica)
            .await?
            .is_some())
    }

    pub fn shutdown(&mut self) {
        self.metadata.shutdown();
    }
}
