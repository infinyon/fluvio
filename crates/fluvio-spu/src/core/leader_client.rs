use std::{collections::HashMap, fmt::Debug, sync::Arc};

use tokio::sync::Mutex;
use async_trait::async_trait;

use fluvio::metrics::ClientMetrics;
use fluvio::{FluvioError, PartitionConsumer};
use fluvio::spu::SpuDirectory;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_socket::{ClientConfig, MultiplexerSocket, StreamSocket, VersionedSerialSocket};
use fluvio_types::{SpuId, PartitionId};
use tracing::{debug, instrument};

use super::SharedReplicaLocalStore;
use super::spus::SharedSpuLocalStore;

/// maintain connections to all leaders
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct LeaderConnections {
    spus: SharedSpuLocalStore,
    replicas: SharedReplicaLocalStore,
    leaders: Arc<Mutex<HashMap<SpuId, StreamSocket>>>,
    metrics: Arc<ClientMetrics>,
}

impl LeaderConnections {
    pub fn new(spus: SharedSpuLocalStore, replicas: SharedReplicaLocalStore) -> Self {
        LeaderConnections {
            spus,
            replicas,
            leaders: Default::default(),
            metrics: Arc::new(ClientMetrics::new()),
        }
    }
    pub fn shared(spus: SharedSpuLocalStore, replicas: SharedReplicaLocalStore) -> Arc<Self> {
        Arc::new(LeaderConnections::new(spus, replicas))
    }

    /// create a connection to leader, it can't find it, return
    #[instrument(skip(self))]
    async fn connect_to_leader(&self, leader: SpuId) -> Result<StreamSocket, FluvioError> {
        if let Some(spu) = self.spus.spec(&leader) {
            debug!("connecting to spu : {:#?}", spu);
            let client_config = ClientConfig::with_addr(spu.public_endpoint.addr());
            let versioned_socket = client_config.connect().await?;
            let (socket, config, versions) = versioned_socket.split();
            Ok(StreamSocket::new(
                config,
                MultiplexerSocket::shared(socket),
                versions,
            ))
        } else {
            Err(FluvioError::SPUNotFound(leader))
        }
    }

    /// create consumer connection to a leader
    #[instrument(skip(self))]
    pub async fn partition_consumer<S>(
        self: Arc<Self>,
        topic: S,
        partition: PartitionId,
    ) -> PartitionConsumer<LeaderConnections>
    where
        S: Into<String> + Debug,
    {
        PartitionConsumer::new(topic.into(), partition, self.clone(), self.metrics.clone())
    }
}

#[async_trait]
impl SpuDirectory for LeaderConnections {
    async fn create_serial_socket(
        &self,
        replica: &ReplicaKey,
    ) -> Result<VersionedSerialSocket, fluvio::FluvioError> {
        if let Some(replica_spec) = self.replicas.spec(replica) {
            let leader_id = replica_spec.leader;

            // check if already have existing connection to same SPU
            let mut client_lock = self.leaders.lock().await;

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
        } else {
            Err(FluvioError::TopicNotFound(replica.to_string()))
        }
    }

    async fn create_stream_with_version<R: fluvio_protocol::api::Request>(
        &self,
        replica: &ReplicaKey,
        request: R,
        version: i16,
    ) -> Result<fluvio_socket::AsyncResponse<R>, fluvio::FluvioError>
    where
        R: Sync + Send,
    {
        if let Some(replica_spec) = self.replicas.spec(replica) {
            let leader_id = replica_spec.leader;
            let mut client_lock = self.leaders.lock().await;

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
        } else {
            Err(FluvioError::TopicNotFound(replica.to_string()))
        }
    }
}
