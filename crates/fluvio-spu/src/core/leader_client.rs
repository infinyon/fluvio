use std::{collections::HashMap, fmt::Debug, sync::Arc};

use async_lock::Mutex;
use async_trait::async_trait;

use fluvio::FluvioError;
use fluvio::spu::{SpuDirectory, SpuSocket};
use fluvio::sockets::{ClientConfig, VersionedSerialSocket};
use fluvio_socket::MultiplexerSocket;
use fluvio_types::SpuId;
use tracing::{debug, instrument};

use super::SharedReplicaLocalStore;
use super::spus::SharedSpuLocalStore;

/// maintain connections to all leaders
#[derive(Debug, Default)]
pub struct LeaderConnections {
    spus: SharedSpuLocalStore,
    replicas: SharedReplicaLocalStore,
    leaders: Arc<Mutex<HashMap<SpuId, SpuSocket>>>,
}

impl LeaderConnections {
    pub fn new(spus: SharedSpuLocalStore, replicas: SharedReplicaLocalStore) -> Self {
        LeaderConnections {
            spus,
            replicas,
            ..Default::default()
        }
    }

    /// create a connection to leader, it can't find it, return
    #[instrument(skip(self))]
    async fn connect_to_leader(&self, leader: SpuId) -> Result<SpuSocket, FluvioError> {
        if let Some(spu) = self.spus.spec(&leader) {
            debug!("connecting to spu : {:#?}", spu);
            let client_config = ClientConfig::with_addr(spu.public_endpoint.addr());
            let versioned_socket = client_config.connect().await?;
            let (socket, config, versions) = versioned_socket.split();
            Ok(SpuSocket::new(
                config,
                MultiplexerSocket::shared(socket),
                versions,
            ))
        } else {
            Err(FluvioError::SPUNotFound(leader))
        }
    }
}

#[async_trait]
impl SpuDirectory for LeaderConnections {
    async fn create_serial_socket(
        &self,
        _replica: &dataplane::ReplicaKey,
    ) -> Result<VersionedSerialSocket, fluvio::FluvioError> {
        panic!("no need")
    }

    async fn create_stream_with_version<R: dataplane::api::Request>(
        &self,
        replica: &dataplane::ReplicaKey,
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
                    .await;
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
