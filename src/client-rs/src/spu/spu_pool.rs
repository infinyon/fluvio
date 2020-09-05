use std::collections::HashMap;
use std::sync::Arc;

use tracing::debug;
use async_mutex::Mutex;

use kf_protocol::api::ReplicaKey;
use flv_types::SpuId;
use kf_socket::AllMultiplexerSocket;
use crate::ClientError;
use crate::client::ClientConfig;
use crate::sync::MetadataStores;
use crate::client::VersionedSerialSocket;
use crate::client::Versions;

struct SpuSocket {
    config: ClientConfig,
    socket: AllMultiplexerSocket,
    versions: Versions,
}

impl SpuSocket {
    async fn create_serial_socket(&mut self) -> VersionedSerialSocket {
        VersionedSerialSocket::new(
            self.socket.create_serial_socket().await,
            self.config.clone(),
            self.versions.clone(),
        )
    }
}

/// connection pool to spu
#[derive(Clone)]
pub struct SpuPool {
    config: ClientConfig,
    metadata: MetadataStores,
    spu_clients: Arc<Mutex<HashMap<SpuId, SpuSocket>>>,
}

impl SpuPool {
    /// create new spu pool from client config template and metadata store
    pub fn new(config: ClientConfig, metadata: MetadataStores) -> Self {
        Self {
            metadata,
            config,
            spu_clients: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // find spu leader by replica
    pub async fn create_serial_socket(
        &self,
        replica: &ReplicaKey,
    ) -> Result<VersionedSerialSocket, ClientError> {
        let partition = self.metadata.partitions().lookup_by_key(replica).await?;

        let leader_id = partition.spec.leader;

        // check if already have existing leader
        let mut client_lock = self.spu_clients.lock().await;

        if let Some(spu_socket) = client_lock.get_mut(&leader_id) {
            return Ok(spu_socket.create_serial_socket().await);
        }

        let spu = self
            .metadata
            .spus()
            .look_up_by_id(partition.spec.leader)
            .await?;

        debug!("connecting to spu: {}", spu.spec);
        let mut client_config = self.config.clone();
        let spu_addr = spu.spec.public_endpoint.addr();
        debug!("spu addr: {}", spu_addr);
        client_config.set_addr(spu_addr);
        let versioned_socket = client_config.connect().await?;
        let (socket, config, versions) = versioned_socket.split();
        let mut spu_socket = SpuSocket {
            socket: AllMultiplexerSocket::new(socket),
            config,
            versions,
        };

        let serial_socket = spu_socket.create_serial_socket().await;

        client_lock.insert(leader_id, spu_socket);

        Ok(serial_socket)
    }
}
