use tracing::debug;

use kf_protocol::api::ReplicaKey;

use crate::ClientError;
use crate::client::ClientConfig;
use crate::sync::MetadataStores;
use crate::client::RawClient;

/// connection pool to spu
#[derive(Clone)]
pub struct SpuPool {
    config: ClientConfig,
    metadata: MetadataStores,
}

impl SpuPool {
    /// create new spu pool from client config template and metadata store
    pub fn new(config: ClientConfig, metadata: MetadataStores) -> Self {
        Self { metadata, config }
    }

    // find spu leader by replica
    pub async fn spu_leader(&self, replica: &ReplicaKey) -> Result<RawClient, ClientError> {
        let partition = self.metadata.partitions().lookup_by_key(replica).await?;
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
        client_config.connect().await
    }
}
