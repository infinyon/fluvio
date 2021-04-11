use fluvio_storage::config::ConfigOption;
use fluvio_storage::FileReplica;
use fluvio_storage::StorageError;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_types::SpuId;

use crate::config::Log;

fn default_config(spu_id: SpuId, config: &ConfigOption) -> ConfigOption {
    let base_dir = config.base_dir.join(format!("spu-logs-{}", spu_id));
    let new_config = config.clone();
    new_config.base_dir(base_dir)
}

/// Create new replica storage.  Each replica is stored with 'spu' prefix
pub(crate) async fn create_replica_storage(
    local_spu: SpuId,
    replica: &ReplicaKey,
    config: &Log,
) -> Result<FileReplica, StorageError> {
    let storage_config = config.as_storage_config();
    let config = default_config(local_spu, &storage_config);
    FileReplica::create(replica.topic.clone(), replica.partition as u32, 0, &storage_config).await
}

pub async fn clear_replica_storage(
    local_spu: SpuId,
    replica: &ReplicaKey,
    config: &Log,
) {
    let storage_config = config.as_storage_config();
    let config = default_config(local_spu, &storage_config);
    FileReplica::clear(replica, &config).await
}
