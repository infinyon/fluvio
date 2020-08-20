use fluvio_storage::ConfigOption;
use fluvio_storage::FileReplica;
use fluvio_storage::StorageError;
use fluvio_metadata::partition::ReplicaKey;
use fluvio_types::SpuId;

fn default_config(spu_id: SpuId, config: &ConfigOption) -> ConfigOption {
    let base_dir = config.base_dir.join(format!("spu-logs-{}", spu_id));
    let new_config = config.clone();
    new_config.base_dir(base_dir)
}

/// Create new replica storage.  Each replica is stored with 'spu' prefix
pub(crate) async fn create_replica_storage(
    local_spu: SpuId,
    replica: &ReplicaKey,
    base_config: &ConfigOption,
) -> Result<FileReplica, StorageError> {
    let config = default_config(local_spu, base_config);
    FileReplica::create(replica.topic.clone(), replica.partition as u32, 0, &config).await
}
