use fluvio_storage::config::ConfigOption;
use fluvio_storage::FileReplica;
use fluvio_storage::StorageError;
use fluvio_controlplane_metadata::partition::ReplicaKey;
use fluvio_types::SpuId;

use crate::config::Log;


/* 
pub async fn clear_replica_storage(local_spu: SpuId, replica: &ReplicaKey, config: &Log) {
    let storage_config = config.as_storage_config();
    let config = default_config(local_spu, &storage_config);
    FileReplica::clear(replica, &config).await
}
*/
