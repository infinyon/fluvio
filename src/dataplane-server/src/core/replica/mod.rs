mod metadata;

pub use self::metadata::ReplicaStore;

use std::sync::Arc;

pub type SharedReplicaLocalStore = Arc<ReplicaStore>;
