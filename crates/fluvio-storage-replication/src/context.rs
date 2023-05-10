
use once_cell::sync::OnceCell;

use crate::core::{spu_local_store, local_spu_id};

use super::{
    leader::{SharedSpuUpdates, FollowerNotifier},
    DefaultSharedReplicaContext, FileReplicaContext,
};

static FOLLOWER_NOTIFIER: OnceCell<SharedSpuUpdates> = OnceCell::new();
static DEFAULT_REPLICA_CTX: OnceCell<DefaultSharedReplicaContext> = OnceCell::new();

pub(crate) fn follower_notifier() -> &'static FollowerNotifier {
    FOLLOWER_NOTIFIER.get().unwrap()
}

pub(crate) async fn sync_follower_update() {
    follower_notifier()
        .sync_from_spus(spu_local_store(), local_spu_id())
        .await;
}

pub(crate) fn default_replica_ctx() -> &'static DefaultSharedReplicaContext {
    DEFAULT_REPLICA_CTX.get().unwrap()
}

/// initialize global variables
pub(crate) fn initialize_replica() {
    FOLLOWER_NOTIFIER.set(FollowerNotifier::shared()).unwrap();
    DEFAULT_REPLICA_CTX
        .set(FileReplicaContext::new_shared_context())
        .unwrap();
}

