//!
//! # Global Context
//!
//! Global Context maintains states need to be shared across in the SPU

use std::sync::Arc;

use once_cell::sync::OnceCell;

use fluvio_types::SpuId;

use fluvio_smartengine::SmartEngine;

use crate::config::SpuConfig;
use crate::control_plane::{StatusMessageSink, SharedStatusUpdate};

use super::leader_client::LeaderConnections;
use super::smartmodule::SmartModuleLocalStore;
use super::derivedstream::DerivedStreamStore;
use super::spus::SharedSpuLocalStore;
use super::SharedReplicaLocalStore;
use super::smartmodule::SharedSmartModuleLocalStore;
use super::derivedstream::SharedStreamStreamLocalStore;
use super::spus::SpuLocalStore;
use super::replica::ReplicaStore;
use super::SharedSpuConfig;

static SPU_STORE: OnceCell<SharedSpuLocalStore> = OnceCell::new();
static REPLICA_STORE: OnceCell<SharedReplicaLocalStore> = OnceCell::new();
static SMARTMODULE_STORE: OnceCell<SharedSmartModuleLocalStore> = OnceCell::new();
static DERIVEDSTREAM_STORE: OnceCell<SharedStreamStreamLocalStore> = OnceCell::new();
static STATUS_UPDATE: OnceCell<SharedStatusUpdate> = OnceCell::new();
static CONFIG: OnceCell<SharedSpuConfig> = OnceCell::new();
static SMART_ENGINE: OnceCell<SmartEngine> = OnceCell::new();
static LEADERS: OnceCell<Arc<LeaderConnections>> = OnceCell::new();

pub(crate) fn spu_local_store() -> &'static SpuLocalStore {
    SPU_STORE.get().unwrap()
}

pub(crate) fn spu_local_store_owned() -> SharedSpuLocalStore {
    SPU_STORE.get().unwrap().clone()
}

pub(crate) fn replica_localstore() -> &'static ReplicaStore {
    REPLICA_STORE.get().unwrap()
}

pub(crate) fn smartmodule_localstore() -> &'static SmartModuleLocalStore {
    SMARTMODULE_STORE.get().unwrap()
}

pub(crate) fn derivedstream_store() -> &'static DerivedStreamStore {
    DERIVEDSTREAM_STORE.get().unwrap()
}

pub(crate) fn status_update_owned() -> SharedStatusUpdate {
    STATUS_UPDATE.get().unwrap().clone()
}

#[cfg(test)]
pub(crate) fn status_update() -> &'static StatusMessageSink {
    STATUS_UPDATE.get().unwrap()
}

/// retrieves local spu id
pub(crate) fn local_spu_id() -> SpuId {
    CONFIG.get().unwrap().id
}

pub(crate) fn config() -> &'static SpuConfig {
    CONFIG.get().unwrap()
}

pub(crate) fn config_owned() -> SharedSpuConfig {
    CONFIG.get().unwrap().clone()
}

/// notify all follower handlers with SPU changes

pub(crate) fn smartengine_owned() -> SmartEngine {
    SMART_ENGINE.get().unwrap().clone()
}

pub(crate) fn leaders() -> Arc<LeaderConnections> {
    LEADERS.get().unwrap().clone()
}

/// initialize global variables
pub(crate) fn initialize(spu_config: SpuConfig) {
    let spus = SpuLocalStore::new_shared();
    let replicas = ReplicaStore::new_shared();

    SPU_STORE.set(spus.clone()).unwrap();
    REPLICA_STORE.set(replicas.clone()).unwrap();
    SMARTMODULE_STORE
        .set(SmartModuleLocalStore::new_shared())
        .unwrap();
    DERIVEDSTREAM_STORE
        .set(DerivedStreamStore::new_shared())
        .unwrap();

    STATUS_UPDATE.set(StatusMessageSink::shared()).unwrap();
    CONFIG.set(Arc::new(spu_config)).unwrap();
    SMART_ENGINE.set(SmartEngine::default()).unwrap();
    LEADERS
        .set(LeaderConnections::shared(spus, replicas))
        .unwrap();

    crate::replication::initialize_replica();
}
