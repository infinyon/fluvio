use fluvio_controlplane::spu_api::update_remote_cluster::RemoteCluster;
use std::sync::Arc;

use crate::core::Spec;
use crate::core::LocalStore;

pub type RemoteClusterLocalStore = LocalStore<RemoteCluster>;

pub type SharedRemoteClusterLocalStore = Arc<RemoteClusterLocalStore>;

impl Spec for RemoteCluster {
    const LABEL: &'static str = "RemoteCluster";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}
