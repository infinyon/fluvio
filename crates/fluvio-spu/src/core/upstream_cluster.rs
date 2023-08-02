use std::sync::Arc;

use fluvio_controlplane::spu_api::update_upstream_cluster::UpstreamCluster;

use crate::core::Spec;
use crate::core::LocalStore;

pub type UpstreamClusterLocalStore = LocalStore<UpstreamCluster>;

pub type SharedUpstreamClusterLocalStore = Arc<UpstreamClusterLocalStore>;

impl Spec for UpstreamCluster {
    const LABEL: &'static str = "UpstreamCluster";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}
