use std::ops::Deref;
use std::sync::Arc;

use fluvio_controlplane_metadata::partition::Replica;
use fluvio_controlplane_metadata::partition::ReplicaKey;

use crate::core::Spec;
use crate::core::LocalStore;

impl Spec for Replica {
    const LABEL: &'static str = "Replica";

    type Key = ReplicaKey;

    fn key(&self) -> &Self::Key {
        &self.id
    }

    fn key_owned(&self) -> Self::Key {
        self.id.clone()
    }
}

#[derive(Default, Debug)]
pub struct ReplicaStore(LocalStore<Replica>);

impl Deref for ReplicaStore {
    type Target = LocalStore<Replica>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl ReplicaStore {
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }
}
