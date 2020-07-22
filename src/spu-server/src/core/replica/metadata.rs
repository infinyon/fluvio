use flv_metadata::partition::Replica;
use flv_metadata::partition::ReplicaKey;

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

pub type ReplicaStore = LocalStore<Replica>;
