use fluvio_controlplane::spu_api::update_mirror::Mirror;
use std::sync::Arc;

use crate::core::Spec;
use crate::core::LocalStore;

pub type MirrorLocalStore = LocalStore<Mirror>;

pub type SharedMirrorLocalStore = Arc<MirrorLocalStore>;

impl Spec for Mirror {
    const LABEL: &'static str = "Mirror";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}
