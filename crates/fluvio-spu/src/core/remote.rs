use std::sync::Arc;

use fluvio_controlplane::spu_api::update_remote::Remote;

use crate::core::Spec;
use crate::core::LocalStore;

pub type RemoteLocalStore = LocalStore<Remote>;

pub type SharedRemoteLocalStore = Arc<RemoteLocalStore>;

impl Spec for Remote {
    const LABEL: &'static str = "Remote";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}
