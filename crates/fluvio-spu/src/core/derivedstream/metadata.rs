use fluvio_controlplane_metadata::message::DerivedStreamControlData;

use crate::core::Spec;
use crate::core::LocalStore;

impl Spec for DerivedStreamControlData {
    const LABEL: &'static str = "DerivedStream";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}

pub type DerivedStreamStore = LocalStore<DerivedStreamControlData>;
