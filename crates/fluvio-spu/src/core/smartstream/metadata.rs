use fluvio_controlplane_metadata::message::SmartStreamControlData;

use crate::core::Spec;
use crate::core::LocalStore;

impl Spec for SmartStreamControlData {
    const LABEL: &'static str = "SmartStream";

    type Key = String;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}

pub type SmartStreamStore = LocalStore<SmartStreamControlData>;
