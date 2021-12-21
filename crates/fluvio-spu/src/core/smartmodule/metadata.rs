use fluvio_controlplane_metadata::smartmodule::SmartModule;
use fluvio_types::SmartModuleName;

use crate::core::Spec;
use crate::core::LocalStore;

impl Spec for SmartModule {
    const LABEL: &'static str = "SmartModule";

    type Key = SmartModuleName;

    fn key(&self) -> &Self::Key {
        &self.name
    }

    fn key_owned(&self) -> Self::Key {
        self.name.clone()
    }
}

pub type SmartModuleLocalStore = LocalStore<SmartModule>;
