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

impl SmartModuleLocalStore {
    #[cfg(test)]
    pub fn indexed_by_name(&self) -> std::collections::BTreeMap<SmartModuleName, SmartModule> {
        let mut map: std::collections::BTreeMap<SmartModuleName, SmartModule> =
            std::collections::BTreeMap::new();

        for sm in self.inner_store().read().values() {
            map.insert(sm.name.clone(), sm.clone());
        }

        map
    }
}
