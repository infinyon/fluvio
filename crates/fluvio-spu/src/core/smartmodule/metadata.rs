use anyhow::Result;

use fluvio_controlplane_metadata::smartmodule::SmartModule;
use fluvio_controlplane_metadata::smartmodule::SmartModulePackageKey;
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

impl LocalStore<SmartModule> {
    /// look by fully qualified SmartModule name
    pub fn find_by_pk_key(&self, fqdn: &str) -> Result<Option<SmartModule>> {
        let pkg_key = SmartModulePackageKey::from_qualified_name(fqdn)?;
        let reader = self.read();
        for (key, sm) in reader.iter() {
            if pkg_key.is_match(key, sm.spec.meta.as_ref().map(|m| &m.package)) {
                return Ok(Some(sm.clone()));
            }
        }
        Ok(None)
    }
}
