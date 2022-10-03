use std::sync::Arc;

use k8_client::{ClientError, K8Client, meta_client::MetadataClient};
use fluvio_controlplane_metadata::smartmodule::{SmartModuleSpec, SmartModuleV1Wrapper};
use k8_types::{UpdatedK8Obj};
use tracing::{debug, info};

/// Migrate SmartModule V1 to V2

pub(crate) struct SmartModuleMigrationController(Arc<K8Client>);

impl SmartModuleMigrationController {
    /// migrate code
    pub(crate) async fn migrate(client: Arc<K8Client>, ns: &str) -> Result<(), ClientError> {
        let controller = Self(client);
        controller.migrate_crd(ns).await
    }

    async fn migrate_crd(&self, ns: &str) -> Result<(), ClientError> {
        let old_smartmodules = self.0.retrieve_items::<SmartModuleV1Wrapper, _>(ns).await?;
        info!(
            old_smartmodule = old_smartmodules.items.len(),
            "SmartModule V1 found"
        );
        for old_sm in old_smartmodules.items {
            let old_spec_wrapper = old_sm.spec;
            let old_metadata = old_sm.metadata;
            if let Some(old_spec) = old_spec_wrapper.inner {
                info!("migrating v1 smartmodule: {}", old_metadata.name);
                let new_spec: SmartModuleSpec = old_spec.into();
                // debug!("new spec: {:#?}", new_spec);
                let input: UpdatedK8Obj<SmartModuleSpec> =
                    UpdatedK8Obj::new(new_spec, old_metadata.clone().into());

                let _smartmodulev2 = self.0.replace_item(input).await?;
            } else {
                debug!(%old_metadata.name, "no v1 smartmodule, skipping");
            }
        }

        Ok(())
    }
}
