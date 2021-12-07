use std::sync::Arc;

use k8_client::{ClientError, K8Client};
use k8_metadata_client::MetadataClient;
use fluvio_controlplane_metadata::topic::{TopicSpec, TopicSpecV1};
use k8_types::{InputK8Obj, UpdateK8ObjStatus};
use tracing::info;

/// Migrate old version of CRD to new version

pub(crate) struct MigrationController(Arc<K8Client>);

impl MigrationController {
    /// migrate code
    pub(crate) async fn migrate(client: Arc<K8Client>, ns: &str) -> Result<(), ClientError> {
        let controller = MigrationController(client);
        controller.migrate_crd(ns).await
    }

    async fn migrate_crd(&self, ns: &str) -> Result<(), ClientError> {
        let old_topics = self.0.retrieve_items::<TopicSpecV1, _>(ns).await?;
        for old_topic in old_topics.items {
            let old_spec = old_topic.spec;
            let old_status = old_topic.status;
            let old_metadata = old_topic.metadata;
            let new_spec: TopicSpec = old_spec.into();
            info!(%old_metadata.name, "migrating topic");
            let input: InputK8Obj<TopicSpec> =
                InputK8Obj::new(new_spec, old_metadata.clone().into());
            self.0.apply(input).await?;
            let update_status: UpdateK8ObjStatus<TopicSpec> =
                UpdateK8ObjStatus::new(old_status, old_metadata.clone().into());
            self.0.update_status(&update_status).await?;
            self.0.delete_item::<TopicSpec, _>(&old_metadata).await?;
        }

        Ok(())
    }
}
