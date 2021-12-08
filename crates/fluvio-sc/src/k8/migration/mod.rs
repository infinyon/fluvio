use std::sync::Arc;

use k8_client::{ClientError, K8Client, meta_client::MetadataClient};
use fluvio_controlplane_metadata::topic::{TopicSpec, TopicV1Wrapper};
use k8_types::{UpdatedK8Obj};
use tracing::{debug};

/// Migrate old version of CRD to new version

pub(crate) struct MigrationController(Arc<K8Client>);

impl MigrationController {
    /// migrate code
    pub(crate) async fn migrate(client: Arc<K8Client>, ns: &str) -> Result<(), ClientError> {
        let controller = MigrationController(client);
        controller.migrate_crd(ns).await
    }

    async fn migrate_crd(&self, ns: &str) -> Result<(), ClientError> {
        let old_topics = self.0.retrieve_items::<TopicV1Wrapper, _>(ns).await?;
        for old_topic in old_topics.items {
            let old_spec_wrapper = old_topic.spec;
            let old_metadata = old_topic.metadata;
            if let Some(old_spec) = old_spec_wrapper.inner {
                println!("migrating v1 topic: {}", old_metadata.name);
                debug!("old topic: {:#?}", old_spec);
                let new_spec: TopicSpec = old_spec.into();
                debug!("new spec: {:#?}", new_spec);
                let input: UpdatedK8Obj<TopicSpec> =
                    UpdatedK8Obj::new(new_spec, old_metadata.clone().into());

                let _topicv2 = self.0.replace_item(input).await?;
            } else {
                debug!(%old_metadata.name, "no v1 topic, skipping");
            }
        }

        Ok(())
    }
}
