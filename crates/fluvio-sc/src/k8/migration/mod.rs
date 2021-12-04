use std::sync::Arc;

use k8_client::{ClientError, K8Client};
use k8_metadata_client::MetadataClient;
use fluvio_controlplane_metadata::topic::{TopicSpec, TopicSpecV1};

/// Migrate old version of CRD to new version

pub(crate) struct MigrationController(Arc<K8Client>);

impl MigrationController {
   
    /// migrate code 
    pub(crate) fn migrate(client: Arc<K8Client>,ns: &str) {
        let controller = MigrationController(client);
        controller.migrate_crd(ns);
    }

    async fn migrate_crd(&self,ns: &str) -> Result<(),ClientError> {
        let old_topics = self.0.retrieve_items::<TopicSpecV1,_>(ns).await?;
        for old_topic in old_topics.items {
            let old_spec = old_topic.spec;
            let old_status = old_topic.status;
            let new_spec: TopicSpec = old_spec.into();
            self.0.create_item(ns,&new_spec).await?;
        }
        Ok(())
    }
    
}
