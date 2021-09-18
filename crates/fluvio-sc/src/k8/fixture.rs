use std::iter;

use fluvio_stream_dispatcher::store::StoreContext;
use tracing::debug;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;

use fluvio_controlplane_metadata::store::{LocalStore, MetadataStoreObject};
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use k8_metadata_client::MetadataClient;
use k8_types::core::namespace::NamespaceSpec;
use k8_types::{InputK8Obj, InputObjectMeta, K8Obj};
use k8_client::{K8Client, SharedK8Client, load_and_share};

use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::config::ScConfig;
use crate::core::{Context, SharedContext};

type ScConfigMetadata = MetadataStoreObject<ScK8Config, K8MetaItem>;

//
// apiVersion: v1
// data:
//   image: infinyon/fluvio:b9281e320266e295e75200d10b769967e23d3ed2
// lbServiceAnnotations: '{"fluvio.io/ingress-address":"192.168.208.2"}'
// podSecurityContext: '{}'
// service: '{"type":"NodePort"}'
// spuPodConfig: '{"nodeSelector":{},"resources":{"limits":{"memory":"1Gi"},"requests":{"memory":"256Mi"}},"storageClass":null}'
//kind: ConfigMap

pub struct TestEnv {
    ns: K8Obj<NamespaceSpec>,
    client: SharedK8Client,
}

impl TestEnv {
    pub async fn create() -> Self {
        let client = load_and_share().expect("creating k8 client");
        let ns = Self::create_unique_ns();
        let ns_obj = Self::create_ns(&ns, &client).await;

        Self { ns: ns_obj, client }
    }

    fn create_unique_ns() -> String {
        let mut rng = thread_rng();
        let ns: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(7)
            .collect();
        ns.to_lowercase()
    }

    #[allow(unused)]
    async fn delete(self) {
        self.client
            .delete_item_with_option::<NamespaceSpec, _>(&self.ns.metadata, None)
            .await
            .expect("delete");
    }

    async fn create_ns(ns: &str, k8_client: &K8Client) -> K8Obj<NamespaceSpec> {
        let input_meta = InputObjectMeta {
            name: ns.to_owned(),
            ..Default::default()
        };

        debug!(%ns,"creating ns");
        let input = InputK8Obj::new(NamespaceSpec::default(), input_meta);
        k8_client.create_item(input).await.expect("ns created")
    }

    pub fn ns(&self) -> &str {
        &self.ns.metadata.name
    }

    pub fn client(&self) -> &SharedK8Client {
        &self.client
    }

    pub async fn create_global_ctx(&self) -> (SharedContext, StoreContext<ScK8Config>) {
        let config_map = ScConfigMetadata::with_spec("fluvio", ScK8Config::default());
        let config_store = LocalStore::new_shared();
        config_store.sync_all(vec![config_map]).await;

        let config_ctx: StoreContext<ScK8Config> = StoreContext::new_with_store(config_store);
        assert!(config_ctx.store().value("fluvio").await.is_some());

        (Context::shared_metadata(ScConfig::default()), config_ctx)
    }
}
