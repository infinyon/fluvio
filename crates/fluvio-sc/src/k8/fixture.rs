use std::iter;

use fluvio_stream_dispatcher::metadata::SharedClient;
use fluvio_stream_dispatcher::store::StoreContext;
use tracing::debug;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;
use k8_client::K8Client;
use k8_client::meta_client::MetadataClient;

use fluvio_controlplane_metadata::store::{LocalStore, MetadataStoreObject};
use fluvio_controlplane_metadata::store::k8::K8MetaItem;
use fluvio_stream_model::k8_types::core::namespace::NamespaceSpec;
use fluvio_stream_model::k8_types::{InputK8Obj, InputObjectMeta, K8Obj};

use crate::k8::objects::spu_k8_config::ScK8Config;
use crate::config::ScConfig;
use crate::core::{Context, K8SharedContext};

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
    client: SharedClient<K8Client>,
}

impl TestEnv {
    pub async fn create() -> Self {
        let client = k8_client::load_and_share().expect("creating k8 client");
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
        use core::ops::Deref;
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

    pub(crate) fn client(&self) -> &SharedClient<K8Client> {
        &self.client
    }

    pub async fn create_global_ctx(
        &self,
    ) -> (K8SharedContext, StoreContext<ScK8Config, K8MetaItem>) {
        let config_map = ScConfigMetadata::with_spec("fluvio", ScK8Config::default());
        let config_store = LocalStore::new_shared();
        config_store.sync_all(vec![config_map]).await;

        let config_ctx: StoreContext<ScK8Config, K8MetaItem> =
            StoreContext::new_with_store(config_store);
        assert!(config_ctx.store().value("fluvio").await.is_some());

        (Context::shared_metadata(ScConfig::default()), config_ctx)
    }
}
