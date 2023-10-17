use k8_client::meta_client::MetadataClient as K8MetadataClient;

use anyhow::{Result, anyhow};
use futures_util::{stream::BoxStream, StreamExt};
use k8_client::meta_client::NameSpace as K8NameSpace;
use tracing::{trace, debug, error};

use fluvio_stream_model::{
    k8_types::{
        Spec as K8Spec,
        options::{DeleteOptions, PropogationPolicy},
        InputK8Obj, UpdateK8ObjStatus, K8Watch,
    },
    store::{
        MetadataStoreList,
        k8::{K8MetaItem, K8ExtendedSpec, K8ConvertError},
        MetadataStoreObject, NameSpace,
        actions::LSUpdate,
    },
    core::{Spec, MetadataItem},
};

use super::MetadataClient;

#[async_trait::async_trait]
impl<T: K8MetadataClient> MetadataClient<K8MetaItem> for T {
    async fn retrieve_items<S>(
        &self,
        namespace: &NameSpace,
    ) -> Result<MetadataStoreList<S, K8MetaItem>>
    where
        S: K8ExtendedSpec,
    {
        let multi_namespace_context = matches!(namespace, NameSpace::All);
        let namespace = to_k8_namespace(namespace);
        let k8_objects = K8MetadataClient::retrieve_items::<S::K8Spec, _>(self, namespace).await?;

        let version = k8_objects.metadata.resource_version;

        let mut items = Vec::with_capacity(k8_objects.items.len());
        for item in k8_objects.items {
            match S::convert_from_k8(item, multi_namespace_context) {
                Ok(converted) => {
                    trace!("converted val: {converted:#?}");
                    items.push(converted);
                }
                Err(K8ConvertError::Skip(obj)) => {
                    debug!("skipping: {} {}", S::LABEL, obj.metadata.name);
                    continue;
                }
                Err(K8ConvertError::KeyConvertionError(err)) => return Err(err.into()),
                Err(K8ConvertError::Other(err)) => return Err(err.into()),
            }
        }
        Ok(MetadataStoreList { version, items })
    }

    async fn delete_item<S>(&self, metadata: K8MetaItem) -> Result<()>
    where
        S: K8ExtendedSpec,
    {
        let options = if S::DELETE_WAIT_DEPENDENTS {
            Some(DeleteOptions {
                propagation_policy: Some(PropogationPolicy::Foreground),
                ..Default::default()
            })
        } else {
            None
        };

        K8MetadataClient::delete_item_with_option::<S::K8Spec, _>(self, metadata.inner(), options)
            .await?;
        Ok(())
    }

    async fn finalize_delete_item<S>(&self, meta: K8MetaItem) -> Result<()>
    where
        S: K8ExtendedSpec,
    {
        use once_cell::sync::Lazy;
        use serde_json::Value;

        use k8_client::meta_client::PatchMergeType::JsonMerge;

        static FINALIZER: Lazy<Value> = Lazy::new(|| {
            serde_json::from_str(
                r#"
                {
                    "metadata": {
                        "finalizers":null
                    }
                }
            "#,
            )
            .expect("finalizer")
        });

        debug!("final deleting {:#?}", meta);

        K8MetadataClient::patch::<S::K8Spec, _>(
            self,
            &meta.inner().as_input(),
            &FINALIZER,
            JsonMerge,
        )
        .await
        .map(|_| ())
    }

    async fn apply<S>(&self, value: MetadataStoreObject<S, K8MetaItem>) -> Result<()>
    where
        S: K8ExtendedSpec,
        <S as Spec>::Owner: K8ExtendedSpec,
    {
        debug!(label = S::LABEL, key = ?value.key(), "K8 applying");
        trace!("adding KV {:#?} to k8 kv", value);

        let (key, spec, _status, ctx) = value.parts();
        let k8_spec: S::K8Spec = spec.into_k8();

        let mut input_metadata = if let Some(parent_metadata) = ctx.owner() {
            debug!("owner exists");
            let item_name = key.to_string();

            let mut input_metadata = parent_metadata
                .make_child_input_metadata::<<<S as Spec>::Owner as K8ExtendedSpec>::K8Spec>(
                    item_name,
                );
            // set labels

            if let Some(finalizer) = S::FINALIZER {
                input_metadata.finalizers = vec![finalizer.to_owned()];
                for o_ref in &mut input_metadata.owner_references {
                    o_ref.block_owner_deletion = true;
                }
            }
            input_metadata
        } else {
            ctx.item().inner().clone().into()
        };

        input_metadata.labels = ctx.item().get_labels();
        input_metadata.annotations = ctx.item().annotations.clone();

        trace!("converted metadata: {:#?}", input_metadata);
        let new_k8 = InputK8Obj::new(k8_spec, input_metadata);

        debug!("input {:#?}", new_k8);

        K8MetadataClient::apply(self, new_k8).await.map(|_| ())
    }

    async fn update_spec<S>(&self, metadata: K8MetaItem, spec: S) -> Result<()>
    where
        S: K8ExtendedSpec,
    {
        debug!("K8 Update Spec: {} key: {}", S::LABEL, metadata.name);
        trace!("K8 Update Spec: {:#?}", spec);
        let k8_spec: <S as K8ExtendedSpec>::K8Spec = spec.into_k8();

        trace!("updating spec: {:#?}", k8_spec);

        let k8_input: InputK8Obj<S::K8Spec> = InputK8Obj {
            api_version: S::K8Spec::api_version(),
            kind: S::K8Spec::kind(),
            metadata: metadata.inner().clone().into(),
            spec: k8_spec,
            ..Default::default()
        };

        K8MetadataClient::apply(self, k8_input).await.map(|_| ())
    }

    async fn update_spec_by_key<S>(
        &self,
        key: S::IndexKey,
        namespace: &NameSpace,
        spec: S,
    ) -> Result<()>
    where
        S: K8ExtendedSpec,
    {
        let meta = K8MetaItem::new(key.to_string(), namespace.to_string());
        self.update_spec(meta, spec).await
    }

    async fn update_status<S>(
        &self,
        metadata: K8MetaItem,
        status: S::Status,
        namespace: &NameSpace,
    ) -> Result<MetadataStoreObject<S, K8MetaItem>>
    where
        S: K8ExtendedSpec,
    {
        debug!(
            key = %metadata.name,
            version = %metadata.resource_version,
            "update status begin",
        );
        debug!(
            "K8 Update Status: {} key: {} value: {:?}",
            S::LABEL,
            metadata.name,
            status
        );
        trace!("status update: {:#?}", status);
        let k8_status: <S::K8Spec as K8Spec>::Status = S::convert_status_from_k8(status);

        let k8_input: UpdateK8ObjStatus<S::K8Spec> = UpdateK8ObjStatus {
            api_version: S::K8Spec::api_version(),
            kind: S::K8Spec::kind(),
            metadata: metadata.inner().clone().into(),
            status: k8_status,
            ..Default::default()
        };

        let k8_object = K8MetadataClient::update_status(self, &k8_input).await?;
        let multi_namespace_context = matches!(namespace, NameSpace::All);
        S::convert_from_k8(k8_object, multi_namespace_context)
            .map_err(|e| anyhow!("{}, error  converting back: {e:#?}", S::LABEL))
    }

    fn watch_stream_since<S>(
        &self,
        namespace: &NameSpace,
        resource_version: Option<String>,
    ) -> BoxStream<'_, Result<Vec<LSUpdate<S, K8MetaItem>>>>
    where
        S: K8ExtendedSpec,
    {
        let multi_namespace_context = matches!(namespace, NameSpace::All);
        let namespace = to_k8_namespace(namespace);
        let stream =
            K8MetadataClient::watch_stream_since::<S::K8Spec, _>(self, namespace, resource_version);
        let mapped = stream.map(move |result| {
            let result = result?;
            let mut changes: Vec<LSUpdate<S, K8MetaItem>> = Vec::with_capacity(result.len());
            for watch_obj in result {
                let watch_obj = match watch_obj {
                    Ok(watch_obj) => watch_obj,
                    Err(err) => {
                        error!("Problem parsing {} event: {}", S::LABEL, err);
                        continue;
                    }
                };
                match watch_obj {
                    K8Watch::ADDED(k8_obj) => {
                        trace!("{} ADDED: {:#?}", S::LABEL, k8_obj);
                        match S::convert_from_k8(k8_obj, multi_namespace_context) {
                            Ok(new_kv_value) => {
                                debug!("K8: Watch Add: {}:{:?}", S::LABEL, new_kv_value.key());
                                changes.push(LSUpdate::Mod(new_kv_value));
                            }
                            Err(err) => match err {
                                K8ConvertError::Skip(obj) => {
                                    debug!("skipping: {}", obj.metadata.name);
                                }
                                _ => {
                                    error!("converting {} {:#?}", S::LABEL, err);
                                }
                            },
                        }
                    }
                    K8Watch::MODIFIED(k8_obj) => {
                        trace!("{} MODIFIED: {:#?}", S::LABEL, k8_obj);
                        match S::convert_from_k8(k8_obj, multi_namespace_context) {
                            Ok(updated_kv_value) => {
                                debug!(
                                    "K8: Watch Update {}:{:?}",
                                    S::LABEL,
                                    updated_kv_value.key()
                                );
                                changes.push(LSUpdate::Mod(updated_kv_value));
                            }
                            Err(err) => match err {
                                K8ConvertError::Skip(obj) => {
                                    debug!("skipping: {}", obj.metadata.name);
                                }
                                _ => {
                                    error!("converting {} {:#?}", S::LABEL, err);
                                }
                            },
                        }
                    }
                    K8Watch::DELETED(k8_obj) => {
                        trace!("{} DELETE: {:#?}", S::LABEL, k8_obj);
                        let meta: Result<
                            MetadataStoreObject<S, K8MetaItem>,
                            K8ConvertError<S::K8Spec>,
                        > = S::convert_from_k8(k8_obj, multi_namespace_context);
                        match meta {
                            Ok(kv_value) => {
                                debug!("K8: Watch Delete {}:{:?}", S::LABEL, kv_value.key());
                                changes.push(LSUpdate::Delete(kv_value.key_owned()));
                            }
                            Err(err) => match err {
                                K8ConvertError::Skip(obj) => {
                                    debug!("skipping: {}", obj.metadata.name);
                                }
                                _ => {
                                    error!("converting {} {:#?}", S::LABEL, err);
                                }
                            },
                        }
                    }
                }
            }

            Ok(changes)
        });
        mapped.boxed()
    }
}

fn to_k8_namespace(ns: &NameSpace) -> K8NameSpace {
    match ns {
        NameSpace::All => K8NameSpace::All,
        NameSpace::Named(s) => K8NameSpace::Named(s.clone()),
    }
}

#[cfg(test)]
mod tests {
    use std::{fmt::Display, time::Duration};

    use fluvio_stream_model::{
        core::Status,
        k8_types::{Status as K8Status, DefaultHeader, Crd, CrdNames},
        store::k8::default_convert_from_k8,
    };
    use k8_client::memory::MemoryClient;
    use serde::{Serialize, Deserialize};

    use super::*;

    #[fluvio_future::test]
    async fn test_retrieve_insert_delete_items() {
        //given
        let k8_client = MemoryClient::default();
        let meta_object: MetadataStoreObject<TestSpec, K8MetaItem> = MetadataStoreObject::new(
            "spec1".to_string(),
            TestSpec,
            TestSpecStatus("ok".to_string()),
        );

        //when
        let empty = MetadataClient::retrieve_items::<TestSpec>(&k8_client, &NameSpace::All)
            .await
            .expect("retrieved");
        MetadataClient::apply::<TestSpec>(&k8_client, meta_object.clone())
            .await
            .expect("applied");
        let non_empty = MetadataClient::retrieve_items::<TestSpec>(&k8_client, &NameSpace::All)
            .await
            .expect("retrieved");
        MetadataClient::delete_item::<TestSpec>(&k8_client, meta_object.ctx().item().clone())
            .await
            .expect("deleted");
        let after_delete = MetadataClient::retrieve_items::<TestSpec>(&k8_client, &NameSpace::All)
            .await
            .expect("retrieved");

        //then
        assert!(empty.items.is_empty());
        assert_eq!(non_empty.items.len(), 1);
        assert!(after_delete.items.is_empty());
    }

    #[fluvio_future::test]
    async fn test_update_status() {
        //given
        let k8_client = MemoryClient::default();
        let namespace = NameSpace::Named("ns1".to_string());
        let key = "key".to_string();
        let meta = K8MetaItem::new(key.clone(), namespace.to_string());
        let meta_object: MetadataStoreObject<TestSpec, K8MetaItem> =
            MetadataStoreObject::new_with_context(key.clone(), TestSpec, meta.clone().into());

        //when
        MetadataClient::apply(&k8_client, meta_object)
            .await
            .expect("applied");
        MetadataClient::update_status::<TestSpec>(
            &k8_client,
            meta,
            TestSpecStatus("new status".to_string()),
            &namespace,
        )
        .await
        .expect("updated status");

        let items = MetadataClient::retrieve_items::<TestSpec>(&k8_client, &namespace)
            .await
            .unwrap();

        //then
        assert_eq!(items.items.len(), 1);
        assert_eq!(items.items[0].status().to_string(), "new status");
        assert_eq!(items.items[0].ctx().item().revision(), 1);
        assert_eq!(items.items[0].ctx().item().namespace(), "ns1");
    }

    #[fluvio_future::test]
    async fn test_watch_stream_since_start() {
        //given
        let k8_client = MemoryClient::default();
        let namespace = NameSpace::Named("ns1".to_string());
        let stream = MetadataClient::watch_stream_since::<TestSpec>(&k8_client, &namespace, None);
        let key = "key".to_string();
        let meta = K8MetaItem::new(key.clone(), namespace.to_string());
        let meta_object: MetadataStoreObject<TestSpec, K8MetaItem> =
            MetadataStoreObject::new_with_context(key.clone(), TestSpec, meta.clone().into());

        //when
        MetadataClient::apply(&k8_client, meta_object.clone())
            .await
            .expect("applied");
        MetadataClient::update_status::<TestSpec>(
            &k8_client,
            meta_object.ctx().item().clone(),
            TestSpecStatus("new status".to_string()),
            &namespace,
        )
        .await
        .expect("updated status");
        MetadataClient::delete_item::<TestSpec>(&k8_client, meta_object.ctx().item().clone())
            .await
            .expect("deleted");

        let updates = stream
            .take_until(fluvio_future::timer::sleep(Duration::from_secs(2)))
            .collect::<Vec<Result<Vec<LSUpdate<TestSpec, K8MetaItem>>>>>()
            .await;

        //then

        let updates: Vec<_> = updates.into_iter().flatten().flatten().collect();
        assert_eq!(updates.len(), 3);

        assert!(
            matches!(updates.get(0), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq(""))
        );
        assert!(
            matches!(updates.get(1), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq("new status"))
        );
        assert!(matches!(updates.get(2), Some(LSUpdate::Delete(obj)) if obj.to_string().eq(&key)));
    }

    #[fluvio_future::test]
    async fn test_apply_with_parent() {
        //given
        let k8_client = MemoryClient::default();
        let namespace = NameSpace::Named("ns1".to_string());
        let key = "child".to_string();
        let parent_key = "parent".to_string();
        let meta = K8MetaItem::new(key.clone(), namespace.to_string());
        let parent_meta = K8MetaItem::new(parent_key.clone(), namespace.to_string());

        let ctx = fluvio_stream_model::store::k8::K8MetadataContext::new(meta, Some(parent_meta));

        let obj = MetadataStoreObject::new_with_context(key.clone(), TestSpec, ctx);

        //when
        MetadataClient::apply(&k8_client, obj.clone())
            .await
            .expect("applied");
        let items = MetadataClient::retrieve_items::<TestSpec>(&k8_client, &namespace)
            .await
            .expect("retrieved");

        dbg!(&items);
        assert_eq!(items.items.len(), 1);
        assert_eq!(
            items.items[0]
                .ctx()
                .item()
                .inner()
                .owner_references
                .get(0)
                .unwrap()
                .name,
            "parent"
        );
        assert!(items.items[0]
            .ctx()
            .item()
            .inner()
            .finalizers
            .contains(&"FINALIZER1".to_string()));
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestSpec;

    #[derive(Debug, Default, Clone, PartialEq, Eq)]
    struct TestSpecStatus(String);

    impl Spec for TestSpec {
        const LABEL: &'static str = "TEST_SPEC";
        type Status = TestSpecStatus;
        type Owner = Self;
        type IndexKey = String;
    }

    impl Status for TestSpecStatus {}

    impl K8ExtendedSpec for TestSpec {
        type K8Spec = TestK8Spec;

        const FINALIZER: Option<&'static str> = Some("FINALIZER1");

        fn convert_from_k8(
            k8_obj: fluvio_stream_model::k8_types::K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> std::result::Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
        {
            default_convert_from_k8(k8_obj, multi_namespace_context)
        }

        fn convert_status_from_k8(status: Self::Status) -> <Self::K8Spec as K8Spec>::Status {
            TestK8SpecStatus(status.0)
        }

        fn into_k8(self) -> Self::K8Spec {
            TestK8Spec
        }
    }

    impl Display for TestSpecStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    struct TestK8Spec;

    #[derive(Default, Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
    pub struct TestK8SpecStatus(String);

    impl K8Status for TestK8SpecStatus {}

    impl Display for TestK8SpecStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl K8Spec for TestK8Spec {
        type Status = TestK8SpecStatus;
        type Header = DefaultHeader;

        fn metadata() -> &'static Crd {
            &Crd {
                group: "test.fluvio",
                version: "v1",
                names: CrdNames {
                    kind: "myspec",
                    plural: "myspecs",
                    singular: "myspec",
                },
            }
        }
    }

    impl From<TestK8Spec> for TestSpec {
        fn from(_value: TestK8Spec) -> Self {
            Self
        }
    }

    impl From<TestK8SpecStatus> for TestSpecStatus {
        fn from(value: TestK8SpecStatus) -> Self {
            Self(value.0)
        }
    }
}
