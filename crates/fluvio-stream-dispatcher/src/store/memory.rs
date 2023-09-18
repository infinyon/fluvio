use std::collections::HashMap;
use std::sync::Arc;
use core::fmt::Display;

use async_channel::{Sender, Receiver, bounded};
use async_lock::Mutex;
use async_lock::RwLock;
use tracing::debug;
use futures_util::FutureExt;
use futures_util::StreamExt;
use futures_util::stream::BoxStream;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_yaml::Value;

use fluvio_stream_model::k8_types::Spec;
use fluvio_stream_model::k8_types::ListMetadata;
use fluvio_stream_model::k8_types::MetaStatus;
use fluvio_stream_model::k8_types::K8Watch;
use k8_diff::DiffError;
use k8_metadata_client::MetadataClientError;
use k8_metadata_client::TokenStreamResult;
use k8_metadata_client::PatchMergeType;
use k8_metadata_client::NameSpace;
use k8_metadata_client::ListArg;
use k8_metadata_client::MetadataClient;
use k8_types::ObjectMeta;

use k8_types::options::DeleteOptions;
use k8_types::UpdateK8ObjStatus;
use k8_types::DeleteStatus;
use k8_types::K8List;
use k8_types::InputK8Obj;
use k8_types::K8Meta;
use k8_types::K8Obj;

#[derive(Debug)]
pub struct SpecStore {
    data: Arc<RwLock<HashMap<String, Value>>>,
    sender: Arc<Sender<Value>>,
    receiver: Receiver<Value>,
}

impl Default for SpecStore {
    fn default() -> Self {
        let (sender, receiver) = bounded(100);

        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
            sender: Arc::new(sender),
            receiver,
        }
    }
}

impl SpecStore {
    async fn get<S>(&self, key: &str) -> Result<Option<S>, MemoryClientError>
    where
        S: DeserializeOwned,
    {
        let lock = self.data.read().await;
        let Some(value) = lock.get(key) else {
            return Ok(None);
        };

        let output = value.clone();
        drop(lock);

        Ok(serde_yaml::from_value(output)?)
    }

    async fn insert<S>(&self, key: String, mut k8_obj: K8Obj<S>) -> Result<(), MemoryClientError>
    where
        S: Serialize + Spec + Clone + std::fmt::Debug,
    {
        let mut lock = self.data.write().await;

        let maybe_old = lock.get(&key);

        let watch: K8Watch<S> = if let Some(old_version) = maybe_old {
            let old_k8_obj: K8Obj<S> = serde_yaml::from_value(old_version.clone())?;
            let old_resource_version = old_k8_obj
                .metadata
                .resource_version
                .parse::<i32>()
                .unwrap_or_default();
            k8_obj.metadata.resource_version = (old_resource_version + 1).to_string();

            K8Watch::MODIFIED(k8_obj.clone())
        } else {
            K8Watch::ADDED(k8_obj.clone())
        };

        let value = serde_yaml::to_value(k8_obj)?;

        lock.insert(key, value);

        drop(lock);

        let watch_value = serde_yaml::to_value(watch)?;

        let _ = self.sender.send(watch_value).await;

        Ok(())
    }

    async fn items<S>(&self) -> Result<Vec<K8Obj<S>>, MemoryClientError>
    where
        S: Spec,
    {
        let lock = self.data.read().await;
        let items: Result<Vec<K8Obj<S>>, _> = lock
            .values()
            .map(|value| serde_yaml::from_value(value.clone()))
            .collect();

        Ok(items?)
    }

    async fn remove<S>(&self, key: &str) -> Result<Option<Value>, MemoryClientError>
    where
        S: Spec,
    {
        let mut lock = self.data.write().await;
        let Some(value) = lock.remove(key) else {
            return Ok(None);
        };

        drop(lock);

        let k8_obj: K8Obj<S> = serde_yaml::from_value(value.clone())?;

        let watch: K8Watch<S> = K8Watch::DELETED(k8_obj);

        let watch_value = serde_yaml::to_value(watch)?;

        let _ = self.sender.send(watch_value).await;

        Ok(Some(value))
    }

    fn watch_stream<S>(&self) -> BoxStream<'static, TokenStreamResult<S, MemoryClientError>>
    where
        S: Spec + 'static,
        S::Status: 'static,
        S::Header: 'static,
    {
        self.receiver
            .clone()
            .map(|f| {
                Ok(vec![
                    serde_yaml::from_value::<K8Watch<S>>(f.clone()).map_err(|err| err.into())
                ])
            })
            .boxed()
    }
}

#[derive(Debug, Default)]
pub struct MemoryClient {
    data: Mutex<HashMap<String, Arc<SpecStore>>>,
}

impl MemoryClient {
    pub fn new_shared() -> Arc<Self> {
        Arc::new(Self::default())
    }

    async fn get_store<S: Spec>(&self) -> Arc<SpecStore> {
        let kind: String = S::kind();
        let mut stores = self.data.lock().await;
        if let Some(store) = stores.get(&kind) {
            store.clone()
        } else {
            let store = Arc::new(SpecStore::default());
            stores.insert(kind, store.clone());
            store
        }
    }

    pub async fn retrieve_items_inner<S: Spec>(&self) -> Result<K8List<S>, MemoryClientError> {
        let store = self.get_store::<S>().await;
        let items: Vec<K8Obj<S>> = store.items().await?;
        Ok(K8List {
            api_version: S::api_version(),
            kind: S::kind(),
            metadata: ListMetadata {
                _continue: None,
                resource_version: "0".to_owned(),
            },
            items,
        })
    }
}

#[async_trait::async_trait]
impl MetadataClient for MemoryClient {
    type MetadataClientError = MemoryClientError;

    /// retrieval a single item
    async fn retrieve_item<S, M>(&self, metadata: &M) -> Result<K8Obj<S>, Self::MetadataClientError>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        let store = self.get_store::<S>().await;

        let name: String = metadata.name().to_owned();
        let Ok(Some(data)) = store.get::<K8Obj<S>>(&name).await else {
            return Err(MemoryClientError::NotFound);
        };

        Ok(data)
    }

    async fn retrieve_items_with_option<S, N>(
        &self,
        _namespace: N,
        _option: Option<ListArg>,
    ) -> Result<K8List<S>, Self::MetadataClientError>
    where
        S: Spec,
        N: Into<NameSpace> + Send + Sync,
    {
        self.retrieve_items_inner().await
    }

    fn retrieve_items_in_chunks<'a, S, N>(
        self: Arc<Self>,
        _namespace: N,
        _limit: u32,
        _option: Option<ListArg>,
    ) -> BoxStream<'a, K8List<S>>
    where
        S: Spec + 'static,
        N: Into<NameSpace> + Send + Sync + 'static,
    {
        futures_util::stream::pending().boxed()
    }

    async fn delete_item_with_option<S, M>(
        &self,
        metadata: &M,
        _option: Option<DeleteOptions>,
    ) -> Result<DeleteStatus<S>, Self::MetadataClientError>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        let store = self.get_store::<S>().await;
        let key = metadata.name();

        store.remove::<S>(key).await?;

        Ok(DeleteStatus::Deleted(MetaStatus {
            api_version: S::api_version(),
            code: None,
            details: None,
            kind: S::kind(),
            reason: None,
            status: k8_types::StatusEnum::SUCCESS,
            message: None,
        }))
    }

    /// create new object
    async fn create_item<S>(
        &self,
        value: InputK8Obj<S>,
    ) -> Result<K8Obj<S>, Self::MetadataClientError>
    where
        S: Spec,
    {
        let store = self.get_store::<S>().await;
        let key = value.metadata.name.clone();

        let mut k8_obj: K8Obj<S> = K8Obj::new(key.clone(), value.spec);

        let metadata = value.metadata;

        k8_obj.metadata = ObjectMeta {
            name: metadata.name,
            owner_references: metadata.owner_references,
            labels: metadata.labels,
            namespace: metadata.namespace,
            annotations: metadata.annotations,
            finalizers: metadata.finalizers,
            ..Default::default()
        };

        store.insert(key, k8_obj.clone()).await?;

        Ok(k8_obj)
    }

    /// update status
    async fn update_status<S>(
        &self,
        value: &UpdateK8ObjStatus<S>,
    ) -> Result<K8Obj<S>, Self::MetadataClientError>
    where
        S: Spec,
    {
        let store = self.get_store::<S>().await;

        let key = value.metadata.name.clone();
        debug!(key,?value.status,"start updating status");

        let k8_value: Option<K8Obj<S>> = store.get(&key).await?;
        let k8_value = k8_value.ok_or(MemoryClientError::NotFound)?;

        let k8_obj = k8_value.set_status(value.status.clone());
        debug!(key,?value.status,"overwrite set");

        store.insert(key, k8_obj.clone()).await?;

        debug!("done");

        Ok(k8_obj)
    }

    /// patch existing with spec
    async fn patch<S, M>(
        &self,
        _metadata: &M,
        _patch: &serde_json::Value,
        _merge_type: PatchMergeType,
    ) -> Result<K8Obj<S>, Self::MetadataClientError>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        // In SC, this api is not call
        unreachable!()
    }

    /// patch status
    async fn patch_status<S, M>(
        &self,
        _metadata: &M,
        _patch: &serde_json::Value,
        _merge_type: PatchMergeType,
    ) -> Result<K8Obj<S>, Self::MetadataClientError>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        // In SC, this api is not call
        unreachable!()
    }

    /// stream items since resource versions
    fn watch_stream_since<S, N>(
        &self,
        _namespace: N,
        _resource_version: Option<String>,
    ) -> BoxStream<'_, TokenStreamResult<S, Self::MetadataClientError>>
    where
        S: Spec + 'static,
        S::Status: 'static,
        S::Header: 'static,
        N: Into<NameSpace>,
    {
        let ft_stream = async move {
            let kind: String = S::kind();

            let mut lock = self.data.lock().await;
            let store = lock.entry(kind).or_default();
            let st2 = store.clone();
            drop(lock);
            st2.watch_stream()
        };

        ft_stream.flatten_stream().boxed()
    }
}

#[derive(Debug, thiserror::Error)]
pub enum MemoryClientError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Yaml serialization error: {0}")]
    SerdeYaml(#[from] serde_yaml::Error),
    #[error("Diff error")]
    Diff(#[from] DiffError),
    #[error("Json serialization error: {0}")]
    SerdeJson(#[from] serde_json::Error),
    #[error("Not found")]
    NotFound,
    #[error("Patch")]
    Patch,
}

impl MetadataClientError for MemoryClientError {
    fn patch_error() -> Self {
        Self::Patch
    }

    fn not_found(&self) -> bool {
        matches!(self, Self::NotFound)
    }
}

#[cfg(test)]
mod test {
    use k8_metadata_client::MetadataClient;
    use k8_types::{Spec, DefaultHeader, Crd, CrdNames, Status, K8Obj, K8Watch};
    use serde::{Serialize, Deserialize};

    use super::MemoryClient;

    #[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
    pub struct MySpec {
        value: i32,
    }

    #[derive(Serialize, Deserialize, Default, Debug, Eq, PartialEq, Clone)]
    pub struct MySpecStatus {
        value: i32,
    }

    impl Status for MySpecStatus {}

    impl Spec for MySpec {
        type Status = MySpecStatus;
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
    #[fluvio_future::test]
    async fn test_metadata_client_impl() {
        use futures_util::StreamExt;
        let client = MemoryClient::new_shared();

        let mut stream = client.watch_stream_since::<MySpec, String>("".into(), None);

        // test create
        let my_spec = MySpec { value: 10 };
        let value = K8Obj::new("test".to_owned(), my_spec).as_input();
        client.create_item(value).await.expect("failed to create");

        let next_value = stream.next().await;

        let values_diffs = next_value.expect("value added");
        let mut values_diffs = values_diffs.expect("unexpected error");
        assert_eq!(values_diffs.len(), 1, "there must be only one value added");

        let value = values_diffs
            .pop()
            .expect("expected value")
            .expect("expected success");

        let K8Watch::ADDED(k8_obj) = value else {
            panic!("expected added");
        };
        assert_eq!(k8_obj.spec.value, 10);
        assert_eq!(k8_obj.status.value, 0);

        // test update
        let my_spec = MySpec { value: 15 };
        let value = K8Obj::new("test".to_owned(), my_spec).as_input();
        client.create_item(value).await.expect("failed to create");

        let next_value = stream.next().await;
        let values_diffs = next_value.expect("value added");
        let mut values_diffs = values_diffs.expect("unexpected error");
        assert_eq!(values_diffs.len(), 1, "there must be only one value diff");

        let value = values_diffs
            .pop()
            .expect("expected value")
            .expect("expected success");

        let K8Watch::MODIFIED(k8_obj) = value else {
            panic!("expected modified");
        };
        assert_eq!(k8_obj.spec.value, 15);
        assert_eq!(k8_obj.status.value, 0);

        // test update status
        let my_status = MySpecStatus { value: 20 };
        let my_spec = MySpec { value: 15 };
        let value = K8Obj::new("test".to_owned(), my_spec).as_status_update(my_status);
        client
            .update_status(&value)
            .await
            .expect("failed to update status");
        let next_value = stream.next().await;
        let values_diffs = next_value.expect("value added");
        let mut values_diffs = values_diffs.expect("unexpected error");
        assert_eq!(values_diffs.len(), 1, "there must be only one value diff");

        let value = values_diffs
            .pop()
            .expect("expected value")
            .expect("expected success");

        let K8Watch::MODIFIED(k8_obj) = value else {
            panic!("expected modified");
        };
        assert_eq!(k8_obj.spec.value, 15);
        assert_eq!(k8_obj.status.value, 20);

        // test delete
        let meta = k8_obj.metadata.clone();
        client
            .delete_item_with_option::<MySpec, _>(&meta, None)
            .await
            .expect("failed to delete");
        let next_value = stream.next().await;
        let values_diffs = next_value.expect("value added");
        let mut values_diffs = values_diffs.expect("unexpected error");
        assert_eq!(values_diffs.len(), 1, "there must be only one value added");

        let value = values_diffs
            .pop()
            .expect("expected value")
            .expect("expected success");

        let K8Watch::DELETED(_) = value else {
            panic!("expected deleted");
        };
    }
}
