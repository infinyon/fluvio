use std::{
    fmt::Display,
    sync::Arc,
    any::Any,
    collections::{HashMap, hash_map::Entry},
    path::{PathBuf, Path},
};

use anyhow::Result;
use async_channel::{Sender, Receiver, bounded};
use async_lock::RwLock;
use fluvio_controlplane_metadata::{
    topic::TopicSpec, partition::PartitionSpec, spu::SpuSpec, smartmodule::SmartModuleSpec,
    tableformat::TableFormatSpec, spg::K8SpuGroupSpec,
};
use futures_util::{stream::BoxStream, StreamExt, FutureExt};
use k8_metadata_client::{
    MetadataClient, NameSpace, ListArg, PatchMergeType, TokenStreamResult, ObjectKeyNotFound,
};
use k8_types::{
    K8Obj, Spec, K8Meta, K8List, DeleteStatus, options::DeleteOptions, InputK8Obj,
    UpdateK8ObjStatus, K8Watch, ListMetadata,
};
use tracing::{debug, info, warn};

#[derive(Debug)]
pub(crate) struct LocalMetadataStorage {
    base_path: PathBuf,
    stores: HashMap<String, SpecStore>,
}

impl LocalMetadataStorage {
    pub fn load_from<P: AsRef<Path>>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().to_path_buf();
        let mut stores = HashMap::with_capacity(6);
        stores.insert(
            TopicSpec::kind(),
            SpecStore::load_from::<_, TopicSpec>(&base_path)?,
        );
        stores.insert(
            PartitionSpec::kind(),
            SpecStore::load_from::<_, PartitionSpec>(&base_path)?,
        );
        stores.insert(
            SpuSpec::kind(),
            SpecStore::load_from::<_, SpuSpec>(&base_path)?,
        );
        stores.insert(
            SmartModuleSpec::kind(),
            SpecStore::load_from::<_, SmartModuleSpec>(&base_path)?,
        );
        stores.insert(
            TableFormatSpec::kind(),
            SpecStore::load_from::<_, TableFormatSpec>(&base_path)?,
        );
        stores.insert(
            K8SpuGroupSpec::kind(),
            SpecStore::load_from::<_, K8SpuGroupSpec>(&base_path)?,
        );

        Ok(Self { base_path, stores })
    }

    fn get_store<S: Spec>(&self) -> Result<&SpecStore> {
        let kind: String = S::kind();
        self.stores
            .get(&kind)
            .ok_or_else(|| anyhow::anyhow!("store not found for {kind}"))
    }

    pub async fn retrieve_items_inner<S: Spec>(&self) -> Result<K8List<S>> {
        let store = self.get_store::<S>()?;
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

    #[allow(dead_code)]
    pub fn load(&mut self) -> Result<()> {
        info!("loading from {}", self.base_path.to_string_lossy());
        Ok(())
    }
}

#[derive(Debug)]
struct SpecStore {
    data: Arc<RwLock<HashMap<String, SpecPointer>>>,
    sender: Arc<Sender<WatchPointer>>,
    receiver: Receiver<WatchPointer>,
    base_path: PathBuf,
}

impl SpecStore {
    fn load_from<P: AsRef<Path>, S: Spec>(base_path: P) -> Result<Self> {
        let base_path = base_path.as_ref().join(S::kind());
        std::fs::create_dir_all(&base_path)?;
        let mut data = HashMap::new();
        for entry in std::fs::read_dir(&base_path)? {
            let Ok(entry) = entry else {
                continue;
            };
            let path = entry.path();
            match SpecPointer::try_load_from::<_, S>(path) {
                Ok((name, item)) => {
                    debug!("loaded {name} of {}", S::kind());
                    data.insert(name, item);
                }
                Err(err) => {
                    warn!("skipped spec file: {err}");
                }
            };
        }

        let (sender, receiver) = bounded(100);

        Ok(Self {
            data: Arc::new(RwLock::new(data)),
            sender: Arc::new(sender),
            receiver,
            base_path,
        })
    }

    async fn get<S: Spec>(&self, key: &str) -> Result<Option<K8Obj<S>>> {
        let lock = self.data.read().await;
        let Some(value) = lock.get(key) else {
            return Ok(None);
        };

        let output = value.get::<S>()?.clone();
        drop(lock);

        Ok(Some(output))
    }

    async fn insert<S>(&self, key: String, mut k8_obj: K8Obj<S>) -> anyhow::Result<()>
    where
        S: Spec + Clone + std::fmt::Debug + 'static,
    {
        let mut lock = self.data.write().await;

        let watch: K8Watch<S> = match lock.entry(key.clone()) {
            Entry::Occupied(mut entry) => {
                let old_version = entry.get_mut();
                let old_k8_obj = old_version.get::<S>()?;
                let old_resource_version = old_k8_obj
                    .metadata
                    .resource_version
                    .parse::<i32>()
                    .unwrap_or_default();
                k8_obj.metadata.resource_version = (old_resource_version + 1).to_string();
                old_version.set(k8_obj.clone());
                old_version.flush::<S>()?;
                K8Watch::MODIFIED(k8_obj)
            }
            Entry::Vacant(entry) => {
                let new_pointer = SpecPointer::new(k8_obj.clone(), self.base_path.join(key));
                new_pointer.flush::<S>()?;
                entry.insert(new_pointer);
                K8Watch::ADDED(k8_obj)
            }
        };

        drop(lock);

        let _ = self.sender.send(WatchPointer::new(watch)).await;

        Ok(())
    }

    async fn items<S>(&self) -> Result<Vec<K8Obj<S>>>
    where
        S: Spec,
    {
        let lock = self.data.read().await;
        let items: Result<Vec<K8Obj<S>>, _> =
            lock.values().map(|value| value.get_cloned()).collect();

        items
    }

    async fn remove<S>(&self, key: &str) -> Result<Option<K8Obj<S>>>
    where
        S: Spec,
    {
        let mut lock = self.data.write().await;
        let Some(value) = lock.remove(key) else {
            return Ok(None);
        };

        drop(lock);

        let k8_obj = value.delete::<S>()?;

        let watch: K8Watch<S> = K8Watch::DELETED(k8_obj.clone());

        let _ = self.sender.send(WatchPointer::new(watch)).await;

        Ok(Some(k8_obj))
    }

    fn watch_stream<S>(&self) -> BoxStream<'static, TokenStreamResult<S>>
    where
        S: Spec + 'static,
        S::Status: 'static,
        S::Header: 'static,
    {
        self.receiver
            .clone()
            .map(|f| Ok(vec![f.get_cloned::<S>()]))
            .boxed()
    }
}

#[derive(Debug)]
struct SpecPointer {
    inner: Box<dyn Any + Send + Sync>,
    path: PathBuf,
}

impl SpecPointer {
    fn new<S: Spec, P: AsRef<Path>>(inner: K8Obj<S>, path: P) -> Self {
        Self {
            inner: Box::new(inner),
            path: path.as_ref().to_path_buf(),
        }
    }

    fn try_load_from<P: AsRef<Path>, S: Spec>(path: P) -> Result<(String, Self)> {
        let name = path
            .as_ref()
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        let parsed: K8Obj<S> = serde_yaml::from_reader(std::fs::File::open(&path)?)?;
        Ok((name, Self::new(parsed, path)))
    }

    fn get<S: Spec>(&self) -> Result<&K8Obj<S>> {
        self.inner.downcast_ref::<K8Obj<S>>().ok_or_else(|| {
            anyhow::anyhow!(
                "incompatible type {} for spec kind {:?}",
                S::kind(),
                self.inner.type_id()
            )
        })
    }

    fn get_cloned<S: Spec>(&self) -> Result<K8Obj<S>> {
        self.get().map(Clone::clone)
    }

    fn set<S: Spec>(&mut self, obj: K8Obj<S>) {
        self.inner = Box::new(obj);
    }

    fn flush<S: Spec>(&self) -> Result<()> {
        let obj = self.get::<S>()?;

        serde_yaml::to_writer(std::fs::File::create(&self.path)?, obj)?;
        Ok(())
    }

    fn delete<S: Spec>(self) -> Result<K8Obj<S>> {
        let inner = self
            .inner
            .downcast::<K8Obj<S>>()
            .map_err(|_| anyhow::anyhow!("incompatible type {} for spec kind", S::kind()))?;
        std::fs::remove_file(self.path)?;
        Ok(*inner)
    }
}

#[derive(Debug)]
struct WatchPointer {
    inner: Box<dyn Any + Send>,
}

impl WatchPointer {
    fn new<S: Spec>(watch: K8Watch<S>) -> Self {
        Self {
            inner: Box::new(watch),
        }
    }

    fn get<S: Spec>(&self) -> Result<&K8Watch<S>> {
        self.inner.downcast_ref::<K8Watch<S>>().ok_or_else(|| {
            anyhow::anyhow!(
                "incompatible type {} for spec kind {:?}",
                S::kind(),
                self.inner.type_id()
            )
        })
    }

    fn get_cloned<S: Spec>(&self) -> Result<K8Watch<S>> {
        self.get().map(Clone::clone)
    }
}

#[async_trait::async_trait]
impl MetadataClient for LocalMetadataStorage {
    async fn retrieve_item<S, M>(&self, metadata: &M) -> Result<Option<K8Obj<S>>>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        let store = self.get_store::<S>()?;

        let name: String = metadata.name().to_owned();
        store.get::<S>(&name).await
    }

    async fn retrieve_items_with_option<S, N>(
        &self,
        _namespace: N,
        _option: Option<ListArg>,
    ) -> Result<K8List<S>>
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
    ) -> Result<DeleteStatus<S>>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        let store = self.get_store::<S>()?;
        let key = metadata.name();

        store.remove::<S>(key).await?;

        Ok(DeleteStatus::Deleted(k8_types::MetaStatus {
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
    async fn create_item<S>(&self, value: InputK8Obj<S>) -> Result<K8Obj<S>>
    where
        S: Spec,
    {
        let store = self.get_store::<S>()?;
        let key = value.metadata.name.clone();

        let mut k8_obj: K8Obj<S> = K8Obj::new(key.clone(), value.spec);

        let metadata = value.metadata;

        k8_obj.metadata = k8_types::ObjectMeta {
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
    async fn update_status<S>(&self, value: &UpdateK8ObjStatus<S>) -> Result<K8Obj<S>>
    where
        S: Spec,
    {
        let store = self.get_store::<S>()?;

        let key = value.metadata.name.clone();
        debug!(key,?value.status,"start updating status");

        let k8_value: Option<K8Obj<S>> = store.get(&key).await?;
        let k8_value = k8_value.ok_or(ObjectKeyNotFound::new(key.clone()))?;

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
    ) -> Result<K8Obj<S>>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        // TODO: implement or move to another trait
        unimplemented!()
    }

    /// patch status
    async fn patch_status<S, M>(
        &self,
        _metadata: &M,
        _patch: &serde_json::Value,
        _merge_type: PatchMergeType,
    ) -> Result<K8Obj<S>>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        // TODO: implement or move to another trait
        unimplemented!()
    }

    /// stream items since resource versions
    fn watch_stream_since<S, N>(
        &self,
        _namespace: N,
        _resource_version: Option<String>,
    ) -> BoxStream<'_, TokenStreamResult<S>>
    where
        S: Spec + 'static,
        S::Status: 'static,
        S::Header: 'static,
        N: Into<NameSpace>,
    {
        let ft_stream = async move {
            let kind: String = S::kind();

            match self.stores.get(&kind) {
                Some(store) => store.watch_stream(),
                None => futures_util::stream::empty().boxed(),
            }
        };

        ft_stream.flatten_stream().boxed()
    }
}
