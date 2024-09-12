use std::collections::{HashMap, hash_map::Entry};

use serde::{Serialize, Deserialize};

use fluvio_stream_model::core::MetadataItem;

#[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LocalMetadataItem {
    id: String,
    revision: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    parent: Option<Box<LocalMetadataItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    children: Option<HashMap<String, Vec<LocalMetadataItem>>>,
}

impl MetadataItem for LocalMetadataItem {
    type UId = String;

    fn uid(&self) -> &Self::UId {
        &self.id
    }

    fn is_newer(&self, another: &Self) -> bool {
        self.revision > another.revision
    }

    fn owner(&self) -> Option<&Self> {
        self.parent.as_ref().map(|p| p.as_ref())
    }

    fn set_owner(&mut self, owner: Self) {
        self.parent = Some(Box::new(owner));
    }

    fn children(&self) -> Option<&HashMap<String, Vec<Self>>> {
        self.children.as_ref()
    }

    fn set_children(&mut self, children: HashMap<String, Vec<Self>>) {
        self.children = Some(children);
    }
}

impl LocalMetadataItem {
    pub fn new<S: Into<String>>(id: S) -> Self {
        Self {
            id: id.into(),
            ..Default::default()
        }
    }

    pub fn with_parent<S: Into<String>>(id: S, parent: LocalMetadataItem) -> Self {
        Self {
            id: id.into(),
            parent: Some(Box::new(parent)),
            ..Default::default()
        }
    }

    pub fn with_revision(mut self, revision: u64) -> Self {
        self.revision = revision;
        self
    }

    pub fn put_child<S: Into<String>>(&mut self, kind: S, child: LocalMetadataItem) {
        let children = self.children.get_or_insert(Default::default());
        match children.entry(kind.into()) {
            Entry::Occupied(mut entry) => {
                let vec = entry.get_mut();
                if !vec.contains(&child) {
                    vec.push(child);
                }
            }
            Entry::Vacant(entry) => {
                entry.insert(vec![child]);
            }
        }
    }
    pub fn remove_child<S: Into<String>>(&mut self, kind: S, child: &LocalMetadataItem) {
        let children = self.children.get_or_insert(Default::default());
        match children.entry(kind.into()) {
            Entry::Occupied(mut entry) => {
                entry.get_mut().retain(|i| !i.eq(child));
                if entry.get().is_empty() {
                    entry.remove_entry();
                }
            }
            Entry::Vacant(_) => {}
        }
    }
}

cfg_if::cfg_if! {
    if #[cfg(feature = "k8")] {
        use std::{
            path::{Path, PathBuf},
            sync::{Arc, atomic::AtomicU64},
            any::Any,
            ffi::OsStr,
        };

        use anyhow::{Result, anyhow, Context};
        use async_channel::{Sender, Receiver, bounded};
        use parking_lot::RwLock;
        use futures_util::{stream::BoxStream, StreamExt};
        use serde::{de::DeserializeOwned};
        use tracing::{warn, debug, trace};

        use fluvio_stream_model::{
            core::{Spec, MetadataContext},
            store::{
                k8::K8ExtendedSpec, NameSpace, MetadataStoreList, MetadataStoreObject, actions::LSUpdate,
            },
        };

        use super::MetadataClient;

        const MAX_UPDATES_CAPACITY: usize = 100;
        #[derive(Debug)]
        pub struct LocalMetadataStorage {
            path: PathBuf,
            stores: RwLock<HashMap<&'static str, Arc<SpecStore>>>,
        }
        pub type LocalStoreObject<S> = MetadataStoreObject<S, LocalMetadataItem>;

        #[async_trait::async_trait]
        impl MetadataClient<LocalMetadataItem> for LocalMetadataStorage {
            async fn retrieve_items<S>(
                &self,
                _namespace: &NameSpace,
            ) -> Result<MetadataStoreList<S, LocalMetadataItem>>
            where
                S: K8ExtendedSpec,
            {
                let store = self.get_store::<S>()?;
                store.retrieve_items().await
            }

            async fn delete_item<S>(&self, metadata: LocalMetadataItem) -> Result<()>
            where
                S: K8ExtendedSpec,
            {
                trace!(?metadata, "delete item");
                let store = self.get_store::<S>()?;
                if let Some(item) = store.try_retrieve_item::<S>(&metadata).await? {
                    if let Some(owner) = item.ctx().item().owner() {
                        self.unlink_parent::<S>(owner, item.ctx().item()).await?;
                    }
                    self.delete_children(item).await?;
                    store.delete_item(&metadata).await
                };
                Ok(())
            }

            async fn finalize_delete_item<S>(&self, metadata: LocalMetadataItem) -> Result<()>
            where
                S: K8ExtendedSpec,
            {
                self.delete_item::<S>(metadata).await
            }

            async fn apply<S>(&self, mut value: LocalStoreObject<S>) -> Result<()>
            where
                S: K8ExtendedSpec,
                <S as Spec>::Owner: K8ExtendedSpec,
            {
                trace!(?value, "apply");
                let store = self.get_store::<S>()?;
                value.ctx_mut().item_mut().id = value.key().to_string();
                if let Some(owner) = value.ctx().item().owner() {
                    self.link_parent::<S>(owner, value.ctx().item()).await?;
                }
                store.apply(value).await
            }

            async fn update_spec<S>(&self, metadata: LocalMetadataItem, spec: S) -> Result<()>
            where
                S: K8ExtendedSpec,
            {
                use std::str::FromStr;

                trace!(?metadata, ?spec, "update spec");
                let store = self.get_store::<S>()?;
                let item = match store.try_retrieve_item::<S>(&metadata).await? {
                    Some(mut item) => {
                        item.ctx_mut().set_item(metadata);
                        item.set_spec(spec);
                        item
                    }
                    None => LocalStoreObject::new_with_context(
                        S::IndexKey::from_str(metadata.uid()).map_err(|_| {
                            anyhow!("failed to parse key from a string: {}", metadata.uid())
                        })?,
                        spec,
                        MetadataContext::new(metadata),
                    ),
                };
                store.apply(item).await?;
                Ok(())
            }

            async fn update_spec_by_key<S>(
                &self,
                key: S::IndexKey,
                _namespace: &NameSpace,
                spec: S,
            ) -> Result<()>
            where
                S: K8ExtendedSpec,
            {
                trace!(?key, ?spec, "update spec by key");
                let metadata = LocalMetadataItem {
                    id: key.to_string(),
                    ..Default::default()
                };
                let store = self.get_store::<S>()?;
                let item = match store.try_retrieve_item::<S>(&metadata).await? {
                    Some(mut item) => {
                        item.set_spec(spec);
                        item
                    }
                    None => LocalStoreObject::new_with_context(key, spec, MetadataContext::new(metadata)),
                };
                store.apply(item).await
            }

            async fn update_status<S>(
                &self,
                metadata: LocalMetadataItem,
                status: S::Status,
                _namespace: &NameSpace,
            ) -> Result<LocalStoreObject<S>>
            where
                S: K8ExtendedSpec,
            {
                trace!(?metadata, ?status, "update status");
                let store = self.get_store::<S>()?;
                let mut item = store.retrieve_item::<S>(&metadata).await?;
                item.ctx_mut().set_item(metadata.clone());
                item.set_status(status);
                store.apply(item).await?;
                store.retrieve_item::<S>(&metadata).await
            }

            fn watch_stream_since<S>(
                &self,
                _namespace: &NameSpace,
                resource_version: Option<String>,
            ) -> BoxStream<'_, Result<Vec<LSUpdate<S, LocalMetadataItem>>>>
            where
                S: K8ExtendedSpec,
            {
                trace!(label = S::LABEL, ?resource_version, "watch stream");
                let store = self.get_store::<S>();
                match store {
                    Ok(store) => store.watch_stream_since(resource_version.as_ref()),
                    Err(err) => futures_util::stream::once(async { Result::<_>::Err(err) }).boxed(),
                }
            }

            async fn patch_status<S>(
                &self,
                metadata: LocalMetadataItem,
                status: S::Status,
                _namespace: &NameSpace,
            ) -> Result<LocalStoreObject<S>>
            where
                S: K8ExtendedSpec,
            {
                trace!(?metadata, ?status, "patch status");
                let store = self.get_store::<S>()?;
                let mut item = store.retrieve_item::<S>(&metadata).await?;
                item.ctx_mut().set_item(metadata.clone());
                item.set_status(status);
                store.apply(item).await?;
                store.retrieve_item::<S>(&metadata).await
            }
        }

        #[derive(Debug)]
        struct SpecStore {
            version: AtomicU64,
            data: RwLock<HashMap<String, SpecPointer>>,
            sender: Sender<SpecUpdate>,
            receiver: Receiver<SpecUpdate>,
            path: PathBuf,
        }

        #[derive(Debug, Clone)]
        struct SpecPointer {
            inner: Arc<dyn Any + Send + Sync>,
            revision: u64,
            store_revision: u64,
            path: PathBuf,
        }

        #[derive(Debug)]
        enum SpecUpdate {
            Mod(SpecPointer),
            Delete(SpecPointer),
        }

        impl LocalMetadataStorage {
            pub fn new<P: AsRef<Path>>(path: P) -> Self {
                let path = path.as_ref().to_path_buf();
                let stores = Default::default();
                Self { path, stores }
            }

            fn get_store<S: Spec + DeserializeOwned>(&self) -> Result<Arc<SpecStore>> {
                let key = S::LABEL;
                let read = self.stores.read();
                Ok(match read.get(key) {
                    Some(store) => store.clone(),
                    None => {
                        drop(read);
                        let mut write = self.stores.write();
                        let store = Arc::new(SpecStore::load::<S, _>(self.path.join(key))?);
                        write.insert(key, store.clone());
                        drop(write);
                        store
                    }
                })
            }

            async fn delete_children<S: Spec>(&self, item: LocalStoreObject<S>) -> Result<()> {
                if let Some(all) = item.ctx().item().children() {
                    for (kind, children) in all {
                        let child_store = self.get_store_by_key(kind).await?;
                        for child in children {
                            trace!(?item, ?child, "delete child");
                            child_store.delete_item(child).await;
                        }
                    }
                }
                Ok(())
            }

            async fn link_parent<S: Spec>(
                &self,
                parent: &LocalMetadataItem,
                child: &LocalMetadataItem,
            ) -> Result<()> {
                trace!(?parent, ?child, "link parent");
                let parent_store = self.get_store::<S::Owner>()?;
                parent_store
                    .mut_in_place::<S::Owner, _>(parent.uid(), |parent_obj| {
                        parent_obj
                            .ctx_mut()
                            .item_mut()
                            .put_child(S::LABEL, child.clone());
                    })
                    .await?;
                Ok(())
            }

            async fn unlink_parent<S: Spec>(
                &self,
                parent: &LocalMetadataItem,
                child: &LocalMetadataItem,
            ) -> Result<()> {
                trace!(?parent, ?child, "link parent");
                let parent_store = self.get_store::<S::Owner>()?;
                parent_store
                    .mut_in_place::<S::Owner, _>(parent.uid(), |parent_obj| {
                        parent_obj
                            .ctx_mut()
                            .item_mut()
                            .remove_child(S::LABEL, child);
                    })
                    .await?;
                Ok(())
            }

            async fn get_store_by_key(&self, key: &str) -> Result<Arc<SpecStore>> {
                self.stores
                    .read()
                    .get(key)
                    .cloned()
                    .ok_or_else(|| anyhow!("store not found for key {key}"))
            }
        }

        impl SpecStore {
            fn load<S: Spec, P: AsRef<Path>>(path: P) -> Result<Self> {
                std::fs::create_dir_all(&path)?;
                let version = Default::default();
                let mut data: HashMap<String, SpecPointer> = Default::default();
                for entry in std::fs::read_dir(&path)? {
                    let Ok(entry) = entry else {
                        continue;
                    };
                    let path = entry.path();
                    if !path.extension().eq(&Some(OsStr::new("yaml"))) {
                        continue;
                    }
                    let (name, item) = SpecPointer::load::<S, _>(&path).context(format!(
                        "loading metadata '{}' from {}",
                        S::LABEL,
                        path.display()
                    ))?;
                    debug!(kind = S::LABEL, name, "loaded");
                    data.insert(name, item);
                }

                let (sender, receiver) = bounded(MAX_UPDATES_CAPACITY);
                let path = path.as_ref().to_path_buf();
                Ok(Self {
                    version,
                    data: RwLock::new(data),
                    sender,
                    receiver,
                    path,
                })
            }

            async fn retrieve_items<S>(&self) -> Result<MetadataStoreList<S, LocalMetadataItem>>
            where
                S: Spec,
            {
                let version = self
                    .version
                    .load(std::sync::atomic::Ordering::SeqCst)
                    .to_string();
                let read = self.data.read();
                let items: Vec<LocalStoreObject<S>> = read
                    .values()
                    .map(SpecPointer::downcast)
                    .collect::<Result<Vec<_>>>()?;

                Ok(MetadataStoreList { version, items })
            }

            async fn try_retrieve_item<S>(
                &self,
                metadata: &LocalMetadataItem,
            ) -> Result<Option<LocalStoreObject<S>>>
            where
                S: Spec,
            {
                let read = self.data.read();
                read.get(metadata.uid())
                    .map(SpecPointer::downcast)
                    .transpose()
            }

            async fn retrieve_item<S>(&self, metadata: &LocalMetadataItem) -> Result<LocalStoreObject<S>>
            where
                S: Spec,
            {
                self.try_retrieve_item::<S>(metadata)
                    .await?
                    .ok_or_else(|| anyhow!("'{}' not found", metadata.uid()))
            }

            async fn delete_item(&self, metadata: &LocalMetadataItem) {
                let removed = {
                    let mut write = self.data.write();
                    if let Some(removed) = write.remove(metadata.uid()) {
                        removed.delete();
                        drop(write);
                        Some(removed)
                    } else {
                        None
                    }
                };

                if let Some(removed) = removed {
                    self.send_update(SpecUpdate::Delete(removed)).await;
                }
            }

            async fn apply<S>(&self, mut value: LocalStoreObject<S>) -> Result<()>
            where
                S: Spec + Serialize,
            {
                let id = value.ctx().item().uid().to_owned();
                let pointer =
                {
                    let mut write = self.data.write();
                    if let Some(prev) = write.get(&id) {
                        let prev_meta = prev.downcast_ref::<S>()?.ctx().item();
                        let prev_rev = prev_meta.revision;
                        if prev_meta.is_newer(value.ctx().item()) {
                            let new_rev = value.ctx().item().revision;
                            anyhow::bail!("attempt to update by stale value: current version: {prev_rev}, proposed: {new_rev}");
                        }
                        value.ctx_mut().item_mut().revision = prev_rev + 1;
                    };
                    let pointer = SpecPointer::new(self.spec_file_name(&id), value);
                    write.insert(id, pointer.clone());
                    pointer.flush::<S>()?;
                    drop(write);
                    pointer
                };
                self.send_update(SpecUpdate::Mod(pointer)).await;
                Ok(())
            }

            fn watch_stream_since<'a, S>(
                &self,
                resources_version: Option<&String>,
            ) -> BoxStream<'a, Result<Vec<LSUpdate<S, LocalMetadataItem>>>>
            where
                S: Spec,
            {
                match resources_version.map(|rv| rv.parse::<u64>()) {
                    Some(Ok(version)) => self
                        .receiver
                        .clone()
                        .filter(move |update| {
                            let res = update.store_revision() >= version;
                            async move { res }
                        })
                        .map(|update| Ok(vec![update.into_ls_update()?]))
                        .boxed(),
                    Some(Err(err)) => {
                        futures_util::stream::once(async { Result::<_>::Err(err.into()) }).boxed()
                    }
                    None => self
                        .receiver
                        .clone()
                        .map(|update| Ok(vec![update.into_ls_update()?]))
                        .boxed(),
                }
            }

            fn spec_file_name(&self, name: &str) -> PathBuf {
                self.path.join(format!("{name}.yaml"))
            }

            async fn mut_in_place<S: Spec, F>(&self, key: &str, func: F) -> Result<()>
            where
                F: Fn(&mut LocalStoreObject<S>),
            {
                if let Some(spec) = self.data.write().get_mut(key) {
                    let mut obj = spec.downcast::<S>()?;
                    func(&mut obj);
                    spec.set(obj);
                    spec.flush::<S>()?;
                    Ok(())
                } else {
                    anyhow::bail!("'{key}' not found");
                }
            }

            async fn send_update(&self, mut update: SpecUpdate) {
                let store_revision = self
                    .version
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                update.set_store_revision(store_revision);
                trace!(?update, "spec update sending");
                if let Err(err) = self.sender.send(update).await {
                    warn!("store sender failed: {err}");
                }
            }
        }

        impl SpecPointer {
            fn new<S: Spec, P: AsRef<Path>>(path: P, obj: LocalStoreObject<S>) -> Self {
                let revision = obj.ctx().item().revision;
                let inner = Arc::new(obj);
                let path = path.as_ref().to_path_buf();
                let store_revision = Default::default();
                Self {
                    inner,
                    path,
                    revision,
                    store_revision,
                }
            }

            fn load<S: Spec, P: AsRef<Path>>(path: P) -> Result<(String, Self)> {
                let storage: VersionedSpecStorage<S> =
                    serde_yaml::from_reader(std::fs::File::open(&path)?)?;
                let name = storage.meta().uid().clone();
                let pointer = SpecPointer::try_from((storage, path.as_ref().to_path_buf()))?;
                Ok((name, pointer))
            }

            fn downcast_ref<S: Spec>(&self) -> Result<&LocalStoreObject<S>> {
                self.inner
                    .downcast_ref::<LocalStoreObject<S>>()
                    .ok_or_else(|| anyhow::anyhow!("incompatible type for spec kind {}", S::LABEL,))
            }

            fn downcast<S: Spec>(&self) -> Result<LocalStoreObject<S>> {
                self.downcast_ref().cloned()
            }

            fn delete(&self) {
                if let Err(err) = std::fs::remove_file(&self.path) {
                    warn!("unable to delete spec file {}: {err}", self.path.display());
                }
            }

            fn flush<S: Spec>(&self) -> Result<()> {
                let storage: VersionedSpecStorage<S> = self.try_into()?;
                serde_yaml::to_writer(std::fs::File::create(&self.path)?, &storage)?;
                Ok(())
            }

            fn set<S: Spec>(&mut self, obj: LocalStoreObject<S>) {
                self.revision = obj.ctx().item().revision;
                self.inner = Arc::new(obj);
            }
        }

        impl SpecUpdate {
            fn into_ls_update<S: Spec>(self) -> Result<LSUpdate<S, LocalMetadataItem>> {
                Ok(match self {
                    SpecUpdate::Mod(p) => LSUpdate::Mod(p.downcast()?),
                    SpecUpdate::Delete(p) => LSUpdate::Delete(p.downcast_ref::<S>()?.key_owned()),
                })
            }

            fn store_revision(&self) -> u64 {
                match self {
                    SpecUpdate::Mod(p) => p.store_revision,
                    SpecUpdate::Delete(p) => p.store_revision,
                }
            }

            fn set_store_revision(&mut self, store_revision: u64) {
                match self {
                    SpecUpdate::Mod(p) => p.store_revision = store_revision,
                    SpecUpdate::Delete(p) => p.store_revision = store_revision,
                }
            }
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        #[serde(bound(deserialize = "S: DeserializeOwned"))]
        enum VersionedSpecStorage<S>
        where
            S: Spec,
        {
            #[serde(rename = "1.0.0")]
            V1(SpecStorageV1<S>),
        }

        impl<S: Spec> VersionedSpecStorage<S> {
            fn meta(&self) -> &LocalMetadataItem {
                match self {
                    VersionedSpecStorage::V1(storage) => &storage.meta,
                }
            }
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        #[serde(bound(deserialize = "S: DeserializeOwned"))]
        struct SpecStorageV1<S>
        where
            S: Spec,
        {
            meta: LocalMetadataItem,
            key: String,
            status: S::Status,
            spec: S,
        }

        impl<S: Spec> TryFrom<&SpecPointer> for VersionedSpecStorage<S> {
            type Error = anyhow::Error;

            fn try_from(value: &SpecPointer) -> std::result::Result<Self, Self::Error> {
                let MetadataStoreObject {
                    spec,
                    status,
                    key,
                    ctx,
                } = value.downcast::<S>()?;

                let meta = ctx.into_inner();

                Ok(Self::V1(SpecStorageV1 {
                    meta,
                    key: key.to_string(),
                    status,
                    spec,
                }))
            }
        }

        impl<S> TryFrom<(VersionedSpecStorage<S>, PathBuf)> for SpecPointer
        where
            S: Spec,
        {
            type Error = anyhow::Error;

            fn try_from(
                (value, path): (VersionedSpecStorage<S>, std::path::PathBuf),
            ) -> std::result::Result<Self, Self::Error> {
                Ok(match value {
                    VersionedSpecStorage::V1(storage) => {
                        let SpecStorageV1 {
                            meta,
                            key,
                            status,
                            spec,
                        } = storage;
                        let ctx = MetadataContext::new(meta);
                        let key: S::IndexKey = key
                            .parse()
                            .map_err(|_| anyhow!("failed to parse key from '{key}'"))?;
                        let mut obj = LocalStoreObject::new_with_context(key, spec, ctx);
                        obj.set_status(status);
                        SpecPointer::new(path, obj)
                    }
                })
            }
        }

        #[cfg(test)]
        mod tests {
            use std::{
                time::Duration,
                ops::{AddAssign, SubAssign},
            };

            use crate::metadata::fixture::{
                TestSpec, TestStatus,
                parent::{ParentSpec, ParentStatus},
            };

            use super::*;

            #[fluvio_future::test]
            async fn test_spec_store_on_fs() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let kind = TestSpec::LABEL;
                let name = obj.ctx().item().uid().clone();

                //when
                meta_store.apply(obj).await.expect("applied");

                //then
                let spec_file_content =
                    std::fs::read_to_string(meta_folder.as_ref().join(kind).join(format!("{name}.yaml")))
                        .expect("content read");

                assert_eq!(
                    spec_file_content,
                    r#"!1.0.0
meta:
  id: meta
  revision: 0
key: meta
status: ''
spec:
  replica: 1
  replica_spec: !Computed
    count: 1
"#
                );

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_spec_store_loaded_from_fs() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj1 = default_test_store_obj();
                let obj2 = test_store_obj("meta2");
                meta_store.apply(obj1.clone()).await.expect("applied");
                meta_store.apply(obj2.clone()).await.expect("applied");
                drop(meta_store);

                //when
                let meta_store2 = LocalMetadataStorage::new(&meta_folder);
                let list = meta_store2
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("read items");

                //then
                assert_eq!(list.items.len(), 2);
                assert!(list.items.contains(&obj1));
                assert!(list.items.contains(&obj2));

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_spec_delete_from_fs() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let kind = TestSpec::LABEL;
                let name = obj.ctx().item().uid().clone();

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                let path = meta_folder.as_ref().join(kind).join(format!("{name}.yaml"));
                assert!(path.exists());
                meta_store
                    .delete_item::<TestSpec>(obj.ctx_owned().item_owned())
                    .await
                    .expect("deleted");

                //then
                assert!(!path.exists());

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_status() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx_owned().item_owned(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");

                let items = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("retrieved");

                //then
                assert_eq!(items.items.len(), 1);
                assert_eq!(items.version, "2");
                assert_eq!(items.items[0].status().to_string(), "new status");
                assert_eq!(items.items[0].ctx().item().revision, 1);

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_status_if_not_existed() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();

                //when
                let res = meta_store
                    .update_status::<TestSpec>(
                        obj.ctx_owned().item_owned(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await;

                //then
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().to_string(), "'meta' not found");

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_spec() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 5,
                    ..Default::default()
                };

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_spec(obj.ctx_owned().item_owned(), spec)
                    .await
                    .expect("updated status");

                let items = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("retrieved");

                //then
                assert_eq!(items.items.len(), 1);
                assert_eq!(items.version, "2");
                assert_eq!(items.items[0].spec().replica, 5);
                assert_eq!(items.items[0].ctx().item().revision, 1);

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_spec_upsert() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 5,
                    ..Default::default()
                };

                //when
                meta_store
                    .update_spec(obj.ctx_owned().item_owned(), spec)
                    .await
                    .expect("updated status");

                let items = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("retrieved");

                //then
                assert_eq!(items.items.len(), 1);
                assert_eq!(items.version, "1");
                assert_eq!(items.items[0].spec().replica, 5);
                assert_eq!(items.items[0].ctx().item().revision, 0);

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_spec_by_key() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 6,
                    ..Default::default()
                };

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_spec_by_key(obj.key_owned(), &NameSpace::All, spec)
                    .await
                    .expect("updated status");

                let items = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("retrieved");

                //then
                assert_eq!(items.items.len(), 1);
                assert_eq!(items.version, "2");
                assert_eq!(items.items[0].spec().replica, 6);
                assert_eq!(items.items[0].ctx().item().revision, 1);

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_update_spec_by_key_upsert() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 6,
                    ..Default::default()
                };

                //when
                meta_store
                    .update_spec_by_key(obj.key_owned(), &NameSpace::All, spec)
                    .await
                    .expect("updated status");

                let items = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("retrieved");

                //then
                assert_eq!(items.items.len(), 1);
                assert_eq!(items.version, "1");
                assert_eq!(items.items[0].spec().replica, 6);
                assert_eq!(items.items[0].ctx().item().revision, 0);

                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_watch_stream_since_start() {
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let stream = meta_store.watch_stream_since::<TestSpec>(&NameSpace::All, None);

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");
                meta_store
                    .delete_item::<TestSpec>(obj.ctx_owned().item_owned())
                    .await
                    .expect("deleted");

                let updates = stream
                    .take_until(fluvio_future::timer::sleep(Duration::from_secs(2)))
                    .collect::<Vec<Result<Vec<LSUpdate<TestSpec, LocalMetadataItem>>>>>()
                    .await;

                //then

                let updates: Vec<_> = updates.into_iter().flatten().flatten().collect();
                assert_eq!(updates.len(), 3);

                assert!(
                    matches!(updates.first(), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq(""))
                );
                assert!(
                    matches!(updates.get(1), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq("new status"))
                );
                assert!(matches!(updates.get(2), Some(LSUpdate::Delete(deleted)) if deleted.eq(&obj.key)));
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_stale_apply_not_allowed() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let mut obj = default_test_store_obj();
                obj.ctx_mut().item_mut().revision.add_assign(1);
                meta_store.apply(obj.clone()).await.expect("applied");

                //when
                obj.ctx_mut().item_mut().revision.sub_assign(1);
                let res = meta_store.apply(obj.clone()).await;

                //then
                assert!(res.is_err());
                assert_eq!(
                    res.unwrap_err().to_string(),
                    "attempt to update by stale value: current version: 1, proposed: 0"
                );
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_stale_update_status_not_allowed() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");

                //when
                let res = meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await;

                //then
                assert!(res.is_err());
                assert_eq!(
                    res.unwrap_err().to_string(),
                    "attempt to update by stale value: current version: 1, proposed: 0"
                );
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_stale_update_spec_not_allowed() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 5,
                    ..Default::default()
                };

                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_spec(obj.ctx_owned().item_owned(), spec.clone())
                    .await
                    .expect("updated status");

                //when
                let res = meta_store
                    .update_spec(obj.ctx_owned().item_owned(), spec)
                    .await;

                //then
                assert!(res.is_err());
                assert_eq!(
                    res.unwrap_err().to_string(),
                    "attempt to update by stale value: current version: 1, proposed: 0"
                );
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_stale_update_spec_by_key_overwrites() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let spec = TestSpec {
                    replica: 5,
                    ..Default::default()
                };

                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_spec_by_key(obj.key.clone(), &NameSpace::All, spec.clone())
                    .await
                    .expect("updated status");

                //when
                let res = meta_store
                    .update_spec_by_key(obj.key.clone(), &NameSpace::All, spec)
                    .await;

                //then
                assert!(res.is_ok());
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_watch_stream_since_version() {
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let mut obj = default_test_store_obj();
                let stream =
                    meta_store.watch_stream_since::<TestSpec>(&NameSpace::All, Some("4".to_string()));

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .delete_item::<TestSpec>(obj.ctx_owned().item_owned())
                    .await
                    .expect("deleted");
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");

                obj.ctx_mut().item_mut().revision.add_assign(1);
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status2".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");
                meta_store
                    .delete_item::<TestSpec>(obj.ctx_owned().item_owned())
                    .await
                    .expect("deleted");

                let updates = stream
                    .take_until(fluvio_future::timer::sleep(Duration::from_secs(2)))
                    .collect::<Vec<Result<Vec<LSUpdate<TestSpec, LocalMetadataItem>>>>>()
                    .await;

                //then

                let updates: Vec<_> = updates.into_iter().flatten().flatten().collect();
                assert_eq!(updates.len(), 2);

                assert!(
                    matches!(updates.first(), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq("new status2"))
                );
                assert!(matches!(updates.get(1), Some(LSUpdate::Delete(deleted)) if deleted.eq(&obj.key)));
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_two_watch_streams() {
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let obj = default_test_store_obj();
                let stream1 = meta_store.watch_stream_since::<TestSpec>(&NameSpace::All, None);
                let stream2 = meta_store.watch_stream_since::<TestSpec>(&NameSpace::All, None);

                //when
                meta_store.apply(obj.clone()).await.expect("applied");
                meta_store
                    .update_status::<TestSpec>(
                        obj.ctx().item().clone(),
                        TestStatus("new status".to_string()),
                        &NameSpace::All,
                    )
                    .await
                    .expect("updated status");
                meta_store
                    .delete_item::<TestSpec>(obj.ctx_owned().item_owned())
                    .await
                    .expect("deleted");

                let updates1 = stream1
                    .take_until(fluvio_future::timer::sleep(Duration::from_secs(2)))
                    .collect::<Vec<Result<Vec<LSUpdate<TestSpec, LocalMetadataItem>>>>>()
                    .await;

                let updates2 = stream2
                    .take_until(fluvio_future::timer::sleep(Duration::from_secs(2)))
                    .collect::<Vec<Result<Vec<LSUpdate<TestSpec, LocalMetadataItem>>>>>()
                    .await;

                //then

                let updates1: Vec<_> = updates1.into_iter().flatten().flatten().collect();
                assert_eq!(updates1.len(), 3);

                assert!(
                    matches!(updates1.first(), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq(""))
                );
                assert!(
                    matches!(updates1.get(1), Some(LSUpdate::Mod(obj)) if obj.status.to_string().eq("new status"))
                );
                assert!(matches!(updates1.get(2), Some(LSUpdate::Delete(deleted)) if deleted.eq(&obj.key)));

                let updates2: Vec<_> = updates2.into_iter().flatten().flatten().collect();
                assert_eq!(updates2.len(), 0);
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_cascade_children_deletion() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let (parent, children) = test_parent_with_children(2);
                meta_store
                    .apply(parent.clone())
                    .await
                    .expect("applied parent");
                for child in children {
                    meta_store.apply(child).await.expect("applied child");
                }
                //when
                let before = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("items");

                meta_store
                    .delete_item::<ParentSpec>(parent.ctx().item().clone())
                    .await
                    .expect("deleted parent");

                let after = meta_store
                    .retrieve_items::<TestSpec>(&NameSpace::All)
                    .await
                    .expect("items");

                //then
                assert_eq!(before.items.len(), 2);
                assert!(after.items.is_empty());
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_parent_linking() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let (mut parent, mut children) = test_parent_with_children(1);
                let child = children.remove(0);
                parent.ctx_mut().item_mut().set_children(Default::default());
                meta_store
                    .apply(parent.clone())
                    .await
                    .expect("applied parent");

                //when
                meta_store
                    .apply(child.clone())
                    .await
                    .expect("applied child");

                let parent_meta = meta_store
                    .retrieve_items::<ParentSpec>(&NameSpace::All)
                    .await
                    .expect("items")
                    .items
                    .remove(0)
                    .ctx_owned()
                    .into_inner();

                assert_eq!(parent_meta.children().unwrap().len(), 1);
                assert_eq!(
                    parent_meta
                        .children()
                        .unwrap()
                        .get(TestSpec::LABEL)
                        .expect("test spec children")
                        .len(),
                    1
                );

                assert!(parent_meta
                    .children()
                    .unwrap()
                    .get(TestSpec::LABEL)
                    .expect("test spec children")
                    .contains(child.ctx().item()),);

                meta_store
                    .delete_item::<TestSpec>(child.ctx().item().clone())
                    .await
                    .expect("deleted child");

                //then
                let parent_meta = meta_store
                    .retrieve_items::<ParentSpec>(&NameSpace::All)
                    .await
                    .expect("items")
                    .items
                    .remove(0)
                    .ctx_owned()
                    .into_inner();

                assert!(parent_meta.children().unwrap().is_empty());
                drop(meta_folder)
            }

            #[fluvio_future::test]
            async fn test_parent_is_not_existed() {
                //given
                let meta_folder = tempfile::tempdir().expect("temp dir created");
                let meta_store = LocalMetadataStorage::new(&meta_folder);
                let (_, mut children) = test_parent_with_children(1);
                let child = children.remove(0);

                //when
                let res = meta_store.apply(child).await;

                //then
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().to_string(), "'parent' not found");
                drop(meta_folder)
            }

            #[test]
            fn test_ser() {
                //given
                let spec = TestSpec {
                    replica: 1,
                    ..Default::default()
                };
                let meta = meta_with_parent();

                let spec_storage = SpecStorageV1 {
                    key: "key1".to_string(),
                    status: TestStatus("status1".to_string()),
                    spec,
                    meta,
                };

                //when
                let str =
                    serde_yaml::to_string(&VersionedSpecStorage::V1(spec_storage)).expect("serialized");

                //then
                assert_eq!(
                    str,
                    r#"!1.0.0
meta:
  id: meta1
  revision: 1
  parent:
    id: parent1
    revision: 2
key: key1
status: status1
spec:
  replica: 1
  replica_spec: !Computed
    count: 1
"#
                );
            }

            #[test]
            fn test_deser() {
                //given
                let input = r#"!1.0.0
meta:
  id: meta
  revision: 2
  parent:
    id: parent1
    revision: 0
key: key1
status: status3
spec:
  replica: 2
  replica_spec: !Computed
    count: 1
"#;
                //when
                let parsed: VersionedSpecStorage<TestSpec> =
                    serde_yaml::from_str(input).expect("deserialized");

                //then
                assert_eq!(
                    parsed,
                    VersionedSpecStorage::V1(SpecStorageV1 {
                        meta: LocalMetadataItem::with_parent("meta", LocalMetadataItem::new("parent1"))
                            .with_revision(2),
                        key: "key1".to_string(),
                        status: TestStatus("status3".to_string()),
                        spec: TestSpec {
                            replica: 2,
                            ..Default::default()
                        },
                    })
                )
            }

            #[test]
            fn test_serde_parent() {
                //given
                let spec = ParentSpec { replica: 1 };
                let mut meta = LocalMetadataItem::new("parent").with_revision(1);
                let child1 = LocalMetadataItem::with_parent("child1", meta.clone()).with_revision(1);
                let child2 = LocalMetadataItem::with_parent("child2", meta.clone()).with_revision(2);

                let children = [(TestSpec::LABEL.to_owned(), vec![child1, child2])].into();
                meta.set_children(children);

                let spec_storage = VersionedSpecStorage::V1(SpecStorageV1 {
                    key: "key1".to_string(),
                    status: ParentStatus("status1".to_string()),
                    spec,
                    meta,
                });

                //when
                let str = serde_yaml::to_string(&spec_storage).expect("serialized");

                //then
                assert_eq!(
                    str,
                    r#"!1.0.0
meta:
  id: parent
  revision: 1
  children:
    TEST_SPEC:
    - id: child1
      revision: 1
      parent:
        id: parent
        revision: 1
    - id: child2
      revision: 2
      parent:
        id: parent
        revision: 1
key: key1
status: status1
spec:
  replica: 1
"#
                );

                let deser: VersionedSpecStorage<ParentSpec> =
                    serde_yaml::from_str(&str).expect("deserialized");
                assert_eq!(spec_storage, deser);
            }

            #[test]
            fn test_metadata_put_child() {
                //given
                let mut meta = LocalMetadataItem::new("parent1");

                //when
                meta.put_child("kind1", LocalMetadataItem::new("child1"));
                meta.put_child("kind1", LocalMetadataItem::new("child1"));
                meta.put_child("kind1", LocalMetadataItem::new("child2"));
                meta.put_child("kind2", LocalMetadataItem::new("child1"));

                //then
                assert!(meta.children().is_some());
                let mut chidlren = meta.children.take().unwrap();
                assert_eq!(chidlren.len(), 2);

                let kind1 = chidlren.remove("kind1").unwrap();
                assert_eq!(kind1.len(), 2);
                assert!(kind1.contains(&LocalMetadataItem::new("child1")));
                assert!(kind1.contains(&LocalMetadataItem::new("child2")));

                let kind2 = chidlren.remove("kind2").unwrap();
                assert_eq!(kind2.len(), 1);
                assert!(kind2.contains(&LocalMetadataItem::new("child1")));
            }

            #[test]
            fn test_metadata_remove_child() {
                //given
                let mut meta = LocalMetadataItem::new("parent1");
                meta.put_child("kind1", LocalMetadataItem::new("child1"));
                meta.put_child("kind1", LocalMetadataItem::new("child2"));
                meta.put_child("kind2", LocalMetadataItem::new("child1"));

                //when
                meta.remove_child("kind1", &LocalMetadataItem::new("child1"));
                meta.remove_child("kind2", &LocalMetadataItem::new("child1"));
                meta.remove_child("kind3", &LocalMetadataItem::new("child1"));

                //then
                assert!(meta.children().is_some());
                let mut chidlren = meta.children.take().unwrap();
                assert_eq!(chidlren.len(), 1);

                let kind1 = chidlren.remove("kind1").unwrap();
                assert_eq!(kind1.len(), 1);
                assert!(kind1.contains(&LocalMetadataItem::new("child2")));
            }

            fn default_test_store_obj() -> LocalStoreObject<TestSpec> {
                test_store_obj("meta")
            }

            fn test_store_obj(key: &str) -> LocalStoreObject<TestSpec> {
                let meta = LocalMetadataItem {
                    id: key.to_string(),
                    revision: 0,
                    ..Default::default()
                };
                let spec = TestSpec {
                    replica: 1,
                    ..Default::default()
                };
                LocalStoreObject::new_with_context(meta.uid().to_string(), spec, MetadataContext::new(meta))
            }

            fn meta_with_parent() -> LocalMetadataItem {
                let parent = LocalMetadataItem::new("parent1").with_revision(2);
                LocalMetadataItem::with_parent("meta1", parent).with_revision(1)
            }

            fn test_parent_with_children(
                children_count: usize,
            ) -> (
                LocalStoreObject<ParentSpec>,
                Vec<LocalStoreObject<TestSpec>>,
            ) {
                let mut parent_meta = LocalMetadataItem {
                    id: "parent".to_string(),
                    revision: 0,
                    ..Default::default()
                };
                let children_meta: Vec<LocalMetadataItem> = (0..children_count)
                    .map(|i| LocalMetadataItem {
                        id: format!("child{i}"),
                        revision: 1,
                        ..Default::default()
                    })
                    .collect();
                let parent_spec = ParentSpec { replica: 1 };
                parent_meta.set_children([(TestSpec::LABEL.to_owned(), children_meta.clone())].into());
                let parent_ctx = MetadataContext::new(parent_meta.clone());
                let parent_obj =
                    LocalStoreObject::new_with_context(parent_meta.uid().clone(), parent_spec, parent_ctx);

                let children_objs = children_meta
                    .into_iter()
                    .map(|mut meta| {
                        meta.set_owner(parent_meta.clone());
                        LocalStoreObject::new_with_context(
                            meta.uid().to_string(),
                            TestSpec::default(),
                            MetadataContext::new(meta),
                        )
                    })
                    .collect();
                (parent_obj, children_objs)
            }
        }
    }
}
