use std::{fmt::Display, sync::Arc};

use anyhow::Result;
use futures_util::stream::BoxStream;
use k8_client::memory::MemoryClient;
use k8_metadata_client::{MetadataClient, NameSpace, ListArg, PatchMergeType, TokenStreamResult};
use k8_types::{
    K8Obj, Spec, K8Meta, K8List, DeleteStatus, options::DeleteOptions, InputK8Obj,
    UpdateK8ObjStatus,
};
use tracing::info;

#[derive(Debug)]
pub(crate) struct LocalMetadataStorage {
    in_memory: MemoryClient,
}

impl LocalMetadataStorage {
    pub fn new() -> Self {
        Self {
            in_memory: MemoryClient::default(),
        }
    }

    async fn flush(&self) -> Result<()> {
        let file = std::fs::File::create("/tmp/dump.data")?;
        self.in_memory.serialize_to(file).await?;
        info!("flushed");
        Ok(())
    }
}

#[async_trait::async_trait]
impl MetadataClient for LocalMetadataStorage {
    async fn retrieve_item<S, M>(&self, metadata: &M) -> Result<Option<K8Obj<S>>>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        self.flush().await?;
        self.in_memory.retrieve_item(metadata).await
    }

    async fn retrieve_items_with_option<S, N>(
        &self,
        namespace: N,
        option: Option<ListArg>,
    ) -> Result<K8List<S>>
    where
        S: Spec,
        N: Into<NameSpace> + Send + Sync,
    {
        self.flush().await?;
        self.in_memory
            .retrieve_items_with_option(namespace, option)
            .await
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
        unimplemented!()
    }

    async fn delete_item_with_option<S, M>(
        &self,
        metadata: &M,
        option: Option<DeleteOptions>,
    ) -> Result<DeleteStatus<S>>
    where
        S: Spec,
        M: K8Meta + Send + Sync,
    {
        self.flush().await?;
        self.in_memory
            .delete_item_with_option(metadata, option)
            .await
    }

    async fn create_item<S>(&self, value: InputK8Obj<S>) -> Result<K8Obj<S>>
    where
        S: Spec,
    {
        self.flush().await?;
        self.in_memory.create_item(value).await
    }

    async fn update_status<S>(&self, value: &UpdateK8ObjStatus<S>) -> Result<K8Obj<S>>
    where
        S: Spec,
    {
        self.flush().await?;
        self.in_memory.update_status(value).await
    }

    async fn patch<S, M>(
        &self,
        metadata: &M,
        patch: &serde_json::Value,
        merge_type: PatchMergeType,
    ) -> Result<K8Obj<S>>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        self.flush().await?;
        self.in_memory.patch(metadata, patch, merge_type).await
    }

    async fn patch_status<S, M>(
        &self,
        metadata: &M,
        patch: &serde_json::Value,
        merge_type: PatchMergeType,
    ) -> Result<K8Obj<S>>
    where
        S: Spec,
        M: K8Meta + Display + Send + Sync,
    {
        self.flush().await?;
        self.in_memory
            .patch_status(metadata, patch, merge_type)
            .await
    }

    fn watch_stream_since<S, N>(
        &self,
        namespace: N,
        resource_version: Option<String>,
    ) -> BoxStream<'_, TokenStreamResult<S>>
    where
        S: Spec + 'static,
        S::Status: 'static,
        S::Header: 'static,
        N: Into<NameSpace>,
    {
        self.in_memory
            .watch_stream_since(namespace, resource_version)
    }
}
