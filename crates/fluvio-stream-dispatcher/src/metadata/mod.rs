#[cfg(feature = "k8")]
pub mod k8;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use fluvio_stream_model::{
    store::{
        MetadataStoreList, k8::K8ExtendedSpec, MetadataStoreObject, actions::LSUpdate, NameSpace,
    },
    core::{Spec, MetadataItem},
};

pub type SharedClient<C> = std::sync::Arc<C>;

#[async_trait]
pub trait MetadataClient<M: MetadataItem>: Send + Sync {
    async fn retrieve_items<S>(&self, namespace: &NameSpace) -> Result<MetadataStoreList<S, M>>
    where
        S: K8ExtendedSpec;

    async fn delete_item<S>(&self, metadata: M) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn finalize_delete_item<S>(&self, metadata: M) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn apply<S>(&self, value: MetadataStoreObject<S, M>) -> Result<()>
    where
        S: K8ExtendedSpec,
        <S as Spec>::Owner: K8ExtendedSpec;

    async fn update_spec<S>(&self, metadata: M, spec: S) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn update_spec_by_key<S>(
        &self,
        key: S::IndexKey,
        namespace: &NameSpace,
        spec: S,
    ) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn update_status<S>(
        &self,
        metadata: M,
        status: S::Status,
        namespace: &NameSpace,
    ) -> Result<MetadataStoreObject<S, M>>
    where
        S: K8ExtendedSpec;

    fn watch_stream_since<S>(
        &self,
        namespace: &NameSpace,
        resource_version: Option<String>,
    ) -> BoxStream<'_, Result<Vec<LSUpdate<S, M>>>>
    where
        S: K8ExtendedSpec;
}
