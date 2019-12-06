use std::fmt::Debug;
use std::fmt::Display;


use log::debug;
use log::trace;
use futures::stream::BoxStream;
use futures::stream::once;
use futures::future::ready;
use futures::future::FutureExt;
use futures::stream::StreamExt;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use serde_json::Value;



use k8_diff::Changes;
use k8_diff::Diff;
use k8_diff::DiffError;
use metadata_core::Spec;
use metadata_core::metadata::K8Meta;
use metadata_core::metadata::InputK8Obj;
use metadata_core::metadata::UpdateK8ObjStatus;
use metadata_core::metadata::K8List;
use metadata_core::metadata::K8Obj;
use metadata_core::metadata::K8Status;
use metadata_core::metadata::K8Watch;

use crate::DiffSpec;
use crate::ApplyResult;


/// trait for metadata client
pub trait MetadataClientError {

    /// is not founded
    fn not_founded(&self) -> bool;

    // create new patch error
    fn patch_error() -> Self;

}



// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html

pub type TokenStreamResult<S,P,E> = Result<Vec<Result<K8Watch<S,P>, E>>, E>;

pub fn as_token_stream_result<S,E>(events: Vec<K8Watch<S,S::Status>>) -> TokenStreamResult<S,S::Status,E> where S: Spec {

    Ok(events.into_iter().map(|event| Ok(event)).collect())
}



#[async_trait]
pub trait MetadataClient {

    type MetadataClientError: MetadataClientError + Send;
    
    /// retrieval a single item
    async fn retrieve_item<S,M>(
        &self,
        metadata: &M
    ) -> Result<K8Obj<S,S::Status>, Self::MetadataClientError>
        where
            K8Obj<S,S::Status>: DeserializeOwned,
            S: Spec,
            M: K8Meta<S> + Send + Sync;

    async fn retrieve_items<S>(
        &self,
        namespace: &str,
    ) -> Result<K8List<S,S::Status>, Self::MetadataClientError>
        where
            K8List<S,S::Status>: DeserializeOwned,
            S: Spec;

    async fn delete_item<S,M>(
        &self,
        metadata: &M,
    ) -> Result<K8Status, Self::MetadataClientError>
        where
            S: Spec,
            M: K8Meta<S> + Send + Sync;

    /// create new object
    async fn create_item<S>(
        &self,
        value: InputK8Obj<S>
    ) -> Result<K8Obj<S,S::Status>, Self::MetadataClientError>
        where
            InputK8Obj<S>: Serialize + Debug,
            K8Obj<S,S::Status>: DeserializeOwned,
            S: Spec + Send;

    /// apply object, this is similar to ```kubectl apply```
    /// for now, this doesn't do any optimization
    /// if object doesn't exist, it will be created
    /// if object exist, it will be patched by using strategic merge diff
    async fn apply<S>(
        &self,
        value: InputK8Obj<S>,
    ) -> Result<ApplyResult<S,S::Status>, Self::MetadataClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned + Debug,
        S: Spec + Serialize + Debug + Clone + Send ,
        S::Status: Send,
        Self::MetadataClientError: From<serde_json::Error> + From<DiffError> + Send
    {
        debug!("applying '{}' changes", value.metadata.name);
        trace!("applying {:#?}", value);
       
        match self.retrieve_item(&value.metadata).await {
            Ok(item) => {
                let mut old_spec = item.spec;
                old_spec.make_same(&value.spec);
                // we don't care about status
                let new_spec = serde_json::to_value(DiffSpec::from(value.spec.clone()))?;
                let old_spec = serde_json::to_value(DiffSpec::from(old_spec))?;
                let diff = old_spec.diff(&new_spec)?;
                match diff {
                    Diff::None => {
                        debug!("no diff detected, doing nothing");
                        Ok(ApplyResult::None)
                    }
                    Diff::Patch(p) => {
                        let json_diff = serde_json::to_value(p)?;
                        debug!("detected diff: old vs. new spec");
                        trace!("new spec: {:#?}", &new_spec);
                        trace!("old spec: {:#?}", &old_spec);
                        trace!("new/old diff: {:#?}", json_diff);
                        let patch_result = self.patch_spec(&value.metadata, &json_diff).await?;
                        Ok(ApplyResult::Patched(patch_result))
                    }
                    _ => Err(Self::MetadataClientError::patch_error()),
                }
            }
            Err(err) =>  {
                if err.not_founded() {
                    debug!("item '{}' not found, creating ...", value.metadata.name);
                    let created_item = self.create_item(value.into()).await?;
                    Ok(ApplyResult::Created(created_item))
                } else {
                    Err(err)
                }   
            },
        }
    }

    /// update status
    async fn update_status<S>(
        &self,
        value: &UpdateK8ObjStatus<S,S::Status>,
    ) -> Result<K8Obj<S,S::Status>, Self::MetadataClientError>
        where
            UpdateK8ObjStatus<S,S::Status>: Serialize + Debug,
            K8Obj<S,S::Status>: DeserializeOwned,
            S: Spec + Send + Sync,
            S::Status: Send + Sync;


    /// patch existing with spec
    async fn patch_spec<S,M>(
        &self,
        metadata: &M,
        patch: &Value,
    ) -> Result<K8Obj<S,S::Status>, Self::MetadataClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec + Debug,
        M: K8Meta<S> + Display + Send + Sync;

              
    
    
    /// stream items since resource versions
    fn watch_stream_since<S>(
        &self,
        namespace: &str,
        resource_version: Option<String>,
    ) -> BoxStream<'_,TokenStreamResult<S,S::Status,Self::MetadataClientError>>
    where
        K8Watch<S,S::Status>: DeserializeOwned,
        S: Spec + Debug + 'static,
        S::Status: Debug;
    
    
    fn watch_stream_now<S>(
        &self,
        ns: String,
    ) -> BoxStream<'_,TokenStreamResult<S,S::Status,Self::MetadataClientError>>
        where
            K8Watch<S,S::Status>: DeserializeOwned,
            K8List<S,S::Status>: DeserializeOwned,
            S: Spec + Debug + 'static + Send,
            S::Status: Debug + Send,
            Self: Sync
    {
                
        let ft_stream = async move {
            let namespace = ns.as_ref();
            let items_ft = self.retrieve_items(namespace);
            let item_now_result = items_ft.await;

            match item_now_result {
                Ok(item_now_list) => {
                    let resource_version = item_now_list.metadata.resource_version;

                    let items_watch_stream =
                        self.watch_stream_since(namespace, Some(resource_version));

                    let items_list = item_now_list
                        .items
                        .into_iter()
                        .map(|item| Ok(K8Watch::ADDED(item)))
                        .collect();
                    let list_stream = once(ready(Ok(items_list)));

                    list_stream.chain(items_watch_stream).left_stream()
                    // list_stream
                }
                Err(err) => once(ready(Err(err))).right_stream(),
            }
        };

        ft_stream.flatten_stream().boxed()
    }
    



    /// Check if the object exists, return true or false.
    async fn exists<S,M>(
        &self,
        metadata: &M,
    ) -> Result<bool, Self::MetadataClientError>
        where
            K8Obj<S,S::Status>: DeserializeOwned + Serialize + Debug + Clone,
            S: Spec + Serialize + Debug,
            M: K8Meta<S> + Display + Send + Sync
    {
        debug!("check if '{}' exists", metadata);
        
        match self.retrieve_item(metadata).await {
            Ok(_) => Ok(true),
            Err(err) => {
                if err.not_founded() {
                    Ok(false)
                } else {
                    Err(err)
                }
            }
        }
    }
}

