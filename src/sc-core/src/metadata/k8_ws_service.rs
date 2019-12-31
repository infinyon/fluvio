//!
//! # Update KV Store with SPU status (online/offline)
//!
use std::fmt::Debug;
use std::fmt::Display;
use std::convert::Into;
use std::io::Error as IoError;
use std::io::ErrorKind;

use log::trace;
use log::warn;
use log::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;
use async_trait::async_trait;

use flv_metadata::topic::TopicSpec;
use flv_metadata::partition::PartitionSpec;
use flv_metadata::spu::SpuSpec;
use k8_metadata::core::metadata::InputK8Obj;

use types::log_on_err;

use k8_metadata::core::Spec as K8Spec;
use k8_metadata::core::metadata::UpdateK8ObjStatus;
use k8_metadata::client::MetadataClient;
use k8_metadata::client::SharedClient;

use crate::ScServerError;
use crate::core::Spec;
use crate::core::common::KVObject;
use crate::core::WSUpdateService;
use crate::core::common::WSAction;


pub struct K8WSUpdateService<C>(SharedClient<C>);

impl <C>Clone for K8WSUpdateService<C> {

    fn clone(&self) -> Self {
        Self(self.0.clone())
    }

}


impl<C> K8WSUpdateService<C>
where
    C: MetadataClient,
{
    pub fn new(client: SharedClient<C>) -> Self {
        Self(client)
    }

    pub fn client(&self) -> &C {
        &self.0
    }

    pub fn own_client(&self) -> SharedClient<C> {
        self.0.clone()
    }

    pub async fn add<S>(&self, value: KVObject<S>) -> Result<(), C::MetadataClientError>
    where
        S: Spec + Debug + Into<<S as Spec>::K8Spec>,
        S::Status: Debug + PartialEq,
        S::Key: Display + Debug,
        <S as Spec>::K8Spec: Debug + Default + DeserializeOwned + Serialize + Clone + Send,
        <<S as Spec>::K8Spec as K8Spec>::Status:
            Default + Debug + DeserializeOwned + Serialize + Clone + Send,
    {
        debug!("Adding: {}:{}", S::LABEL, value.key());
        trace!("adding KV {:#?} to k8 kv", value);

        let (key, spec, kv_ctx) = value.parts();
        let k8_spec: S::K8Spec = spec.into();
        if let Some(item_ctx) = kv_ctx.item_ctx {
            let new_k8 = InputK8Obj::new(k8_spec, item_ctx.into());

            self.0
                .apply(new_k8)
                .await
                .map(|_| ())
                .map_err(|err| err.into())
        } else if let Some(ref parent_metadata) = kv_ctx.parent_ctx {
            let item_name = key.to_string();

            let new_k8 = InputK8Obj::new(
                k8_spec,
                parent_metadata
                    .make_child_input_metadata::<<<S as Spec>::Owner as Spec>::K8Spec>(item_name),
            );

            self.0.apply(new_k8).await.map(|_| ())
        } else {
            Err(IoError::new(
                ErrorKind::Other,
                format!("{} add failed - no item or context {}", S::LABEL, key),
            )
            .into())
        }
    }

    /// only update the status
    async fn update_status<S>(&self, value: KVObject<S>) -> Result<(), C::MetadataClientError>
    where
        S: Spec + Debug,
        S::Key: Debug + Display,
        S::Status: Debug + Display + Into<<<S as Spec>::K8Spec as K8Spec>::Status>,
        <S as Spec>::K8Spec: Debug + Default + Serialize + DeserializeOwned + Send + Sync,
        <<S as Spec>::K8Spec as K8Spec>::Status:
            Default + Debug + Serialize + DeserializeOwned + Send + Sync,
    {
        debug!(
            "K8 Update Status: {} key: {} value: {}",
            S::LABEL,
            value.key(),
            value.status
        );
        trace!("status update: {:#?}", value.status);

        let k8_status: <<S as Spec>::K8Spec as K8Spec>::Status = value.status().clone().into();

        if let Some(ref kv_ctx) = value.kv_ctx().item_ctx {
            let k8_input: UpdateK8ObjStatus<S::K8Spec, <<S as Spec>::K8Spec as K8Spec>::Status> =
                UpdateK8ObjStatus {
                    api_version: S::K8Spec::api_version(),
                    kind: S::K8Spec::kind(),
                    metadata: kv_ctx.clone().into(),
                    status: k8_status,
                    ..Default::default()
                };

            self.0.update_status(&k8_input).await.map(|_| ())
        } else {
            Err(IoError::new(
                ErrorKind::Other,
                "KVS update failed - missing  KV ctx".to_owned(),
            )
            .into())
        }
    }

    /// update both spec and status
    pub async fn update_spec<S>(&self, value: KVObject<S>) -> Result<(), C::MetadataClientError>
    where
        S: Spec + Debug + Into<<S as Spec>::K8Spec>,
        S::Key: Debug + Display,
        S::Status: Debug + Into<<<S as Spec>::K8Spec as K8Spec>::Status>,
        <S as Spec>::K8Spec: Debug + Default + Serialize + DeserializeOwned + Clone + Send,
        <<S as Spec>::K8Spec as K8Spec>::Status:
            Default + Debug + Serialize + DeserializeOwned + Clone + Send,
    {
        debug!("K8 Update Spec: {} key: {}", S::LABEL, value.key());
        trace!("K8 Update Spec: {:#?}", value);
        let k8_spec: <S as Spec>::K8Spec = value.spec().clone().into();

        if let Some(ref kv_ctx) = value.kv_ctx().item_ctx {
            trace!("updating spec: {:#?}", k8_spec);

            let k8_input: InputK8Obj<S::K8Spec> = InputK8Obj {
                api_version: S::K8Spec::api_version(),
                kind: S::K8Spec::kind(),
                metadata: kv_ctx.clone().into(),
                spec: k8_spec,
                ..Default::default()
            };

            self.0.apply(k8_input).await.map(|_| ())
        } else {
            Err(IoError::new(
                ErrorKind::Other,
                "KVS update failed - missing  KV ctx".to_owned(),
            )
            .into())
        }
    }

    async fn inner_process<S>(&self, action: WSAction<S>) -> Result<(), ScServerError>
    where
        S: Spec + Debug + Into<<S as Spec>::K8Spec>,
        S::Key: Display + Debug,
        S::Status: Debug + PartialEq + Display,
        <S as Spec>::K8Spec: Clone + Debug + Default + Serialize + DeserializeOwned + Send + Sync,
        <<S as Spec>::K8Spec as K8Spec>::Status:
            From<S::Status> + Clone + Default + Debug + Serialize + DeserializeOwned + Send + Sync,
    {
        match action {
            WSAction::Add(value) => log_on_err!(self.add(value).await),
            WSAction::UpdateStatus(value) => log_on_err!(self.update_status(value).await),
            WSAction::UpdateSpec(value) => log_on_err!(self.update_spec(value).await),
            WSAction::Delete(_key) => warn!("delete not yet implemente"),
        }
        Ok(())
    }
}

#[async_trait]
impl<C> WSUpdateService for K8WSUpdateService<C>
where
    C: MetadataClient,
{
    async fn update_spu(&self, ws_actions: WSAction<SpuSpec>) -> Result<(), ScServerError> {
        let service = self.clone();
        service.inner_process(ws_actions).await?;
        Ok(())
    }

    async fn update_topic(&self, ws_actions: WSAction<TopicSpec>) -> Result<(), ScServerError> {
        let service = self.clone();
        service.inner_process(ws_actions).await?;
        Ok(())
    }

    async fn update_partition(
        &self,
        ws_actions: WSAction<PartitionSpec>,
    ) -> Result<(), ScServerError> {
        let service = self.clone();
        service.inner_process(ws_actions).await?;
        Ok(())
    }
}
