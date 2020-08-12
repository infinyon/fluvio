//!
//! # Update KV Store with SPU status (online/offline)
//!
use std::fmt::Display;
use std::convert::Into;
use std::marker::PhantomData;

use tracing::trace;
use tracing::debug;
use serde::de::DeserializeOwned;
use serde::Serialize;

use flv_types::log_on_err;
use k8_metadata_client::MetadataClient;
use k8_metadata_client::SharedClient;

use crate::k8::metadata::InputK8Obj;
use crate::core::Spec;
use crate::store::k8::K8ExtendedSpec;
use crate::store::k8::K8MetaItem;
use crate::k8::metadata::Spec as K8Spec;
use crate::k8::metadata::UpdateK8ObjStatus;

use crate::store::*;
use super::k8_actions::K8Action;

pub struct K8WSUpdateService<C, S> {
    client: SharedClient<C>,
    data: PhantomData<S>,
}

impl<C, S> K8WSUpdateService<C, S>
where
    C: MetadataClient,
    S: K8ExtendedSpec + Into<<S as K8ExtendedSpec>::K8Spec>,
    <S as Spec>::Owner: K8ExtendedSpec,
    S::Status: PartialEq + Display + Into<<<S as K8ExtendedSpec>::K8Spec as K8Spec>::Status>,
    S::IndexKey: Display,
    <S as K8ExtendedSpec>::K8Spec: DeserializeOwned + Serialize + Send + Sync,
{
    pub fn new(client: SharedClient<C>) -> Self {
        Self {
            client,
            data: PhantomData,
        }
    }

    /// add/update
    async fn apply(
        &self,
        value: MetadataStoreObject<S, K8MetaItem>,
    ) -> Result<(), C::MetadataClientError>
where {
        debug!("K8 Adding {}:{}", S::LABEL, value.key());
        trace!("adding KV {:#?} to k8 kv", value);

        let (key, spec, _status, ctx) = value.parts();
        let k8_spec: S::K8Spec = spec.into();

        if let Some(parent_metadata) = ctx.owner() {
            let item_name = key.to_string();

            let new_k8 = InputK8Obj::new(
                k8_spec,
                parent_metadata
                    .make_child_input_metadata::<<<S as Spec>::Owner as K8ExtendedSpec>::K8Spec>(
                        item_name,
                    ),
            );

            self.client.apply(new_k8).await.map(|_| ())
        } else {
            let new_k8 = InputK8Obj::new(k8_spec, ctx.item_owned().into());

            trace!("adding k8 {:#?} ", new_k8);

            self.client
                .apply(new_k8)
                .await
                .map(|_| ())
                .map_err(|err| err.into())
        }
    }

    /// only update the status
    async fn update_status(
        &self,
        metadata: K8MetaItem,
        status: S::Status,
    ) -> Result<(), C::MetadataClientError>
where {
        debug!(
            "K8 Update Status: {} key: {} value: {}",
            S::LABEL,
            metadata.name,
            status
        );
        trace!("status update: {:#?}", status);

        let k8_status: <<S as K8ExtendedSpec>::K8Spec as K8Spec>::Status = status.into();

        let k8_input: UpdateK8ObjStatus<S::K8Spec> = UpdateK8ObjStatus {
            api_version: S::K8Spec::api_version(),
            kind: S::K8Spec::kind(),
            metadata: metadata.into(),
            status: k8_status,
            ..Default::default()
        };

        self.client.update_status(&k8_input).await.map(|_| ())
    }

    /// update spec only
    async fn update_spec(
        &self,
        metadata: K8MetaItem,
        spec: S,
    ) -> Result<(), C::MetadataClientError>
where {
        debug!("K8 Update Spec: {} key: {}", S::LABEL, metadata.name);
        trace!("K8 Update Spec: {:#?}", spec);

        let k8_spec: <S as K8ExtendedSpec>::K8Spec = spec.into();

        trace!("updating spec: {:#?}", k8_spec);

        let k8_input: InputK8Obj<S::K8Spec> = InputK8Obj {
            api_version: S::K8Spec::api_version(),
            kind: S::K8Spec::kind(),
            metadata: metadata.into(),
            spec: k8_spec,
            ..Default::default()
        };

        self.client.apply(k8_input).await.map(|_| ())
    }

    async fn delete(&self, meta: K8MetaItem) -> Result<(), C::MetadataClientError> {
        self.client
            .delete_item::<S::K8Spec, _>(&meta)
            .await
            .map(|_| ())
    }

    pub async fn process(&self, action: K8Action<S>) {
        match action {
            K8Action::Apply(value) => log_on_err!(self.apply(value).await),
            K8Action::UpdateStatus((status, meta)) => {
                log_on_err!(self.update_status(meta, status).await)
            }
            K8Action::UpdateSpec((spec, meta)) => log_on_err!(self.update_spec(meta, spec).await),
            K8Action::Delete(meta) => log_on_err!(self.delete(meta).await),
        }
    }
}
