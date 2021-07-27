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

use k8_metadata_client::{MetadataClient, SharedClient};

use crate::k8_types::{InputK8Obj, K8Obj, Spec as K8Spec, UpdateK8ObjStatus};
use crate::core::{Spec, MetadataItem};
use crate::store::k8::{K8ExtendedSpec, K8MetaItem};

use crate::store::*;

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
    pub async fn apply(
        &self,
        value: MetadataStoreObject<S, K8MetaItem>,
    ) -> Result<(), C::MetadataClientError> {
        debug!("K8 Applying {}:{}", S::LABEL, value.key());
        trace!("adding KV {:#?} to k8 kv", value);

        let (key, spec, _status, ctx) = value.parts();
        let k8_spec: S::K8Spec = spec.into();

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
        input_metadata.owner_references = ctx.item().owner_references.clone();

        trace!("converted metadata: {:#?}", input_metadata);
        let new_k8 = InputK8Obj::new(k8_spec, input_metadata);

        debug!("input {:#?}", new_k8);

        self.client.apply(new_k8).await.map(|_| ())
    }

    /// only update the status
    pub async fn update_status(
        &self,
        metadata: K8MetaItem,
        status: S::Status,
    ) -> Result<K8Obj<S::K8Spec>, C::MetadataClientError> {
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
            metadata: metadata.inner().clone().into(),
            status: k8_status,
            ..Default::default()
        };

        self.client.update_status(&k8_input).await
    }

    /// update spec only
    pub async fn update_spec(
        &self,
        metadata: K8MetaItem,
        spec: S,
    ) -> Result<(), C::MetadataClientError> {
        debug!("K8 Update Spec: {} key: {}", S::LABEL, metadata.name);
        trace!("K8 Update Spec: {:#?}", spec);

        let k8_spec: <S as K8ExtendedSpec>::K8Spec = spec.into();

        trace!("updating spec: {:#?}", k8_spec);

        let k8_input: InputK8Obj<S::K8Spec> = InputK8Obj {
            api_version: S::K8Spec::api_version(),
            kind: S::K8Spec::kind(),
            metadata: metadata.inner().clone().into(),
            spec: k8_spec,
            ..Default::default()
        };

        self.client.apply(k8_input).await.map(|_| ())
    }

    pub async fn delete(&self, meta: K8MetaItem) -> Result<(), C::MetadataClientError> {
        use k8_metadata_client::k8_types::options::{DeleteOptions, PropogationPolicy};

        let options = if S::DELETE_WAIT_DEPENDENTS {
            Some(DeleteOptions {
                propagation_policy: Some(PropogationPolicy::Foreground),
                ..Default::default()
            })
        } else {
            None
        };

        debug!("deleting {:#?} with: {:#?}", meta, options);

        self.client
            .delete_item_with_option::<S::K8Spec, _>(meta.inner(), options)
            .await
            .map(|_| ())
    }

    pub async fn final_delete(&self, meta: K8MetaItem) -> Result<(), C::MetadataClientError> {
        use once_cell::sync::Lazy;
        use serde_json::Value;

        use k8_metadata_client::PatchMergeType::JsonMerge;

        // this may not work in non K8
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

        self.client
            .patch::<S::K8Spec, _>(&meta.inner().as_input(), &FINALIZER, JsonMerge)
            .await
            .map(|_| ())
    }
}
