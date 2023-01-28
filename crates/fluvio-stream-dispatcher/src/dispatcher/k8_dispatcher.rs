use std::time::Duration;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io::Error as IoError;
use std::io::ErrorKind;

use futures_lite::stream::StreamExt;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::instrument;
use serde::de::DeserializeOwned;
use serde::Serialize;
use once_cell::sync::Lazy;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;

use k8_metadata_client::{MetadataClient, SharedClient, NameSpace};

use crate::k8_types::{K8List, K8Watch, Spec as K8Spec};

use crate::core::Spec;
use crate::store::k8::K8ExtendedSpec;
use crate::store::StoreContext;
use crate::actions::WSAction;

use convert::*;
use super::*;

static SC_RECONCILIATION_INTERVAL_SEC: Lazy<u64> = Lazy::new(|| {
    use std::env;

    let var_value = env::var("FLV_SC_RECONCILIATION_INTERVAL").unwrap_or_default();
    let wait_time: u64 = var_value.parse().unwrap_or(60 * 5);
    wait_time
});

/// For each spec, process updates from Kubernetes metadata
pub struct K8ClusterStateDispatcher<S, C>
where
    S: K8ExtendedSpec,
    <S as Spec>::Owner: K8ExtendedSpec,
    S::Status: PartialEq,
    S::IndexKey: Debug,
{
    client: SharedClient<C>,
    namespace: NameSpace,
    ctx: StoreContext<S>,
    ws_update_service: K8WSUpdateService<C, S>,
}

impl<S, C> Debug for K8ClusterStateDispatcher<S, C>
where
    S: K8ExtendedSpec,
    <S as Spec>::Owner: K8ExtendedSpec,
    S::Status: PartialEq,
    S::IndexKey: Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} K8StateDispatcher", S::LABEL)
    }
}

impl<S, C> K8ClusterStateDispatcher<S, C>
where
    S: K8ExtendedSpec + Sync + Send + 'static,
    <S as Spec>::Owner: K8ExtendedSpec,
    S::Status: Display + Sync + Send + 'static,
    S::Status: Into<<<S as K8ExtendedSpec>::K8Spec as K8Spec>::Status>,
    S::IndexKey: Display + Sync + Send + 'static,
    S: K8ExtendedSpec + Into<<S as K8ExtendedSpec>::K8Spec>,
    K8Watch<S::K8Spec>: DeserializeOwned,
    K8List<S::K8Spec>: DeserializeOwned,
    S::K8Spec: Sync + Send + 'static,
    <S as K8ExtendedSpec>::K8Spec: DeserializeOwned + Serialize + Send + Sync,
    C: MetadataClient + 'static,
    S::IndexKey: Display,
{
    /// start dispatcher
    pub fn start(namespace: impl Into<NameSpace>, client: SharedClient<C>, ctx: StoreContext<S>) {
        let ws_update_service = K8WSUpdateService::new(client.clone());
        let dispatcher = Self {
            namespace: namespace.into(),
            client,
            ctx,
            ws_update_service,
        };

        spawn(dispatcher.outer_loop());
    }

    #[instrument(
        name = "K8StateDispatcher",
        skip(self),
        fields(
            spec = S::LABEL,
            namespace = self.namespace.named(),
        )
    )]
    async fn outer_loop(mut self) {
        loop {
            debug!("starting rconcilation loop");
            if let Err(err) = self.reconcillation_loop().await {
                error!(
                    "error with reconcillation loop: {:#?}, sleep 10 seconds",
                    err
                );
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    ///
    /// Main Event Loop
    async fn reconcillation_loop(&mut self) -> Result<(), IoError> {
        use tokio::select;

        debug!("begin new reconcillation loop");

        let resume_stream = Some(self.retrieve_all_k8_items().await?);

        let client = self.client.clone();

        let mut reconcile_timer = sleep(Duration::from_secs(*SC_RECONCILIATION_INTERVAL_SEC));

        // create watch streams
        let mut k8_stream =
            client.watch_stream_since::<S::K8Spec, _>(self.namespace.clone(), resume_stream);

        loop {
            trace!("dispatcher waiting");
            let ws_receiver = self.ctx.receiver();

            select! {
                _ = &mut reconcile_timer => {
                    debug!("reconcillation timer fired - kickoff re-sync all");
                    break;
                },

                k8_result = k8_stream.next() =>  {

                    trace!("received K8 stream next");
                    if let Some(result) = k8_result {
                        match result {
                            Ok(auth_token_msgs) => {

                                if k8_watch_events_to_metadata_actions(
                                    Ok(auth_token_msgs),
                                    self.ctx.store(),
                                    self.multi_namespace_context()
                                ).await.is_none() {
                                    debug!( "no changes to applying changes to watch events");
                                }

                            }
                            Err(err) => error!("watch error {}", err),
                        }

                    } else {
                        debug!("k8 stream terminated, exiting event loop");
                        break;
                    }

                },

                msg = ws_receiver.recv() => {
                    match msg {
                        Ok(action) => {
                            debug!("store: received ws action: {action}");
                            self.process_ws_action(action).await;
                        },
                        Err(err) => {
                            error!("WS channel error: {err}");
                            panic!("WS channel error: {err}");
                        }
                    }
                }

            }
        }

        Ok(())
    }

    ///
    /// Retrieve all items from Kubernetes (K8) store for forward them to processing engine
    ///
    #[instrument(skip(self))]
    async fn retrieve_all_k8_items(&mut self) -> Result<String, IoError> {
        debug!("retrieving all k8 items");
        let k8_objects = self
            .client
            .retrieve_items::<S::K8Spec, _>(self.namespace.clone())
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("error retrieving k8: {err:?}"),
                )
            })?;

        let version = k8_objects.metadata.resource_version.clone();
        debug!(
            Spec = S::LABEL,
            version = &*version,
            item_count = k8_objects.items.len(),
            "Retrieving items",
        );
        // wait to receive all items before sending to channel
        k8_events_to_metadata_actions(k8_objects, self.ctx.store(), self.multi_namespace_context())
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("error converting k8: {err}"),
                )
            })?;
        Ok(version)
    }

    #[instrument(skip(self, action))]
    async fn process_ws_action(&mut self, action: WSAction<S>) {
        use crate::store::k8::K8MetaItem;

        match action {
            WSAction::Apply(obj) => {
                if let Err(err) = self.ws_update_service.apply(obj).await {
                    error!("error: {}, applying {}", S::LABEL, err);
                }
            }
            WSAction::UpdateSpec((key, spec)) => {
                let read_guard = self.ctx.store().read().await;
                let (spec, metadata) = if let Some(obj) = read_guard.get(&key) {
                    (spec, obj.inner().ctx().item().clone())
                } else {
                    // create new ctx
                    let meta = K8MetaItem::new(key.to_string(), self.namespace.named().to_owned());
                    (spec, meta)
                };
                if let Err(err) = self.ws_update_service.update_spec(metadata, spec).await {
                    error!("error: {:#?}, update spec {:#?}", S::LABEL, err);
                }
            }
            WSAction::UpdateStatus((key, status)) => {
                let read_guard = self.ctx.store().read().await;
                let meta = if let Some(obj) = read_guard.get(&key) {
                    obj.inner().ctx().item().clone()
                } else {
                    error!("update status: {} without existing item: {}", S::LABEL, key);
                    return;
                };
                drop(read_guard);
                debug!(
                    key = %key,
                    version = %meta.resource_version,
                    "update status begin",
                );
                match self
                    .ws_update_service
                    .update_status(meta, status.clone())
                    .await
                {
                    Ok(item) => {
                        //println!("updated status item: {:#?}", item);

                        use crate::store::actions::LSUpdate;

                        debug!(
                            name = %item.metadata.name,
                            version = %item.metadata.resource_version,
                            status = ?item.status,
                            "update status success"
                        );

                        match convert::k8_obj_to_kv_obj(item, self.multi_namespace_context()) {
                            Ok(updated_item) => {
                                let changes = vec![LSUpdate::Mod(updated_item)];

                                let _ = self.ctx.store().apply_changes(changes).await;
                            }
                            Err(err) => error!("{},error  converting back: {:#?}", S::LABEL, err),
                        }
                    }
                    Err(err) => {
                        error!(
                            "{}, update status err: {}, key: {}, status: {:#?}",
                            S::LABEL,
                            err,
                            key,
                            status
                        );
                    }
                }
            }
            WSAction::Delete(key) => {
                let read_guard = self.ctx.store().read().await;
                if let Some(obj) = read_guard.get(&key) {
                    if let Err(err) = self
                        .ws_update_service
                        .delete(obj.inner().ctx().item().clone())
                        .await
                    {
                        error!("error: {}, deleting {}", S::LABEL, err);
                    }
                } else {
                    error!(
                        key = &*format!("{key}"),
                        "Store: trying to delete non existent key",
                    );
                }
            }
            WSAction::DeleteFinal(key) => {
                let read_guard = self.ctx.store().read().await;
                if let Some(obj) = read_guard.get(&key) {
                    if let Err(err) = self
                        .ws_update_service
                        .final_delete(obj.inner().ctx().item().clone())
                        .await
                    {
                        error!("error: {}, deleting final {}", S::LABEL, err);
                    }
                } else {
                    error!(
                        key = &*format!("{key}"),
                        "Store: trying to delete final non existent key",
                    );
                }
            }
        }
    }

    fn multi_namespace_context(&self) -> bool {
        matches!(self.namespace, NameSpace::All)
    }
}

mod convert {

    //!
    //! # Auth Token Actions
    //!
    //! Converts Kubernetes Auth-Token events into Auth-Token actions
    //!

    use std::fmt::Display;

    use tracing::{debug, error, trace};
    use tracing::instrument;
    use crate::k8_types::{K8List, K8Obj, K8Watch};
    use crate::store::actions::*;
    use crate::store::k8::{K8MetaItem, K8ExtendedSpec, K8ConvertError};
    use crate::core::Spec;
    use k8_metadata_client::*;

    use crate::store::*;
    use crate::StoreError;

    ///
    /// Translate full metadata items from KVInputAction against MemStore which contains local state
    /// It only generates KVInputAction if incoming k8 object is different from memstore
    ///
    /// This will be replaced with store::sync_all
    #[instrument(skip(k8_tokens, local_store))]
    pub async fn k8_events_to_metadata_actions<S>(
        k8_tokens: K8List<S::K8Spec>,
        local_store: &LocalStore<S, K8MetaItem>,
        multi_namespace_context: bool,
    ) -> Result<(), StoreError>
    where
        S: K8ExtendedSpec + PartialEq,
        <S as Spec>::Owner: K8ExtendedSpec,
        S::Status: PartialEq,
        S::IndexKey: Display,
    {
        let mut meta_items = vec![];
        for k8_obj in k8_tokens.items {
            trace!("converting kv: {:#?}", k8_obj);
            let new_kv_value = match k8_obj_to_kv_obj(k8_obj, multi_namespace_context) {
                Ok(k8_value) => k8_value,
                Err(err) => match err {
                    K8ConvertError::Skip(obj) => {
                        debug!("skipping: {} {}", S::LABEL, obj.metadata.name);
                        continue;
                    }
                    K8ConvertError::KeyConvertionError(err) => return Err(err.into()),
                    K8ConvertError::Other(err) => return Err(err.into()),
                },
            };

            debug!("K8: Received Last {}:{}", S::LABEL, new_kv_value.key());
            meta_items.push(new_kv_value);
        }

        local_store.sync_all(meta_items).await;
        Ok(())
    }

    ///
    /// Translates watch events into metadata action and apply into local store
    ///
    #[instrument(skip(stream, local_store))]
    pub async fn k8_watch_events_to_metadata_actions<S, E>(
        stream: TokenStreamResult<S::K8Spec, E>,
        local_store: &LocalStore<S, K8MetaItem>,
        multi_namespace_context: bool,
    ) -> Option<SyncStatus>
    where
        S: K8ExtendedSpec + PartialEq,
        S::IndexKey: Display,
        <S as Spec>::Owner: K8ExtendedSpec,
        S::Status: PartialEq,
        E: MetadataClientError,
        S::IndexKey: Display,
    {
        let events = stream.unwrap();
        debug!("k8 {}: received watch events: {}", S::LABEL, events.len());
        let mut changes = vec![];

        // loop through items and generate add/mod actions
        for token in events {
            match token {
                Ok(watch_obj) => match watch_obj {
                    K8Watch::ADDED(k8_obj) => {
                        trace!("{} ADDED: {:#?}", S::LABEL, k8_obj);
                        match k8_obj_to_kv_obj(k8_obj, multi_namespace_context) {
                            Ok(new_kv_value) => {
                                debug!("K8: Watch Add: {}:{}", S::LABEL, new_kv_value.key());
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
                        match k8_obj_to_kv_obj(k8_obj, multi_namespace_context) {
                            Ok(updated_kv_value) => {
                                debug!("K8: Watch Update {}:{}", S::LABEL, updated_kv_value.key());
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
                        > = k8_obj_to_kv_obj(k8_obj, multi_namespace_context);
                        match meta {
                            Ok(kv_value) => {
                                debug!("K8: Watch Delete {}:{}", S::LABEL, kv_value.key());
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
                },
                Err(err) => error!("Problem parsing {} event: {} ... (exiting)", S::LABEL, err),
            }
        }

        local_store.apply_changes(changes).await
    }

    ///
    /// Translates K8 object into Internal metadata object
    ///
    pub fn k8_obj_to_kv_obj<S>(
        k8_obj: K8Obj<S::K8Spec>,
        multi_namespace_context: bool,
    ) -> Result<MetadataStoreObject<S, K8MetaItem>, K8ConvertError<S::K8Spec>>
    where
        S: K8ExtendedSpec,
        <S as Spec>::Owner: K8ExtendedSpec,
    {
        S::convert_from_k8(k8_obj, multi_namespace_context).map(|val| {
            trace!("converted val: {:#?}", val);
            val
        })
    }
}
