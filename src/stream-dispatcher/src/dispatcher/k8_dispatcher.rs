use std::time::Duration;
use std::fmt;
use std::fmt::{Debug, Display};
use std::io::Error as IoError;
use std::io::ErrorKind;

use futures_lite::stream::StreamExt;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::trace;
use tracing::instrument;
use serde::de::DeserializeOwned;
use serde::Serialize;
use once_cell::sync::Lazy;

use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;

use k8_metadata_client::MetadataClient;
use k8_metadata_client::SharedClient;
use k8_metadata_client::NameSpace;

use crate::k8::metadata::K8List;
use crate::k8::metadata::K8Watch;
use crate::k8::metadata::Spec as K8Spec;

use crate::core::Spec;
use crate::store::k8::K8ExtendedSpec;
use crate::store::StoreContext;
use crate::actions::WSAction;

use convert::*;
use super::*;

static SC_RECONCILIATION_INTERVAL_SEC: Lazy<u64> = Lazy::new(|| {
    use std::env;

    let var_value = env::var("FLV_SC_RECONCILIATION_INTERVAL").unwrap_or_default();
    let wait_time: u64 = var_value.parse().unwrap_or(60);
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
        fields(
            namespace = self.namespace.named(),
        )
    )]
    async fn outer_loop(mut self) {
        info!("starting k8 dispatcher loop");
        loop {
            debug!("starting inner loop");
            self.inner_loop().await;
        }
    }

    ///
    /// Main Event Loop
    ///
    #[instrument(skip(self))]
    async fn inner_loop(&mut self) {
        use tokio::select;

        info!("begin new reconcillation loop");

        let mut resume_stream: Option<String> = None;
        // retrieve all items from K8 store first
        match self.retrieve_all_k8_items().await {
            Ok(items) => {
                resume_stream = Some(items);
            }
            Err(err) => error!("cannot retrieve K8 store objects: {}", err),
        };

        let client = self.client.clone();

        let mut reconcile_timer = sleep(Duration::from_secs(*SC_RECONCILIATION_INTERVAL_SEC));

        // create watch streams
        let mut k8_stream =
            client.watch_stream_since::<S::K8Spec, _>(self.namespace.clone(), resume_stream);

        loop {
            debug!("dispatcher waiting");
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

                                if let Some(status) = k8_watch_events_to_metadata_actions(
                                    Ok(auth_token_msgs),
                                    self.ctx.store(),
                                ).await
                                {
                                    if status.has_spec_changes() {
                                        self.ctx.notify_spec_changes();
                                    }
                                    if status.has_status_changes() {
                                        self.ctx.notify_status_changes();
                                    }
                                } else {
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
                            debug!("store: received ws action: {}", action);
                            self.process_ws_action(action).await;
                        },
                        Err(err) => {
                            error!("WS channel error: {}", err);
                            panic!(-1);
                        }
                    }
                }

            }
        }
    }

    ///
    /// Retrieve all items from Kubernetes (K8) store for forward them to processing engine
    ///
    #[instrument(skip(self))]
    async fn retrieve_all_k8_items(&mut self) -> Result<String, IoError> {
        let k8_objects = self
            .client
            .retrieve_items::<S::K8Spec, _>(self.namespace.clone())
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("error retrieving k8: {}", err),
                )
            })?;

        let version = k8_objects.metadata.resource_version.clone();
        debug!(
            version = &*version,
            item_count = k8_objects.items.len(),
            "Retrieving items",
        );
        // wait to receive all items before sending to channel
        k8_events_to_metadata_actions(k8_objects, self.ctx.store())
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("error converting k8: {}", err),
                )
            })?;
        debug!("{}: notifying update all", S::LABEL);
        self.ctx.notify_spec_changes();
        self.ctx.notify_status_changes();
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
                    error!("error: {}, update spec {}", S::LABEL, err);
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
                    "{} begin update status key: {}, revision: {}",
                    S::LABEL,
                    key,
                    meta.resource_version
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
                            "{} k8 update Status: {}, rev: {},stats: {:#?}",
                            S::LABEL,
                            item.metadata.name,
                            item.metadata.resource_version,
                            item.status,
                        );

                        match convert::k8_obj_to_kv_obj(item) {
                            Ok(updated_item) => {
                                let changes = vec![LSUpdate::Mod(updated_item)];

                                if let Some(changes) = self.ctx.store().apply_changes(changes).await
                                {
                                    if changes.has_spec_changes() {
                                        self.ctx.notify_spec_changes();
                                    }
                                    if changes.has_status_changes() {
                                        self.ctx.notify_status_changes();
                                    }
                                }
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
                        key = &*format!("{}", key),
                        "Store: trying to delete non existent key",
                    );
                }
            }
        }
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
    use crate::k8::metadata::K8List;
    use crate::k8::metadata::K8Obj;
    use crate::k8::metadata::K8Watch;
    use crate::store::actions::*;
    use crate::store::k8::K8MetaItem;
    use crate::store::k8::K8ExtendedSpec;
    use crate::store::k8::K8ConvertError;
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
            let new_kv_value = match k8_obj_to_kv_obj(k8_obj) {
                Ok(k8_value) => k8_value,
                Err(err) => match err {
                    K8ConvertError::Skip(obj) => {
                        debug!("skipping: {}", obj.metadata.name);
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
        debug!("k8 {}: received  watch events: {}", S::LABEL, events.len());
        let mut changes = vec![];

        // loop through items and generate add/mod actions
        for token in events {
            match token {
                Ok(watch_obj) => match watch_obj {
                    K8Watch::ADDED(k8_obj) => match k8_obj_to_kv_obj(k8_obj) {
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
                    },
                    K8Watch::MODIFIED(k8_obj) => match k8_obj_to_kv_obj(k8_obj) {
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
                    },
                    K8Watch::DELETED(k8_obj) => {
                        let meta: Result<
                            MetadataStoreObject<S, K8MetaItem>,
                            K8ConvertError<S::K8Spec>,
                        > = k8_obj_to_kv_obj(k8_obj);
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
    ) -> Result<MetadataStoreObject<S, K8MetaItem>, K8ConvertError<S::K8Spec>>
    where
        S: K8ExtendedSpec,
        <S as Spec>::Owner: K8ExtendedSpec,
    {
        S::convert_from_k8(k8_obj)
            .map(|val| {
                trace!("converted val: {:#?}", val.spec);
                val
            })
            .map_err(|err| err)
    }
}
