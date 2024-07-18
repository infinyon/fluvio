use std::time::Duration;
use std::fmt;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_stream_model::core::MetadataItem;
use fluvio_stream_model::store::NameSpace;
use futures_util::stream::StreamExt;
use tracing::debug;
use tracing::error;
use tracing::trace;
use tracing::instrument;
use serde::de::DeserializeOwned;
use serde::Serialize;
use once_cell::sync::Lazy;

use fluvio_future::task::spawn;
use fluvio_future::task::JoinHandle;
use fluvio_future::timer::sleep;

use crate::core::Spec;
use crate::metadata::{SharedClient, MetadataClient};
use crate::store::k8::K8ExtendedSpec;
use crate::store::StoreContext;
use crate::actions::WSAction;

static SC_RECONCILIATION_INTERVAL_SEC: Lazy<u64> = Lazy::new(|| {
    use std::env;

    let var_value = env::var("FLV_SC_RECONCILIATION_INTERVAL").unwrap_or_default();
    let wait_time: u64 = var_value.parse().unwrap_or(60 * 5);
    wait_time
});

/// For each spec, process updates from metadata client
pub struct MetadataDispatcher<S, C, M>
where
    S: K8ExtendedSpec,
    <S as Spec>::Owner: K8ExtendedSpec,
    M: MetadataItem,
{
    client: SharedClient<C>,
    namespace: NameSpace,
    ctx: StoreContext<S, M>,
}

impl<S, C, M> Debug for MetadataDispatcher<S, C, M>
where
    S: K8ExtendedSpec,
    <S as Spec>::Owner: K8ExtendedSpec,
    M: MetadataItem,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{} K8StateDispatcher", S::LABEL)
    }
}

impl<S, C, M> MetadataDispatcher<S, C, M>
where
    S: K8ExtendedSpec,
    <S as Spec>::Owner: K8ExtendedSpec,
    <S as K8ExtendedSpec>::K8Spec: DeserializeOwned + Serialize,
    C: MetadataClient<M> + 'static,
    M: MetadataItem,
{
    /// start dispatcher
    pub fn start(
        namespace: impl Into<NameSpace>,
        client: SharedClient<C>,
        ctx: StoreContext<S, M>,
    ) -> JoinHandle<()> {
        let dispatcher = Self {
            namespace: namespace.into(),
            client,
            ctx,
        };

        spawn(dispatcher.outer_loop())
    }

    #[instrument(
        name = "MetadataDispatcher",
        skip(self),
        fields(
            spec = S::LABEL,
            namespace = self.namespace.as_str(),
        )
    )]
    async fn outer_loop(mut self) {
        loop {
            debug!("starting reconciliation loop");
            if let Err(err) = self.reconcillation_loop().await {
                error!(
                    "error with reconciliation loop: {:#?}, sleep 10 seconds",
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
        let mut k8_stream = client.watch_stream_since::<S>(&self.namespace, resume_stream);

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
                            Ok(changes) => {
                                self.ctx.store().apply_changes(changes).await;
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
            .retrieve_items::<S>(&self.namespace)
            .await
            .map_err(|err| {
                IoError::new(
                    ErrorKind::InvalidData,
                    format!("error retrieving k8: {err:?}"),
                )
            })?;

        let version = k8_objects.version.clone();
        debug!(
            Spec = S::LABEL,
            version = &*version,
            item_count = k8_objects.items.len(),
            "Retrieving items",
        );
        self.ctx.store().sync_all(k8_objects.items).await;

        Ok(version)
    }

    #[instrument(skip(self, action))]
    async fn process_ws_action(&mut self, action: WSAction<S, M>) {
        match action {
            WSAction::Apply(obj) => {
                if let Err(err) = self.client.apply(obj).await {
                    error!("error: {}, applying {}", S::LABEL, err);
                }
            }
            WSAction::UpdateSpec((key, spec)) => {
                let read_guard = self.ctx.store().read().await;
                if let Some(obj) = read_guard.get(&key) {
                    let meta = obj.inner().ctx().item().clone();
                    if let Err(err) = self.client.update_spec(meta, spec).await {
                        error!("error: {:#?}, update spec {:#?}", S::LABEL, err);
                    }
                } else {
                    // create new ctx
                    if let Err(err) = self
                        .client
                        .update_spec_by_key(key, &self.namespace, spec)
                        .await
                    {
                        error!("error: {:#?}, update spec by key {:#?}", S::LABEL, err);
                    }
                };
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
                match self
                    .client
                    .update_status(meta.clone(), status.clone(), &self.namespace)
                    .await
                {
                    Ok(updated_item) => {
                        use crate::store::actions::LSUpdate;
                        debug!(
                            key = ?updated_item.status,
                            status = ?updated_item.status,
                            "update status success"
                        );
                        let changes = vec![LSUpdate::Mod(updated_item)];
                        let _ = self.ctx.store().apply_changes(changes).await;
                    }
                    Err(err) => {
                        error!(
                            "{}, update status err: {}, key: {}, status: {:#?}",
                            S::LABEL,
                            err,
                            key,
                            status
                        );

                        if err.to_string().contains("409") {
                            // Does server side apply on conflict as a workaround
                            if let Err(err) = self
                                .client
                                .patch_status::<S>(meta, status.clone(), &self.namespace)
                                .await
                            {
                                error!(
                                    "{}, patch status err: {}, key: {}, status: {:#?}",
                                    S::LABEL,
                                    err,
                                    key,
                                    status
                                );
                            } else {
                                tracing::info!("successfully patched status for {key}");
                            }
                        }
                    }
                }
            }
            WSAction::Delete(key) => {
                let read_guard = self.ctx.store().read().await;
                if let Some(obj) = read_guard.get(&key) {
                    if let Err(err) = self
                        .client
                        .delete_item::<S>(obj.inner().ctx().item().clone())
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
                        .client
                        .finalize_delete_item::<S>(obj.inner().ctx().item().clone())
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
}
