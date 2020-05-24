//!
//! # Kubernetes Dispatcher
//!
//! Dispatcher is the Event Loop, listens to messages form etcd KV store, translates them
//! to actions, and sends them to Streaming Coordinator Workflow for processing.
//!
use std::time::Duration;
use std::sync::Arc;
use std::fmt::Debug;
use std::fmt::Display;

use futures::future::FutureExt;
use futures::channel::mpsc::Sender;
use futures::select;
use futures::stream::StreamExt;
use futures::sink::SinkExt;
use log::debug;
use log::error;
use log::info;
use log::trace;
use serde::de::DeserializeOwned;

use flv_util::actions::Actions;
use flv_types::defaults::SC_RECONCILIATION_INTERVAL_SEC;
use flv_future_aio::task::spawn;
use flv_future_aio::timer::sleep;
use k8_metadata::metadata::K8List;
use k8_metadata::metadata::K8Watch;
use k8_metadata::metadata::Spec as K8Spec;
use k8_metadata_client::MetadataClient;
use k8_metadata_client::SharedClient;

use crate::core::common::new_channel;
use crate::core::common::LocalStore;
use crate::core::common::LSChange;
use crate::core::Spec;
use crate::core::WSChangeChannel;

use super::k8_events_to_actions::k8_events_to_metadata_actions;
use super::k8_events_to_actions::k8_event_stream_to_metadata_actions;

/// Sends out Local State Changes by comparing against Cluster state stored in local K8 where SC is running
/// Similar to Kubernetes Shared Informer
pub struct K8ClusterStateDispatcher<S, C>
where
    S: Spec,
    S::Status: Debug + PartialEq,
    S::Key: Debug,
{
    client: SharedClient<C>,
    metadata: Arc<LocalStore<S>>,
    senders: Vec<Sender<Actions<LSChange<S>>>>,
    namespace: String,
}

impl<S, C> K8ClusterStateDispatcher<S, C>
where
    S: Spec + PartialEq + Debug + Sync + Send + 'static,
    S::Status: PartialEq + Debug + Sync + Send + 'static,
    S::Key: Display + Debug + Clone + Sync + Send + 'static,
    K8Watch<S::K8Spec>: DeserializeOwned,
    K8List<S::K8Spec>: DeserializeOwned,
    S::K8Spec: Debug + Sync + Send + 'static,
    <<S as Spec>::K8Spec as K8Spec>::Status: Debug + Sync + Send + 'static,
    C: MetadataClient + 'static,
{
    pub fn new(namespace: String, client: SharedClient<C>, metadata: Arc<LocalStore<S>>) -> Self {
        Self {
            namespace,
            client,
            metadata,
            senders: vec![],
        }
    }

    pub fn create_channel(&mut self) -> WSChangeChannel<S> {
        let (sender, receiver) = new_channel();
        self.senders.push(sender);
        receiver
    }

    pub fn run(self) {
        spawn(self.outer_loop());
    }

    async fn outer_loop(mut self) {
        info!("starting {} kv dispatcher loop", S::LABEL);
        loop {
            self.inner_loop().await;
        }
    }

    ///
    /// Kubernetes Dispatcher Event Loop
    ///
    async fn inner_loop(&mut self) {
        let mut resume_stream: Option<String> = None;
        // retrieve all items from K8 store first
        match self.retrieve_all_k8_items().await {
            Ok(items) => {
                resume_stream = Some(items);
            }
            Err(err) => error!("cannot retrieve K8 store objects: {}", err),
        };

        let client = self.client.clone();

        // create watch streams
        let mut k8_stream = client
            . watch_stream_since::<S::K8Spec,_>(self.namespace.clone(), resume_stream)
            .fuse();

        trace!("starting watch stream for: {}", S::LABEL);
        loop {
            select! {
                _ = (sleep(Duration::from_secs(SC_RECONCILIATION_INTERVAL_SEC))).fuse() => {
                    debug!("timer fired - kickoff SC reconciliation");
                    break;
                },

                k8_result = k8_stream.next() =>  {

                    if let Some(result) = k8_result {
                        match result {
                            Ok(auth_token_msgs) => {
                                let actions = k8_event_stream_to_metadata_actions(
                                    Ok(auth_token_msgs),
                                    &self.metadata,
                                );
                                self.send_actions(actions).await;
                            }
                            Err(err) => error!("{}", err),
                        }

                    } else {
                        debug!("SPU stream terminated, during update auth-token processing... reconnecting");
                        break;
                    }
                },

            }
        }
    }

    ///
    /// Retrieve all items from Kubernetes (K8) store for forward them to processing engine
    ///
    async fn retrieve_all_k8_items(&mut self) -> Result<String, C::MetadataClientError> {
        let k8_objects = self
            .client
            .retrieve_items::<S::K8Spec,_>(self.namespace.clone())
            .await?;

        self.process_retrieved_items(k8_objects).await
    }

    ///
    /// Convert items into actions and send to Controller dispatcher for processing
    ///
    async fn process_retrieved_items(
        &mut self,
        k8_items: K8List<S::K8Spec>,
    ) -> Result<String, C::MetadataClientError> {
        let version = k8_items.metadata.resource_version.clone();

        debug!("UpdateAll {}", S::LABEL);
        // wait to receive all items before sending to channel
        let actions = k8_events_to_metadata_actions(k8_items, &self.metadata);

        self.send_actions(actions).await;

        // return versions to the caller
        Ok(version)
    }

    async fn send_actions(&mut self, actions: Actions<LSChange<S>>) {
        // for now do serially
        trace!(
            "sending {} LS Changes: {} to {} senders",
            S::LABEL,
            actions.count(),
            self.senders.len()
        );
        for sender in &mut self.senders {
            if let Err(err) = sender.send(actions.clone()).await {
                error!("error sending actions: {:#?}", err);
            }
        }
    }
}
