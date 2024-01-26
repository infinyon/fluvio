use std::fmt;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use adaptive_backoff::prelude::{ExponentialBackoff, ExponentialBackoffBuilder, Backoff};
use tracing::{info, trace, error, debug, warn, instrument};
use tokio::select;
use futures_util::stream::StreamExt;
use anyhow::{anyhow, Result};
use adaptive_backoff::backoff::BackoffBuilder;

use fluvio_controlplane::spu_api::update_remote_cluster::UpdateRemoteClusterRequest;
use fluvio_controlplane::spu_api::update_upstream_cluster::UpdateUpstreamClusterRequest;
use fluvio_controlplane::sc_api::register_spu::RegisterSpuRequest;
use fluvio_controlplane::sc_api::update_lrs::UpdateLrsRequest;
use fluvio_controlplane::spu_api::api::{InternalSpuRequest, InternalSpuApi};
use fluvio_controlplane::spu_api::update_replica::UpdateReplicaRequest;
use fluvio_controlplane::spu_api::update_smartmodule::UpdateSmartModuleRequest;
use fluvio_controlplane::spu_api::update_spu::UpdateSpuRequest;
use flv_util::print_cli_err;
use fluvio_future::task::spawn;
use fluvio_future::timer::sleep;
use fluvio_protocol::api::RequestMessage;
use fluvio_socket::{FluvioSocket, FluvioSink};
use fluvio_storage::FileReplica;

use crate::core::SharedGlobalContext;

use super::message_sink::SharedStatusUpdate;

pub(crate) type SharedScDispatcherMetrics = Arc<ScDispatcherMetrics>;

// keep track of various internal state of dispatcher
#[derive(Default)]
pub(crate) struct ScDispatcherMetrics {
    loop_count: AtomicU64,      // overal loop count
    replica_changes: AtomicU64, // replica changes received from sc
    spu_changes: AtomicU64,     // spu changes received from sc
    reconnect: AtomicU64,       // number of reconnect to sc
    smartmodule: AtomicU64,     // number of sm updates from sc
}

#[allow(dead_code)]
impl ScDispatcherMetrics {
    fn increase_loop_count(&self) {
        self.loop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_loop_count(&self) -> u64 {
        self.loop_count.load(Ordering::Relaxed)
    }

    fn increase_reconn_count(&self) {
        self.reconnect.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_reconn_count(&self) -> u64 {
        self.reconnect.load(Ordering::Relaxed)
    }

    fn increase_spu_changes(&self) {
        self.spu_changes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_spu_changes(&self) -> u64 {
        self.spu_changes.load(Ordering::Relaxed)
    }

    fn increase_replica_changes(&self) {
        self.replica_changes.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_replica_changes(&self) -> u64 {
        self.replica_changes.load(Ordering::Relaxed)
    }

    fn increase_smartmodule(&self) {
        self.smartmodule.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn get_smartmodule(&self) -> u64 {
        self.smartmodule.load(Ordering::Relaxed)
    }
}

/// Controller for handling connection to SC
/// including registering and reconnect
pub struct ScDispatcher<S> {
    ctx: SharedGlobalContext<S>,
    status_update: SharedStatusUpdate,
    metrics: SharedScDispatcherMetrics,
}

impl<S> fmt::Debug for ScDispatcher<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ScDispatcher")
    }
}

impl ScDispatcher<FileReplica> {
    pub(crate) fn run(ctx: SharedGlobalContext<FileReplica>) -> SharedScDispatcherMetrics {
        let metrics = Arc::new(ScDispatcherMetrics::default());

        let dispatcher = Self {
            status_update: ctx.status_update_owned(),
            ctx,
            metrics: metrics.clone(),
        };

        spawn(dispatcher.dispatch_loop());

        metrics
    }

    #[instrument()]
    async fn dispatch_loop(mut self) {
        let mut backoff = create_backoff();

        // clippy: this loop never loops due to final match break/break
        // loop {
        debug!(loop_count = self.metrics.get_loop_count(), "starting loop");

        let mut socket = self.create_socket_to_sc(&mut backoff).await;
        info!(
            local_spu_id=%self.ctx.local_spu_id(),
            "established connection to sc for spu",
        );

        // register and exit on error
        let _ = match self.send_spu_registration(&mut socket).await {
            Ok(status) => status,
            Err(err) => {
                print_cli_err!(format!(
                    "spu registration failed with sc due to error: {err}"
                ));
                warn!("spu registration failed with sc due to error: {err}");
                return;
                // break;
            }
        };

        // continuously process updates from and send back status to SC
        info!("starting sc request loop");
        match self.request_loop(socket).await {
            Ok(_) => {
                // break;
            }
            Err(err) => {
                warn!(?err, "error connecting to sc, waiting before reconnecting",);
                // break;
            }
        }
        // }
    }

    #[instrument(
        skip(self),
        name = "sc_dispatch_loop",
        fields(
            socket = socket.id()
        )
    )]
    async fn request_loop(&mut self, socket: FluvioSocket) -> Result<()> {
        use async_io::Timer;

        /// Interval between each send to SC
        /// SC status are not source of truth, it is delayed derived data.
        const MIN_SC_SINK_TIME: Duration = Duration::from_millis(400);

        let (mut sink, mut stream) = socket.split();
        let mut api_stream = stream.api_stream::<InternalSpuRequest, InternalSpuApi>();

        let mut status_timer = Timer::interval(MIN_SC_SINK_TIME);

        loop {
            trace!("waiting");

            select! {

                _ = status_timer.next() =>  {
                    self.send_status_back_to_sc(&mut sink).await?;
                },

                sc_request = api_stream.next() => {
                    debug!("got request from sc");
                    match sc_request {
                        Some(Ok(InternalSpuRequest::UpdateReplicaRequest(request))) => {

                            self.metrics.increase_replica_changes();
                            self.handle_update_replica_request(request,&mut sink).await;
                        },
                        Some(Ok(InternalSpuRequest::UpdateSpuRequest(request))) => {
                            self.metrics.increase_spu_changes();
                            if let Err(err) = self.handle_update_spu_request(request).await {
                                error!(%err, "error handling update spu request");
                                break;
                            }
                        },
                        Some(Ok(InternalSpuRequest::UpdateSmartModuleRequest(request))) => {
                            self.metrics.increase_smartmodule();
                            if let Err(err) = self.handle_update_smartmodule_request(request).await {
                                error!(%err, "error handling update SmartModule request", );
                                break;
                            }
                        },
                        Some(Ok(InternalSpuRequest::UpdateRemoteClusterRequest(request))) => {
                            if let Err(err) = self.handle_remote_cluster_request(request).await {
                                error!(%err, "error handling update remote cluster request", );
                                break;
                            }
                        },
                        Some(Ok(InternalSpuRequest::UpdateUpstreamClusterRequest(request))) => {
                            if let Err(err) = self.handle_upstream_cluster_request(request).await {
                                error!(%err, "error handling update upstream cluster request", );
                                break;
                            }
                        },
                        Some(Err(err)) => {
                            error!(%err, "Api error");
                            break;
                        },
                        None => {
                            debug!("sc connection terminated");
                            break;
                        }
                    }
                }

            }
        }

        debug!("exiting sc request loop");

        Ok(())
    }

    /// send status back to sc, if there is error return false
    #[instrument(skip(self))]
    async fn send_status_back_to_sc(&mut self, sc_sink: &mut FluvioSink) -> Result<()> {
        let requests = self.status_update.remove_all().await;

        if requests.is_empty() {
            debug!("sending empty status");
        } else {
            debug!(requests = ?requests, "sending status back to sc");
        }
        let message = RequestMessage::new_request(UpdateLrsRequest::new(requests));

        sc_sink
            .send_request(&message)
            .await
            .map_err(|err| anyhow!("error sending status back to sc: {}", err))
    }

    /// register local spu to sc
    #[instrument(
        skip(self),
        fields(socket = socket.id())
    )]
    async fn send_spu_registration(&self, socket: &mut FluvioSocket) -> Result<bool> {
        let local_spu_id = self.ctx.local_spu_id();

        debug!(%local_spu_id, "sending spu registration request",);

        let register_req = RegisterSpuRequest::new(local_spu_id);
        let mut message = RequestMessage::new_request(register_req);
        message
            .get_mut_header()
            .set_client_id(format!("spu: {local_spu_id}"));

        let response = socket.send(&message).await?;

        trace!(?response, "register response",);

        let register_resp = &response.response;
        if register_resp.is_error() {
            warn!(
                err = register_resp.error_message(),
                %local_spu_id,
                "spu registration failed",
            );

            Ok(false)
        } else {
            info!(local_spu_id, "spu registration successful");

            Ok(true)
        }
    }

    /// connect to sc if can't connect try until we succeed
    /// or if we received termination message
    async fn create_socket_to_sc(&mut self, backoff: &mut ExponentialBackoff) -> FluvioSocket {
        let spu_id = self.ctx.local_spu_id();
        let sc_endpoint = self.ctx.config().sc_endpoint().to_string();

        loop {
            info!(
                %sc_endpoint,
                spu_id,
                "trying to create socket to sc",

            );
            match FluvioSocket::connect(&sc_endpoint).await {
                Ok(socket) => {
                    info!(spu_id, "connected to sc for spu");
                    self.metrics.increase_reconn_count();
                    return socket;
                }
                Err(err) => {
                    warn!("error connecting to sc: {}", err);
                    let wait = backoff.wait();
                    info!(wait = wait.as_secs(), spu_id, "sleeping ms");
                    sleep(wait).await;
                }
            }
        }
    }

    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    #[instrument(skip(self, req_msg))]
    async fn handle_update_replica_request(
        &mut self,
        req_msg: RequestMessage<UpdateReplicaRequest>,
        sc_sink: &mut FluvioSink,
    ) {
        use crate::core::ReplicaChange;

        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"replica request");

        for action in self.ctx.apply_replica_update(request).await.into_iter() {
            match action {
                ReplicaChange::Remove(remove) => {
                    let message = RequestMessage::new_request(remove);
                    if let Err(err) = sc_sink.send_request(&message).await {
                        error!("error sending back to sc {err:#?}");
                    }
                }
                ReplicaChange::StorageError(err) => {
                    error!("error storage {err:#?}");
                }
            }
        }
    }

    ///
    /// Follower Update Handler sent by a peer Spu
    ///
    #[instrument(skip(self, req_msg), name = "update_spu_request")]
    async fn handle_update_spu_request(
        &mut self,
        req_msg: RequestMessage<UpdateSpuRequest>,
    ) -> Result<()> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"starting spu update");

        let _actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received spu sync all"
            );
            trace!("received spu all items: {:#?}", request.all);
            self.ctx.spu_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received spu changes"
            );
            trace!("received spu change items: {:#?}", request.changes);
            self.ctx.spu_localstore().apply_changes(request.changes)
        };

        self.ctx.sync_follower_update().await;

        trace!("finish spu update");

        Ok(())
    }

    ///
    /// Handle SmartModule update sent by SC
    ///
    #[instrument(skip(self, req_msg), name = "update_smartmodule_request")]
    async fn handle_update_smartmodule_request(
        &mut self,
        req_msg: RequestMessage<UpdateSmartModuleRequest>,
    ) -> Result<()> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"starting SmartModule update");

        let actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received smartmodule sync all"
            );
            trace!("received spu all items: {:#?}", request.all);
            self.ctx.smartmodule_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received smartmoudle changes"
            );
            trace!("received smartmodule change items: {:#?}", request.changes);
            self.ctx
                .smartmodule_localstore()
                .apply_changes(request.changes)
        };

        debug!(actions = actions.count(), "finished SmartModule update");

        Ok(())
    }

    async fn handle_remote_cluster_request(
        &mut self,
        req_msg: RequestMessage<UpdateRemoteClusterRequest>,
    ) -> anyhow::Result<()> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"starting remote cluster update");

        let actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received remote cluster sync all"
            );
            trace!("received spu all items: {:#?}", request.all);
            self.ctx.remote_cluster_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received remote cluster changes"
            );
            trace!(
                "received remote cluster change items: {:#?}",
                request.changes
            );
            self.ctx
                .remote_cluster_localstore()
                .apply_changes(request.changes)
        };

        debug!(actions = actions.count(), "finished remote cluster update");

        Ok(())
    }

    async fn handle_upstream_cluster_request(
        &mut self,
        req_msg: RequestMessage<UpdateUpstreamClusterRequest>,
    ) -> anyhow::Result<()> {
        let (_, request) = req_msg.get_header_request();

        debug!( message = ?request,"starting upstream cluster update");

        let actions = if !request.all.is_empty() {
            debug!(
                epoch = request.epoch,
                item_count = request.all.len(),
                "received upstream cluster sync all"
            );
            trace!("received spu all items: {:#?}", request.all);
            self.ctx.upstream_cluster_localstore().sync_all(request.all)
        } else {
            debug!(
                epoch = request.epoch,
                item_count = request.changes.len(),
                "received upstream cluster changes"
            );
            trace!(
                "received upsream cluster change items: {:#?}",
                request.changes
            );
            self.ctx
                .upstream_cluster_localstore()
                .apply_changes(request.changes)
        };

        debug!(
            actions = actions.count(),
            "finished upstream cluster update"
        );

        Ok(())
    }
}

fn create_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::default()
        .min(Duration::from_secs(1))
        .max(Duration::from_secs(300))
        .build()
        .unwrap()
}
