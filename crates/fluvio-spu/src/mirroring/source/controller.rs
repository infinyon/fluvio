use std::{
    fmt,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering, AtomicI64},
    },
    time::Duration,
};

use futures_util::StreamExt;
use tokio::select;
use tracing::{debug, error, warn, instrument};
use anyhow::{anyhow, Result};
use adaptive_backoff::prelude::{
    ExponentialBackoffBuilder, BackoffBuilder, ExponentialBackoff, Backoff,
};

use fluvio::config::TlsPolicy;
use fluvio_controlplane::{
    spu_api::update_upstream_cluster::UpstreamCluster, upstream_cluster::UpstreamClusterSpec,
};
use fluvio_controlplane_metadata::partition::SourcePartitionConfig;
use fluvio_storage::{ReplicaStorage, FileReplica};

use fluvio_socket::{FluvioSocket, FluvioSink};
use fluvio_spu_schema::{Isolation, server::mirror::StartMirrorRequest};
use fluvio_future::{task::spawn, timer::sleep, net::DomainConnector};
use fluvio_protocol::{record::Offset, api::RequestMessage};
use fluvio_types::event::{StickyEvent, offsets::OffsetChangeListener};

use crate::{
    replication::leader::SharedLeaderState,
    core::{GlobalContext, upstream_cluster::SharedUpstreamClusterLocalStore},
    // core::metrics,
};
use crate::mirroring::targt::{
    target_api::TargetMirrorRequest, api_key::MirrorTargetApiEnum,
    update_offsets::UpdateTargetOffsetRequest,
};

use super::sync::FilePartitionSyncRequest;

pub(crate) type SharedMirrorControllerState = Arc<MirrorControllerState>;

/// Metrics for mirror controller
#[derive(Debug)]
pub(crate) struct MirrorControllerMetrics {
    loop_count: AtomicU64,
    connect_count: AtomicU64,
    connect_failure: AtomicU64,
    target_leo: AtomicI64,
}

#[allow(dead_code)]
impl MirrorControllerMetrics {
    fn update_target_leo(&self, leo: Offset) {
        self.target_leo.store(leo, Ordering::SeqCst);
    }

    fn get_target_leo(&self) -> Offset {
        self.target_leo.load(Ordering::SeqCst)
    }

    fn increase_loop_count(&self) {
        self.loop_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn get_loop_count(&self) -> u64 {
        self.loop_count.load(Ordering::Relaxed)
    }

    fn increase_conn_count(&self) {
        self.connect_count.fetch_add(1, Ordering::Relaxed);
    }

    fn get_conn_count(&self) -> u64 {
        self.connect_count.load(Ordering::Relaxed)
    }

    fn increase_conn_failure(&self) {
        self.connect_failure.fetch_add(1, Ordering::Relaxed);
    }

    fn get_conn_failure(&self) -> u64 {
        self.connect_failure.load(Ordering::Relaxed)
    }
}

/// State for mirror controller which can be shared across tasks
#[derive(Debug)]
pub(crate) struct MirrorControllerState {
    metrics: MirrorControllerMetrics,

    #[allow(dead_code)]
    shutdown: Arc<StickyEvent>,
}

impl MirrorControllerState {
    pub(crate) fn new() -> Self {
        Self {
            metrics: MirrorControllerMetrics {
                loop_count: AtomicU64::new(0),
                target_leo: AtomicI64::new(-1), // -1 indicate this is unknown
                connect_count: AtomicU64::new(0),
                connect_failure: AtomicU64::new(0),
            },
            shutdown: StickyEvent::shared(),
        }
    }

    #[allow(dead_code)]
    pub(crate) fn get_metrics(&self) -> &MirrorControllerMetrics {
        &self.metrics
    }
}

const CLUSTER_LOOKUP_SEC: u64 = 5; // 1 sec

/// This controller run on mirror source.
/// It's main responsbility is to synchronize mirror target from source.
/// Source will always initiate connection to target.
/// The synchronization activites are trigger by 2 events.
/// 1. Leader offset change due to new records
/// 2. Offset update event from target
/// Based on those events, controller will try update target with missing records.
/// Target will send periodic update event even if it has fully caught up with events.
pub(crate) struct MirrorSourceToTargetController<S> {
    leader: SharedLeaderState<S>,
    source: SourcePartitionConfig,
    state: Arc<MirrorControllerState>,
    upstream_cluster_store: SharedUpstreamClusterLocalStore,
    max_bytes: u32,
    isolation: Isolation,
}

impl<S> fmt::Debug for MirrorSourceToTargetController<S>
where
    S: ReplicaStorage,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "MirrorSource {}->{}",
            self.leader.id(),
            self.source.upstream_cluster
        )
    }
}

impl<S> MirrorSourceToTargetController<S>
where
    S: ReplicaStorage + Sync + Send + 'static,
{
    pub(crate) fn run(
        ctx: &GlobalContext<FileReplica>,
        leader: SharedLeaderState<S>,
        source: SourcePartitionConfig,
        isolation: Isolation,
        max_bytes: u32,
    ) -> SharedMirrorControllerState {
        debug!(
            isolation = ?isolation,
            max_bytes,
            "starting mirror source controller {:#?}",source);
        let state = Arc::new(MirrorControllerState::new());

        let controller = Self {
            leader,
            isolation,
            source,
            state: state.clone(),
            max_bytes,
            upstream_cluster_store: ctx.upstream_cluster_localstore_owned(),
        };
        spawn(controller.dispatch_loop());
        state
    }

    #[instrument()]
    async fn dispatch_loop(self) {
        let mut offset_listener = self.leader.offset_listener(&self.isolation);

        let mut backoff = create_backoff();

        debug!("initial delay to wait for upstream cluster to be ready");
        sleep(Duration::from_secs(CLUSTER_LOOKUP_SEC)).await;

        loop {
            // first find upstream cluster
            if let Some(upstream_cluster) = self.find_upstream_cluster() {
                self.state.metrics.increase_loop_count();
                debug!(name = upstream_cluster.name, "found upstream cluster");
                let target_socket = self
                    .create_socket_to_target(&mut backoff, &upstream_cluster.spec)
                    .await;
                //  debug!(target = self.target, "connected to target");
                if let Err(err) = self
                    .sync_mirror_loop(&upstream_cluster, &mut offset_listener, target_socket)
                    .await
                {
                    error!("error syncing mirror loop {}", err);
                    self.backoff_and_wait(&mut backoff).await;
                }
            } else {
                warn!(
                    upstream = self.source.upstream_cluster,
                    "upstream cluster not found, waiting 1 second"
                );
                sleep(Duration::from_secs(CLUSTER_LOOKUP_SEC)).await;
            }
        }
    }

    #[instrument]
    // main sync loop for each target connection
    async fn sync_mirror_loop(
        &self,
        upstream_cluster: &UpstreamCluster,
        leader_offset_listner: &mut OffsetChangeListener,
        (target_socket, tls): (FluvioSocket, bool),
    ) -> Result<()> {
        //  debug!(target = self.target, "start syncing mirror");

        let (mut target_sink, mut target_stream) = target_socket.split();

        if tls {
            debug!("tls enabled, disabling zero copy sink");
            target_sink.disable_zerocopy();
        }

        //
        let mut target_api_stream =
            target_stream.api_stream::<TargetMirrorRequest, MirrorTargetApiEnum>();

        self.send_initial_request(upstream_cluster, &mut target_sink)
            .await?;

        // this flag is set to true, target need to be refreshed leader's offsets and any recordset.
        let mut target_updated_needed = false;

        // target_updated_needed triggers warning, despite being used in loop
        #[allow(unused)]
        loop {
            let target_leo = self.state.metrics.get_target_leo();

            debug!(target_leo, target_updated_needed, "waiting for next event");

            // update target if flag is set and we know what target leo is
            if target_updated_needed && target_leo >= 0 {
                self.update_upstream(&mut target_sink, target_leo).await?;
                target_updated_needed = false;
            }

            select! {

                    _ = leader_offset_listner.listen() => {
                        debug!("leader offset has changed, upstream needs to be updated");
                        target_updated_needed = true;
                    }

                    msg = target_api_stream.next() => {
                        debug!("received response from target");
                        if let Some(req_msg_target) = msg {
                            let target_msg = req_msg_target?;

                            match target_msg {
                                TargetMirrorRequest::UpdateTargetOffset(req)=> {
                                    target_updated_needed = self.update_from_target(req)?;
                                }
                             }

                        } else {
                            debug!("leader socket has terminated");
                            break;
                        }

                    }
            }

            self.state.metrics.increase_conn_count();
        }

        debug!("target has closed connection, terminating loop");

        Ok(())
    }

    async fn send_initial_request(
        &self,
        upstream_cluster: &UpstreamCluster,
        target_sink: &mut FluvioSink,
    ) -> Result<()> {
        // always starts with mirrong request
        let start_mirror_request = RequestMessage::new_request(StartMirrorRequest {
            remote_cluster_id: upstream_cluster.spec.source_id.clone(),
            source_replica: self.leader.id().to_string(),
            ..Default::default()
        });

        debug!("sending start mirror request: {:#?}", start_mirror_request);

        // send start mirror request
        target_sink
            .send_request(&start_mirror_request)
            .await
            .map_err(|err| err.into())
    }

    /// received new offset from target, update controller's knowledge
    /// it will return true if target needs to be updated
    #[instrument(skip(req))]
    fn update_from_target(&self, req: RequestMessage<UpdateTargetOffsetRequest>) -> Result<bool> {
        let leader_leo = self.leader.leo();
        let old_target_leo = self.state.metrics.get_target_leo();
        let new_target_leo = req.request.leo;
        debug!(
            leader_leo,
            old_target_leo, new_target_leo, "received update from target"
        );
        // if old target leo is not initialized, we need to update target
        if old_target_leo < 0 {
            debug!(new_target_leo, "updating target leo from uninitialized");
            self.state.metrics.update_target_leo(new_target_leo);
        }
        match new_target_leo.cmp(&leader_leo) {
            std::cmp::Ordering::Greater => {
                // target leo should never be greater than leader's leo
                warn!(
                    leader_leo,
                    new_target_leo,
                    "target has more records, this should not happen, this is error"
                );
                return Err(anyhow!("target's leo: {new_target_leo} > leader's leo: {leader_leo} this should not happen, this is error"));
            }
            std::cmp::Ordering::Less => {
                debug!(
                    new_target_leo,
                    leader_leo, "target has less records, need to refresh target"
                );
                self.state.metrics.update_target_leo(new_target_leo);
                Ok(true)
            }
            std::cmp::Ordering::Equal => {
                debug!(
                    new_target_leo,
                    "target has same records, no need to refresh target"
                );
                Ok(false)
            }
        }
    }

    #[instrument]
    async fn update_upstream(&self, sink: &mut FluvioSink, target_leo: Offset) -> Result<()> {
        debug!("updating upstream");
        if let Some(sync_request) = self.generate_target_sync(target_leo).await? {
            debug!(?sync_request, "upstream sync");
            let request = RequestMessage::new_request(sync_request);
            // .set_client_id(format!("leader: {}", self.ctx.local_spu_id()));
            sink.encode_file_slices(&request, request.header.api_version())
                .await?;
            Ok(())
        } else {
            Ok(())
        }
    }

    /// look up upstream cluster from local store
    /// this may retur None if remote cluster is send by SC by time controller is started
    fn find_upstream_cluster(&self) -> Option<UpstreamCluster> {
        let read = self.upstream_cluster_store.read();
        read.get(&self.source.upstream_cluster).cloned()
    }

    /// compute records necessary to fill in gap for mirror target
    async fn generate_target_sync(
        &self,
        target_leo: Offset,
    ) -> Result<Option<FilePartitionSyncRequest>> {
        // leader off should be always greater than source leo
        let leader_offset = self.leader.as_offset();

        // if source mirror is all caught up, there is no need to send out update
        if leader_offset.leo == target_leo {
            debug!("target has caught up, just chilling out");
            return Ok(None);
        }

        let mut partition_response = FilePartitionSyncRequest {
            leo: leader_offset.leo,
            hw: leader_offset.hw,
            ..Default::default()
        };

        if leader_offset.leo > target_leo {
            match self
                .leader
                .read_records(target_leo, self.max_bytes, self.isolation)
                .await
            {
                Ok(slice) => {
                    debug!(
                        hw = slice.end.hw,
                        leo = slice.end.leo,
                        replica = %self.leader.id(),
                        "read records"
                    );
                    if let Some(file_slice) = slice.file_slice {
                        partition_response.records = file_slice.into();
                    }
                    Ok(Some(partition_response))
                }
                Err(err) => {
                    error!(%err, "error reading records");
                    Err(anyhow!("error reading records: {}", err))
                }
            }
        } else {
            //
            debug!(
                hw = leader_offset.hw,
                leo = leader_offset.leo,
                target_leo,
                "oh no mirror target has more records"
            );
            Err(anyhow!(
                "leader has more records than target, this should not happen"
            ))
        }
    }

    /// create socket to target, this will always succeed
    #[instrument]
    async fn create_socket_to_target(
        &self,
        backoff: &mut ExponentialBackoff,
        upstream_cluster: &UpstreamClusterSpec,
    ) -> (FluvioSocket, bool) {
        let tlspolicy = option_tlspolicy(upstream_cluster);

        loop {
            self.state.metrics.increase_conn_count();

            let endpoint = &upstream_cluster.target.endpoint;
            debug!(
                endpoint,
                attempt = self.state.metrics.get_conn_count(),
                "trying connect to target",
            );

            let res = if let Some(tlspolicy) = &tlspolicy {
                match DomainConnector::try_from(tlspolicy.clone()) {
                    Ok(connector) => {
                        // box connector?
                        FluvioSocket::connect_with_connector(endpoint, &(*connector)).await
                    }
                    Err(err) => {
                        error!(
                            "error establishing tls with leader at: <{}> err: {}",
                            endpoint, err
                        );
                        self.backoff_and_wait(backoff).await;
                        continue;
                    }
                }
                // FluvioSocket::connect(&endpoint)
            } else {
                FluvioSocket::connect(endpoint).await
            };
            match res {
                Ok(socket) => {
                    debug!("connected");
                    return (socket, tlspolicy.is_some());
                }

                Err(err) => {
                    error!("error connecting to leader at: <{}> err: {}", endpoint, err);
                    self.backoff_and_wait(backoff).await;
                }
            }
        }
    }

    async fn backoff_and_wait(&self, backoff: &mut ExponentialBackoff) {
        let wait = backoff.wait();
        debug!(seconds = wait.as_secs(), "starting backing off, sleeping");
        sleep(wait).await;
        debug!("resume from backing off");
        self.state.metrics.increase_conn_failure();
    }
}

fn create_backoff() -> ExponentialBackoff {
    ExponentialBackoffBuilder::default()
        .min(Duration::from_secs(1))
        .max(Duration::from_secs(300))
        .build()
        .unwrap()
}

fn option_tlspolicy(ups: &UpstreamClusterSpec) -> Option<TlsPolicy> {
    use fluvio::config::{TlsCerts, TlsConfig};

    let ct = match &ups.target.tls {
        Some(ct) => ct,
        _ => {
            return None;
        }
    };

    let certs = TlsCerts {
        domain: ct.domain.clone(),
        key: ct.client_key.clone(),
        cert: ct.client_cert.clone(),
        ca_cert: ct.ca_cert.clone(),
    };
    let tlscfg = TlsConfig::Inline(certs);
    Some(TlsPolicy::from(tlscfg))
}
